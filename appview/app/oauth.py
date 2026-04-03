# Adapted from the AT Protocol OAuth cookbook example:
# https://github.com/bluesky-social/cookbook/tree/main/python-oauth-web-app
# Original: atproto_oauth.py, atproto_security.py

import json
import logging
import time
import urllib.request
from urllib.parse import urlparse

import httpx
from authlib.common.security import generate_token
from authlib.jose import JsonWebKey, jwt
from authlib.oauth2.rfc7636 import create_s256_code_challenge

log = logging.getLogger(__name__)

SAFE_HTTP_TIMEOUT = httpx.Timeout(connect=2.0, read=10.0, write=10.0, pool=10.0)


def is_safe_url(url: str) -> bool:
    """Check that a URL looks safe for server-side requests (SSRF mitigation).

    This is a partial filter. The httpx client's own protections provide
    additional coverage.
    """
    parts = urlparse(url)
    if not (
        parts.scheme == "https"
        and parts.hostname is not None
        and parts.hostname == parts.netloc
        and parts.username is None
        and parts.password is None
        and parts.port is None
    ):
        return False

    segments = parts.hostname.split(".")
    if not (
        len(segments) >= 2
        and segments[-1] not in ["local", "arpa", "internal", "localhost"]
    ):
        return False

    if segments[-1].isdigit():
        return False

    return True


def validate_authserver_meta(meta: dict, url: str) -> None:
    """Validate Authorization Server metadata against AT Protocol OAuth requirements.

    Raises ValueError if any requirement is not met.
    """
    fetch_url = urlparse(url)
    issuer_url = urlparse(meta.get("issuer", ""))

    checks = [
        (issuer_url.hostname == fetch_url.hostname, "issuer hostname mismatch"),
        (issuer_url.scheme == "https", "issuer must be HTTPS"),
        (issuer_url.port is None, "issuer must not specify port"),
        (issuer_url.path in ["", "/"], "issuer must not have path"),
        (issuer_url.fragment == "", "issuer must not have fragment"),
        ("code" in meta.get("response_types_supported", []), "code response type required"),
        (
            "authorization_code" in meta.get("grant_types_supported", []),
            "authorization_code grant required",
        ),
        (
            "refresh_token" in meta.get("grant_types_supported", []),
            "refresh_token grant required",
        ),
        ("S256" in meta.get("code_challenge_methods_supported", []), "S256 PKCE required"),
        (
            "private_key_jwt" in meta.get("token_endpoint_auth_methods_supported", []),
            "private_key_jwt auth method required",
        ),
        (
            "ES256" in meta.get("token_endpoint_auth_signing_alg_values_supported", []),
            "ES256 signing required",
        ),
        ("atproto" in meta.get("scopes_supported", []), "atproto scope required"),
        (
            meta.get("authorization_response_iss_parameter_supported") is True,
            "ISS parameter support required",
        ),
        (
            meta.get("pushed_authorization_request_endpoint") is not None,
            "PAR endpoint required",
        ),
        (
            meta.get("require_pushed_authorization_requests") is True,
            "PAR must be required",
        ),
        ("ES256" in meta.get("dpop_signing_alg_values_supported", []), "ES256 DPoP required"),
        (
            meta.get("client_id_metadata_document_supported") is True,
            "client_id metadata document support required",
        ),
    ]

    for condition, message in checks:
        if not condition:
            raise ValueError(f"Invalid auth server metadata: {message}")


def resolve_pds_authserver(client: httpx.Client, pds_url: str) -> str:
    """Resolve a PDS URL to its Authorization Server URL."""
    if not is_safe_url(pds_url):
        raise ValueError(f"Unsafe PDS URL: {pds_url}")
    resp = client.get(
        f"{pds_url}/.well-known/oauth-protected-resource",
        follow_redirects=False,
        timeout=SAFE_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    if resp.status_code != 200:
        raise ValueError(f"Unexpected status {resp.status_code} from PDS oauth-protected-resource")
    return resp.json()["authorization_servers"][0]


def fetch_authserver_meta(client: httpx.Client, url: str) -> dict:
    """Fetch and validate Authorization Server metadata."""
    if not is_safe_url(url):
        raise ValueError(f"Unsafe auth server URL: {url}")
    resp = client.get(
        f"{url}/.well-known/oauth-authorization-server",
        follow_redirects=False,
        timeout=SAFE_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    meta = resp.json()
    validate_authserver_meta(meta, url)
    return meta


def client_assertion_jwt(
    client_id: str, authserver_url: str, client_secret_jwk: JsonWebKey
) -> str:
    """Create a self-signed client assertion JWT for auth server requests."""
    now = int(time.time())
    return jwt.encode(
        {"alg": "ES256", "kid": client_secret_jwk["kid"]},
        {
            "iss": client_id,
            "sub": client_id,
            "aud": authserver_url,
            "jti": generate_token(),
            "iat": now,
            "exp": now + 60,
        },
        client_secret_jwk,
    ).decode("utf-8")


def _dpop_jwt(
    method: str,
    url: str,
    nonce: str,
    dpop_private_jwk: JsonWebKey,
    access_token: str | None = None,
    expiry: int = 30,
) -> str:
    """Create a DPoP proof JWT.

    When access_token is provided, includes an 'ath' (access token hash) claim
    for resource server (PDS) requests.
    """
    dpop_pub_jwk = json.loads(dpop_private_jwk.as_json(is_private=False))
    now = int(time.time())
    body = {
        "jti": generate_token(),
        "htm": method,
        "htu": url,
        "iat": now,
        "exp": now + expiry,
    }
    if nonce:
        body["nonce"] = nonce
    if access_token:
        body["ath"] = create_s256_code_challenge(access_token)
    return jwt.encode(
        {"typ": "dpop+jwt", "alg": "ES256", "jwk": dpop_pub_jwk},
        body,
        dpop_private_jwk,
    ).decode("utf-8")


def _parse_www_authenticate(data: str) -> tuple[str, dict]:
    """Minimal WWW-Authenticate header parser for DPoP nonce errors."""
    scheme, _, params = data.partition(" ")
    items = urllib.request.parse_http_list(params)
    opts = urllib.request.parse_keqv_list(items)
    return scheme, opts


def _is_dpop_nonce_error(resp: httpx.Response) -> bool:
    """Check if a response indicates a DPoP nonce is needed or has changed.

    Servers signal this via either:
    1. WWW-Authenticate header with error="use_dpop_nonce"
    2. JSON response body with error="use_dpop_nonce"
    """
    if resp.status_code not in [400, 401]:
        return False

    www_auth = resp.headers.get("WWW-Authenticate")
    if www_auth:
        try:
            scheme, params = _parse_www_authenticate(www_auth)
            if scheme.lower() == "dpop" and params.get("error") == "use_dpop_nonce":
                return True
        except Exception:
            pass

    try:
        body = resp.json()
        if isinstance(body, dict) and body.get("error") == "use_dpop_nonce":
            return True
    except Exception:
        pass

    return False


def _authserver_post(
    client: httpx.Client,
    authserver_url: str,
    client_id: str,
    client_secret_jwk: JsonWebKey,
    dpop_private_jwk: JsonWebKey,
    dpop_nonce: str,
    post_url: str,
    post_data: dict,
) -> tuple[str, httpx.Response]:
    """POST to an auth server endpoint with client assertion and DPoP proof.

    Handles DPoP nonce errors by retrying once with the server-provided nonce.
    Returns the (possibly updated) DPoP nonce and the response.
    """
    assertion = client_assertion_jwt(client_id, authserver_url, client_secret_jwk)
    post_data = {
        **post_data,
        "client_id": client_id,
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion": assertion,
    }

    if not is_safe_url(post_url):
        raise ValueError(f"Unsafe auth server URL: {post_url}")

    dpop_proof = _dpop_jwt("POST", post_url, dpop_nonce, dpop_private_jwk)
    resp = client.post(
        post_url,
        data=post_data,
        headers={"DPoP": dpop_proof},
        follow_redirects=False,
        timeout=SAFE_HTTP_TIMEOUT,
    )

    if _is_dpop_nonce_error(resp):
        dpop_nonce = resp.headers["DPoP-Nonce"]
        log.debug("Retrying with new auth server DPoP nonce")
        dpop_proof = _dpop_jwt("POST", post_url, dpop_nonce, dpop_private_jwk)
        resp = client.post(
            post_url,
            data=post_data,
            headers={"DPoP": dpop_proof},
            follow_redirects=False,
            timeout=SAFE_HTTP_TIMEOUT,
        )

    return dpop_nonce, resp


def send_par_request(
    client: httpx.Client,
    authserver_url: str,
    authserver_meta: dict,
    login_hint: str | None,
    client_id: str,
    redirect_uri: str,
    scope: str,
    client_secret_jwk: JsonWebKey,
    dpop_private_jwk: JsonWebKey,
) -> tuple[str, str, str, httpx.Response]:
    """Send a Pushed Authorization Request (PAR).

    Returns (pkce_verifier, state, dpop_nonce, response).
    """
    par_url = authserver_meta["pushed_authorization_request_endpoint"]
    if not is_safe_url(par_url):
        raise ValueError(f"Unsafe PAR URL: {par_url}")

    state = generate_token()
    pkce_verifier = generate_token(48)
    code_challenge = create_s256_code_challenge(pkce_verifier)

    par_body = {
        "response_type": "code",
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
        "redirect_uri": redirect_uri,
        "scope": scope,
    }
    if login_hint:
        par_body["login_hint"] = login_hint

    dpop_nonce, resp = _authserver_post(
        client=client,
        authserver_url=authserver_url,
        client_id=client_id,
        client_secret_jwk=client_secret_jwk,
        dpop_private_jwk=dpop_private_jwk,
        dpop_nonce="",
        post_url=par_url,
        post_data=par_body,
    )

    return pkce_verifier, state, dpop_nonce, resp


def initial_token_request(
    client: httpx.Client,
    auth_request: dict,
    code: str,
    client_id: str,
    redirect_uri: str,
    client_secret_jwk: JsonWebKey,
) -> tuple[dict, str]:
    """Exchange an authorization code for tokens.

    Returns (token_response, dpop_nonce).
    The caller must verify token_response["sub"] matches the expected DID.
    """
    authserver_url = auth_request["authserver_iss"]
    authserver_meta = fetch_authserver_meta(client, authserver_url)
    token_url = authserver_meta["token_endpoint"]

    if not is_safe_url(token_url):
        raise ValueError(f"Unsafe token URL: {token_url}")

    dpop_private_jwk = JsonWebKey.import_key(json.loads(auth_request["dpop_private_jwk"]))

    dpop_nonce, resp = _authserver_post(
        client=client,
        authserver_url=authserver_url,
        client_id=client_id,
        client_secret_jwk=client_secret_jwk,
        dpop_private_jwk=dpop_private_jwk,
        dpop_nonce=auth_request["dpop_authserver_nonce"],
        post_url=token_url,
        post_data={
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": auth_request["pkce_verifier"],
            "redirect_uri": redirect_uri,
        },
    )

    resp.raise_for_status()
    return resp.json(), dpop_nonce


def refresh_token_request(
    client: httpx.Client,
    session: dict,
    client_id: str,
    client_secret_jwk: JsonWebKey,
) -> tuple[dict, str]:
    """Refresh an access token.

    Returns (token_response, dpop_nonce).
    """
    authserver_url = session["authserver_iss"]
    authserver_meta = fetch_authserver_meta(client, authserver_url)
    token_url = authserver_meta["token_endpoint"]

    if not is_safe_url(token_url):
        raise ValueError(f"Unsafe token URL: {token_url}")

    dpop_private_jwk = JsonWebKey.import_key(json.loads(session["dpop_private_jwk"]))

    dpop_nonce, resp = _authserver_post(
        client=client,
        authserver_url=authserver_url,
        client_id=client_id,
        client_secret_jwk=client_secret_jwk,
        dpop_private_jwk=dpop_private_jwk,
        dpop_nonce=session["dpop_authserver_nonce"],
        post_url=token_url,
        post_data={
            "grant_type": "refresh_token",
            "refresh_token": session["refresh_token"],
        },
    )

    if resp.status_code not in [200, 201]:
        log.error("Token refresh error: %s", resp.json())
    resp.raise_for_status()
    return resp.json(), dpop_nonce


def revoke_token_request(
    client: httpx.Client,
    session: dict,
    client_id: str,
    client_secret_jwk: JsonWebKey,
) -> None:
    """Revoke access and refresh tokens."""
    authserver_url = session["authserver_iss"]
    authserver_meta = fetch_authserver_meta(client, authserver_url)

    revoke_url = authserver_meta.get("revocation_endpoint")
    if not revoke_url:
        log.info("Auth server does not support token revocation")
        return

    if not is_safe_url(revoke_url):
        raise ValueError(f"Unsafe revocation URL: {revoke_url}")

    dpop_private_jwk = JsonWebKey.import_key(json.loads(session["dpop_private_jwk"]))
    dpop_nonce = session["dpop_authserver_nonce"]

    for token_type in ["access_token", "refresh_token"]:
        dpop_nonce, resp = _authserver_post(
            client=client,
            authserver_url=authserver_url,
            client_id=client_id,
            client_secret_jwk=client_secret_jwk,
            dpop_private_jwk=dpop_private_jwk,
            dpop_nonce=dpop_nonce,
            post_url=revoke_url,
            post_data={
                "token": session[token_type],
                "token_type_hint": token_type,
            },
        )
        resp.raise_for_status()


def pds_authed_request(
    client: httpx.Client,
    method: str,
    url: str,
    session: dict,
    body: dict | None = None,
) -> tuple[httpx.Response, str]:
    """Make an authenticated request to a user's PDS.

    Returns (response, updated_dpop_pds_nonce). The caller is responsible for
    persisting the updated nonce.
    """
    dpop_private_jwk = JsonWebKey.import_key(json.loads(session["dpop_private_jwk"]))
    dpop_pds_nonce = session.get("dpop_pds_nonce") or ""
    access_token = session["access_token"]

    for _ in range(2):
        dpop_proof = _dpop_jwt(
            method,
            url,
            dpop_pds_nonce,
            dpop_private_jwk,
            access_token=access_token,
            expiry=10,
        )

        headers = {
            "Authorization": f"DPoP {access_token}",
            "DPoP": dpop_proof,
        }

        if method.upper() == "GET":
            resp = client.get(
                url, headers=headers, follow_redirects=False, timeout=SAFE_HTTP_TIMEOUT
            )
        else:
            resp = client.post(
                url, headers=headers, json=body, follow_redirects=False, timeout=SAFE_HTTP_TIMEOUT
            )

        if _is_dpop_nonce_error(resp):
            dpop_pds_nonce = resp.headers["DPoP-Nonce"]
            log.debug("Retrying with new PDS DPoP nonce")
            continue
        break

    return resp, dpop_pds_nonce
