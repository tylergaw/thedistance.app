# Adapted from the AT Protocol OAuth cookbook example:
# https://github.com/bluesky-social/cookbook/tree/main/python-oauth-web-app
# Original: atproto_identity.py

import logging
import re

import httpx

log = logging.getLogger(__name__)

HANDLE_REGEX = re.compile(
    r"^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$"
)
DID_REGEX = re.compile(r"^did:[a-z]+:[a-zA-Z0-9._:%-]*[a-zA-Z0-9._-]$")

SAFE_HTTP_TIMEOUT = httpx.Timeout(connect=2.0, read=10.0, write=10.0, pool=10.0)


def is_valid_handle(handle: str) -> bool:
    return HANDLE_REGEX.match(handle) is not None


def is_valid_did(did: str) -> bool:
    return DID_REGEX.match(did) is not None


def resolve_handle(client: httpx.Client, handle: str) -> str | None:
    """Resolve an AT Protocol handle to a DID.

    Tries the HTTP well-known method first, then falls back to the XRPC endpoint.
    """
    # HTTP well-known
    try:
        resp = client.get(
            f"https://{handle}/.well-known/atproto-did",
            follow_redirects=False,
            timeout=SAFE_HTTP_TIMEOUT,
        )
        if resp.status_code == 200:
            did = resp.text.strip().split()[0]
            if is_valid_did(did):
                return did
    except httpx.HTTPError as e:
        log.debug("HTTP well-known handle resolution failed for %s: %s", handle, e)

    # XRPC fallback
    try:
        resp = client.get(
            "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
            params={"handle": handle},
            timeout=SAFE_HTTP_TIMEOUT,
        )
        if resp.status_code == 200:
            did = resp.json().get("did")
            if did and is_valid_did(did):
                return did
    except httpx.HTTPError as e:
        log.debug("XRPC handle resolution failed for %s: %s", handle, e)

    return None


def resolve_did(client: httpx.Client, did: str) -> dict | None:
    """Resolve a DID to its DID document."""
    if did.startswith("did:plc:"):
        try:
            resp = client.get(
                f"https://plc.directory/{did}",
                timeout=SAFE_HTTP_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()
        except httpx.HTTPError as e:
            log.debug("PLC directory resolution failed for %s: %s", did, e)
        return None

    if did.startswith("did:web:"):
        domain = did[8:]
        if not is_valid_handle(domain):
            log.warning("did:web domain failed validation: %s", domain)
            return None
        try:
            resp = client.get(
                f"https://{domain}/.well-known/did.json",
                follow_redirects=False,
                timeout=SAFE_HTTP_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()
        except httpx.HTTPError as e:
            log.debug("did:web resolution failed for %s: %s", did, e)
        return None

    log.warning("Unsupported DID method: %s", did)
    return None


def pds_endpoint(doc: dict) -> str:
    """Extract the PDS service endpoint from a DID document."""
    for svc in doc.get("service", []):
        if svc.get("id") == "#atproto_pds":
            return svc["serviceEndpoint"]
    raise ValueError("No PDS endpoint found in DID document")


def handle_from_doc(doc: dict) -> str | None:
    """Extract the handle from a DID document's alsoKnownAs field."""
    for aka in doc.get("alsoKnownAs", []):
        if aka.startswith("at://"):
            handle = aka[5:]
            if is_valid_handle(handle):
                return handle
    return None


def resolve_identity(client: httpx.Client, identifier: str) -> tuple[str, str, str]:
    """Resolve a handle or DID to a verified (did, handle, pds_url) tuple.

    Performs bi-directional verification: handle -> DID -> handle round-trip.
    """
    if is_valid_handle(identifier):
        handle = identifier
        did = resolve_handle(client, handle)
        if not did:
            raise ValueError(f"Failed to resolve handle: {handle}")
        doc = resolve_did(client, did)
        if not doc:
            raise ValueError(f"Failed to resolve DID: {did}")
        doc_handle = handle_from_doc(doc)
        if doc_handle != handle:
            raise ValueError(f"Handle mismatch: expected {handle}, got {doc_handle}")
        pds_url = pds_endpoint(doc)
        return did, handle, pds_url

    if is_valid_did(identifier):
        did = identifier
        doc = resolve_did(client, did)
        if not doc:
            raise ValueError(f"Failed to resolve DID: {did}")
        handle = handle_from_doc(doc)
        if not handle:
            raise ValueError(f"No handle found in DID document for {did}")
        verified_did = resolve_handle(client, handle)
        if verified_did != did:
            raise ValueError(f"Handle {handle} resolved to {verified_did}, expected {did}")
        pds_url = pds_endpoint(doc)
        return did, handle, pds_url

    raise ValueError(f"Identifier is not a valid handle or DID: {identifier}")
