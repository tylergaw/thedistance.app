from fastapi import Header, HTTPException


def require_auth(
    authorization: str | None = Header(default=None),
    x_debug_did: str | None = Header(default=None),
) -> str:
    """Verify the request is authenticated and return the user's DID.

    Placeholder until real AT Protocol OAuth is in place. For now, requires
    an Authorization header (value is not validated) and reads the DID from
    X-Debug-DID. Once OAuth is implemented, the DID will come from the
    access token's sub claim.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    if not x_debug_did:
        raise HTTPException(status_code=401, detail="Missing X-Debug-DID header")
    return x_debug_did
