from fastapi import HTTPException, Request

from app.db import get_connection, get_oauth_session


def require_auth(request: Request) -> dict:
    """Verify the request has a valid OAuth session and return the session dict.

    Reads user_did from the session cookie, then looks up the full OAuth
    session from the database. Returns the session row as a dict.
    """
    user_did = request.session.get("user_did")
    if not user_did:
        raise HTTPException(status_code=401, detail="Not authenticated")

    conn = get_connection()
    try:
        session = get_oauth_session(conn, user_did)
    finally:
        conn.close()

    if not session:
        request.session.clear()
        raise HTTPException(status_code=401, detail="Session expired")

    return session
