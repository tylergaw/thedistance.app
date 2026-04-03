from fastapi import BackgroundTasks, Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.auth import require_auth
from app.backfill import backfill as run_backfill
from app.backfill import resolve_handle
from app.db import get_activity as _get
from app.db import get_connection, init_db
from app.db import list_activities as _list

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def row_to_dict(row):
    return dict(row) if row else None


@app.on_event("startup")
def startup():
    init_db()


@app.get("/api/activities")
def list_activities(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    sport_type: str | None = Query(default=None),
):
    conn = get_connection()
    try:
        rows = _list(conn, limit=limit, offset=offset, sport_type=sport_type)
        return [row_to_dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/activities/{did}")
def list_user_activities(
    did: str,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    sport_type: str | None = Query(default=None),
):
    conn = get_connection()
    try:
        rows = _list(conn, limit=limit, offset=offset, sport_type=sport_type, did=did)
        return [row_to_dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/activities/{did}/{rkey}")
def get_activity_endpoint(did: str, rkey: str):
    conn = get_connection()
    try:
        row = _get(conn, did, rkey)
        if not row:
            return JSONResponse(status_code=404, content={"error": "Not found"})
        return row_to_dict(row)
    finally:
        conn.close()


class BackfillRequest(BaseModel):
    handle: str


@app.post("/api/backfill")
def backfill_endpoint(
    req: BackfillRequest,
    background_tasks: BackgroundTasks,
    did: str = Depends(require_auth),
):
    import httpx

    with httpx.Client() as client:
        handle_did = resolve_handle(client, req.handle)

    if handle_did != did:
        return JSONResponse(status_code=403, content={"error": "You can only backfill your own account"})

    background_tasks.add_task(run_backfill, req.handle)
    return {"status": "started", "handle": req.handle}
