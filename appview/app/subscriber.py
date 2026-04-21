import asyncio
import json
import logging

import websockets

from app.db import (
    delete_activity,
    get_connection,
    get_cursor,
    get_oauth_session,
    has_profile,
    init_db,
    set_cursor,
    upsert_activity,
    upsert_profile,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
COLLECTIONS = [
    "app.thedistance.activity",
    "app.thedistance.follow",
    "app.thedistance.like",
    "app.bsky.actor.profile",
]
CURSOR_PERSIST_INTERVAL = 50  # persist cursor every N events


def build_url(cursor=None):
    params = "&".join(f"wantedCollections={c}" for c in COLLECTIONS)
    url = f"{JETSTREAM_URL}?{params}"
    if cursor:
        url += f"&cursor={cursor}"
    return url


def handle_profile_event(conn, did, op, commit):
    if not has_profile(conn, did):
        return

    session = get_oauth_session(conn, did)
    if not session:
        return

    handle = session["handle"]

    if op == "delete":
        upsert_profile(conn, did, handle, None, None, None)
        log.info("Cleared profile for %s", did)
        return

    record = commit.get("record")
    if not record:
        return

    pds_url = session["pds_url"]
    avatar_url = None
    avatar = record.get("avatar")
    if avatar and isinstance(avatar, dict):
        ref = avatar.get("ref", {})
        cid = ref.get("$link")
        if cid:
            avatar_url = (
                f"{pds_url}/xrpc/com.atproto.sync.getBlob"
                f"?did={did}&cid={cid}"
            )

    upsert_profile(
        conn, did, handle,
        record.get("displayName"),
        record.get("description"),
        avatar_url,
    )
    log.info("Updated profile for %s", did)


async def subscribe():
    init_db()
    conn = get_connection()
    cursor = get_cursor(conn)
    events_since_persist = 0

    log.info("Starting subscriber, cursor=%s", cursor)

    while True:
        url = build_url(cursor)
        try:
            async with websockets.connect(url) as ws:
                log.info("Connected to Jetstream")
                async for raw in ws:
                    try:
                        event = json.loads(raw)
                        cursor = event.get("time_us")

                        if event.get("kind") != "commit":
                            continue

                        commit = event.get("commit", {})
                        op = commit.get("operation")
                        collection = commit.get("collection")
                        did = event.get("did")
                        rkey = commit.get("rkey")

                        if not did or not rkey:
                            continue

                        if collection == "app.thedistance.activity":
                            if op in ("create", "update"):
                                record = commit.get("record")
                                if record:
                                    upsert_activity(conn, did, rkey, record)
                                    log.info("Indexed %s %s/%s", op, did, rkey)
                            elif op == "delete":
                                delete_activity(conn, did, rkey)
                                log.info("Deleted %s/%s", did, rkey)

                        elif collection == "app.bsky.actor.profile":
                            handle_profile_event(conn, did, op, commit)

                    except Exception:
                        log.exception("Failed to process event: %s", raw[:200])

                    events_since_persist += 1
                    if events_since_persist >= CURSOR_PERSIST_INTERVAL:
                        set_cursor(conn, cursor)
                        events_since_persist = 0

        except websockets.ConnectionClosed:
            log.warning("Connection closed, reconnecting in 5s...")
            if cursor:
                set_cursor(conn, cursor)
            await asyncio.sleep(5)
        except Exception:
            log.exception("Unexpected error, reconnecting in 10s...")
            if cursor:
                set_cursor(conn, cursor)
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(subscribe())
