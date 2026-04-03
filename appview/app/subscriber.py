import asyncio
import json
import logging

import websockets

from app.db import (
    delete_activity,
    get_connection,
    get_cursor,
    init_db,
    set_cursor,
    upsert_activity,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
COLLECTION = "app.thedistance.activity"
CURSOR_PERSIST_INTERVAL = 50  # persist cursor every N events


def build_url(cursor=None):
    url = f"{JETSTREAM_URL}?wantedCollections={COLLECTION}"
    if cursor:
        url += f"&cursor={cursor}"
    return url


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
                        did = event.get("did")
                        rkey = commit.get("rkey")

                        if not did or not rkey:
                            continue

                        if op in ("create", "update"):
                            record = commit.get("record")
                            if record:
                                upsert_activity(conn, did, rkey, record)
                                log.info("Indexed %s %s/%s", op, did, rkey)
                        elif op == "delete":
                            delete_activity(conn, did, rkey)
                            log.info("Deleted %s/%s", did, rkey)
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
