import logging

import httpx

from app.db import get_connection, has_profile, upsert_activity, upsert_profile
from app.identity import fetch_profile, resolve_identity

log = logging.getLogger(__name__)

COLLECTION = "app.thedistance.activity"


def list_records(client, pds, did):
    url = f"{pds}/xrpc/com.atproto.repo.listRecords"
    cursor = None

    while True:
        params = {"repo": did, "collection": COLLECTION, "limit": 100}
        if cursor:
            params["cursor"] = cursor

        resp = client.get(url, params=params)
        resp.raise_for_status()
        body = resp.json()

        for item in body.get("records", []):
            uri = item["uri"]
            rkey = uri.rsplit("/", 1)[-1]
            yield rkey, item["value"]

        cursor = body.get("cursor")
        if not cursor:
            break


def backfill(identifier):
    log.info("Starting backfill for %s", identifier)

    with httpx.Client() as client:
        did, resolved_handle, pds = resolve_identity(client, identifier)
        log.info("Resolved %s to %s (PDS: %s)", resolved_handle, did, pds)

        conn = get_connection()
        count = 0
        try:
            for rkey, record in list_records(client, pds, did):
                upsert_activity(conn, did, rkey, record)
                count += 1
                log.info("Backfilled %s/%s", did, rkey)

            if not has_profile(conn, did):
                profile = fetch_profile(client, did, pds)
                if profile:
                    upsert_profile(
                        conn, did, resolved_handle,
                        profile["display_name"],
                        profile["description"],
                        profile["avatar_url"],
                    )
                else:
                    upsert_profile(conn, did, resolved_handle, None, None, None)
                log.info("Backfilled profile for %s", did)
        finally:
            conn.close()

    log.info("Backfill complete for %s: %d records", identifier, count)
    return {"did": did, "records": count}
