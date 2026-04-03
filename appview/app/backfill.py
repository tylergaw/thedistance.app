import logging

import httpx

from app.db import get_connection, upsert_activity

log = logging.getLogger(__name__)

RESOLVE_HANDLE_URL = "https://bsky.social/xrpc/com.atproto.identity.resolveHandle"
PLC_DIRECTORY_URL = "https://plc.directory"
COLLECTION = "app.thedistance.activity"


def resolve_handle(client, handle):
    resp = client.get(RESOLVE_HANDLE_URL, params={"handle": handle})
    resp.raise_for_status()
    return resp.json()["did"]


def resolve_pds(client, did):
    resp = client.get(f"{PLC_DIRECTORY_URL}/{did}")
    resp.raise_for_status()
    doc = resp.json()
    for service in doc.get("service", []):
        if service.get("id") == "#atproto_pds":
            return service["serviceEndpoint"]
    raise ValueError(f"No PDS found in DID document for {did}")


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


def backfill(handle):
    log.info("Starting backfill for %s", handle)

    with httpx.Client() as client:
        did = resolve_handle(client, handle)
        log.info("Resolved %s to %s", handle, did)

        pds = resolve_pds(client, did)
        log.info("PDS for %s: %s", did, pds)

        conn = get_connection()
        count = 0
        try:
            for rkey, record in list_records(client, pds, did):
                upsert_activity(conn, did, rkey, record)
                count += 1
                log.info("Backfilled %s/%s", did, rkey)
        finally:
            conn.close()

    log.info("Backfill complete for %s: %d records", handle, count)
    return {"did": did, "records": count}
