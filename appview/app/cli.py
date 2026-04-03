import sys

import uvicorn


def start():
    uvicorn.run("app.main:app", reload=True)


def subscribe():
    import asyncio

    from app.subscriber import subscribe as _subscribe

    asyncio.run(_subscribe())


def backfill():
    import logging

    from app.backfill import backfill as _backfill

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if len(sys.argv) < 2:
        print("Usage: uv run backfill <handle>")
        sys.exit(1)

    handle = sys.argv[1]
    result = _backfill(handle)
    print(f"Backfilled {result['records']} records for {result['did']}")
