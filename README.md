# The Distance

⚠️ Extremely WIP

Decentralized activity tracker on the [AT Protocol](https://atproto.com)

**Domain:** thedistance.app
**Lexicon:** [`app.thedistance.activity`](https://lexicon.garden/lexicon/did:plc:gbye6kw5wlaaa2kaocnqswm3/app.thedistance.activity)

## Local Development

### Structure

- `lexicons/` — AT Protocol lexicon schemas
- `appview/` — Backend: Jetstream subscriber + API server
- `web/` — Frontend
- `scripts/` — Data conversion tools (Strava → activity records)

### System requirements

- [Python 3.12+](https://www.python.org/)
- [uv](https://docs.astral.sh/uv/) — Python package manager
- [PostgreSQL](https://www.postgresql.org/) — running locally, no Docker

You will need a local Postgres database created and running. Set the connection string in your `.env` file (see AppView setup below).

## AppView

The appview is made up of two processes that run simultaneously: a **subscriber** and an **API server**. The subscriber connects to Jetstream and listens for `app.thedistance.activity` records on the network. When it sees one created, updated, or deleted, it writes the change to Postgres. The API server reads from that same database and serves the indexed data over HTTP. Both processes need to be running at the same time, each in its own terminal.

### Setup

```
cd appview
uv sync
```

Copy the example env file and fill in your Postgres connection string:

```
cp .env.example .env
```

### Run the API server

```
uv run start
```

Serves activity data on `http://localhost:8000`.

### Run the subscriber

```
uv run subscribe
```

## Frontend

```
cd web
python -m http.server 8001
```

## Scripts

⚠️ Very crusty, probably doesn't work anymore

Convert Strava export data to activity records:

```
python scripts/strava-to-records.py rides.json -o records/
```

Create a record on your PDS:

```
goat record create --no-validate records/<id>.json
```
