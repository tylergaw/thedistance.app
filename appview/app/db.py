import psycopg
from psycopg.rows import dict_row

from app.config import get_settings


def get_connection():
    return psycopg.connect(get_settings().database_url, row_factory=dict_row)


def init_db():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS activities (
                did TEXT NOT NULL,
                rkey TEXT NOT NULL,
                sport_type TEXT NOT NULL,
                title TEXT,
                description TEXT,
                started_at TIMESTAMPTZ NOT NULL,
                elapsed_time INTEGER NOT NULL,
                moving_time INTEGER NOT NULL,
                distance TEXT NOT NULL,
                elevation_gain TEXT,
                avg_speed TEXT,
                max_speed TEXT,
                avg_heart_rate INTEGER,
                max_heart_rate INTEGER,
                avg_cadence INTEGER,
                max_cadence INTEGER,
                avg_power INTEGER,
                max_power INTEGER,
                calories INTEGER,
                polyline TEXT,
                device TEXT,
                source TEXT,
                created_at TIMESTAMPTZ NOT NULL,
                indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (did, rkey)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_activities_started_at
                ON activities (started_at DESC)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_activities_did
                ON activities (did)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cursor (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cursor_value BIGINT NOT NULL
            )
        """)


def upsert_activity(conn, did, rkey, record):
    conn.execute("""
        INSERT INTO activities (
            did, rkey, sport_type, title, description, started_at,
            elapsed_time, moving_time, distance, elevation_gain,
            avg_speed, max_speed, avg_heart_rate, max_heart_rate,
            avg_cadence, max_cadence, avg_power, max_power,
            calories, polyline, device, source, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (did, rkey) DO UPDATE SET
            sport_type = EXCLUDED.sport_type,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            started_at = EXCLUDED.started_at,
            elapsed_time = EXCLUDED.elapsed_time,
            moving_time = EXCLUDED.moving_time,
            distance = EXCLUDED.distance,
            elevation_gain = EXCLUDED.elevation_gain,
            avg_speed = EXCLUDED.avg_speed,
            max_speed = EXCLUDED.max_speed,
            avg_heart_rate = EXCLUDED.avg_heart_rate,
            max_heart_rate = EXCLUDED.max_heart_rate,
            avg_cadence = EXCLUDED.avg_cadence,
            max_cadence = EXCLUDED.max_cadence,
            avg_power = EXCLUDED.avg_power,
            max_power = EXCLUDED.max_power,
            calories = EXCLUDED.calories,
            polyline = EXCLUDED.polyline,
            device = EXCLUDED.device,
            source = EXCLUDED.source,
            created_at = EXCLUDED.created_at,
            indexed_at = NOW()
    """, (
        did,
        rkey,
        record["sportType"],
        record.get("title"),
        record.get("description"),
        record["startedAt"],
        record["elapsedTime"],
        record["movingTime"],
        record["distance"],
        record.get("elevationGain"),
        record.get("avgSpeed"),
        record.get("maxSpeed"),
        record.get("avgHeartRate"),
        record.get("maxHeartRate"),
        record.get("avgCadence"),
        record.get("maxCadence"),
        record.get("avgPower"),
        record.get("maxPower"),
        record.get("calories"),
        record.get("polyline"),
        record.get("device"),
        record.get("source"),
        record["createdAt"],
    ))
    conn.commit()


def delete_activity(conn, did, rkey):
    conn.execute("DELETE FROM activities WHERE did = %s AND rkey = %s", (did, rkey))
    conn.commit()


def get_cursor(conn):
    row = conn.execute("SELECT cursor_value FROM cursor WHERE id = 1").fetchone()
    return row["cursor_value"] if row else None


def set_cursor(conn, cursor_value):
    conn.execute("""
        INSERT INTO cursor (id, cursor_value) VALUES (1, %s)
        ON CONFLICT (id) DO UPDATE SET cursor_value = EXCLUDED.cursor_value
    """, (cursor_value,))
    conn.commit()


def list_activities(conn, limit=50, offset=0, sport_type=None, did=None):
    query = "SELECT * FROM activities"
    params = []
    conditions = []

    if did:
        conditions.append("did = %s")
        params.append(did)
    if sport_type:
        conditions.append("sport_type = %s")
        params.append(sport_type)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY started_at DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    return conn.execute(query, params).fetchall()


def get_activity(conn, did, rkey):
    return conn.execute(
        "SELECT * FROM activities WHERE did = %s AND rkey = %s",
        (did, rkey),
    ).fetchone()
