import logging
from datetime import datetime, timezone
from io import BytesIO

import fitparse
import polyline

log = logging.getLogger(__name__)


def parse_file(filename: str, data: bytes) -> dict:
    """Detect file format from extension and dispatch to the appropriate parser.

    Returns a dict shaped to match the app.thedistance.activity record schema,
    using snake_case keys matching the database column names.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext == "fit":
        return parse_fit(data)
    else:
        raise ValueError(f"Unsupported file format: .{ext}")


def parse_fit(data: bytes) -> dict:
    """Parse a FIT file and return an activity record dict.

    FIT files contain multiple message types. We extract three:
    - "session": summary stats for the whole activity (times, distance, averages)
    - "record": per-second data points with GPS coordinates and sensor readings
    - "device_info": device metadata (we grab the first product_name we find)

    GPS coordinates in FIT are stored as semicircles (32-bit integers). We convert
    them to degrees and encode the full route as a Google encoded polyline.
    """
    fit = fitparse.FitFile(BytesIO(data))
    fit.parse()

    session = None
    records = []
    device_name = None

    for msg in fit.get_messages():
        if msg.name == "session":
            session = msg
        elif msg.name == "record":
            records.append(msg)
        elif msg.name == "device_info" and not device_name:
            name = msg.get_value("product_name")
            if name:
                device_name = name

    if not session:
        raise ValueError("No session data found in FIT file")

    # Use sub_sport for more specificity (e.g. "gravel_cycling" instead of "cycling"),
    # falling back to sport if sub_sport is absent or "generic"
    sport = session.get_value("sport")
    sub_sport = session.get_value("sub_sport")
    sport_type = sub_sport if sub_sport and sub_sport != "generic" else sport

    start_time = session.get_value("start_time")
    if isinstance(start_time, datetime):
        started_at = start_time.replace(tzinfo=timezone.utc).isoformat()
    else:
        started_at = str(start_time)

    elapsed_time = session.get_value("total_elapsed_time")
    moving_time = session.get_value("total_timer_time")
    distance = session.get_value("total_distance")
    elevation_gain = session.get_value("total_ascent")
    avg_speed = session.get_value("avg_speed")
    max_speed = session.get_value("max_speed")
    avg_heart_rate = session.get_value("avg_heart_rate")
    max_heart_rate = session.get_value("max_heart_rate")
    avg_cadence = session.get_value("avg_cadence")
    max_cadence = session.get_value("max_cadence")
    avg_power = session.get_value("avg_power")
    max_power = session.get_value("max_power")
    calories = session.get_value("total_calories")

    # Build polyline from per-second GPS records
    route_points = []
    for rec in records:
        lat = rec.get_value("position_lat")
        lon = rec.get_value("position_long")
        if lat is not None and lon is not None:
            lat_deg = lat * (180 / 2**31)
            lon_deg = lon * (180 / 2**31)
            route_points.append((lat_deg, lon_deg))

    encoded_polyline = polyline.encode(route_points) if route_points else None

    # Generate a default title from time of day and sport type
    sport_display_names = {"cycling": "Ride", "walking": "Walk", "hiking": "Hike"}
    sport_display = sport_display_names.get(
        str(sport_type), str(sport_type).replace("_", " ").title()
    )

    if isinstance(start_time, datetime):
        hour = start_time.hour
    else:
        hour = 12

    if hour < 12:
        time_of_day = "Morning"
    elif hour < 17:
        time_of_day = "Afternoon"
    else:
        time_of_day = "Evening"

    # Build the activity dict. Required fields are always included.
    # Optional fields are only included when present in the FIT data.
    activity = {
        "title": f"{time_of_day} {sport_display}",
        "sport_type": str(sport_type) if sport_type else "unknown",
        "started_at": started_at,
        "elapsed_time": int(elapsed_time) if elapsed_time else 0,
        "moving_time": int(moving_time) if moving_time else 0,
        "distance": str(round(distance, 1)) if distance else "0",
        "source": "fit-file",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    if elevation_gain is not None:
        activity["elevation_gain"] = str(round(float(elevation_gain), 1))
    if avg_speed is not None:
        activity["avg_speed"] = str(round(float(avg_speed), 3))
    if max_speed is not None:
        activity["max_speed"] = str(round(float(max_speed), 3))
    if avg_heart_rate is not None:
        activity["avg_heart_rate"] = int(avg_heart_rate)
    if max_heart_rate is not None:
        activity["max_heart_rate"] = int(max_heart_rate)
    if avg_cadence is not None:
        activity["avg_cadence"] = int(avg_cadence)
    if max_cadence is not None:
        activity["max_cadence"] = int(max_cadence)
    if avg_power is not None:
        activity["avg_power"] = int(avg_power)
    if max_power is not None:
        activity["max_power"] = int(max_power)
    if calories is not None:
        activity["calories"] = int(calories)
    if encoded_polyline:
        activity["polyline"] = encoded_polyline
    if device_name:
        activity["device"] = device_name

    return activity
