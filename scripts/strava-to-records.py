#!/usr/bin/env python
"""Convert Strava export JSON to app.thedistance.activity record files."""

import argparse
import json
import os
from datetime import datetime, timezone


def convert_ride(ride):
    """Convert a single Strava ride to an app.thedistance.activity record."""
    record = {
        "$type": "app.thedistance.activity",
        "sportType": ride["sport_type"].lower(),
        "startedAt": ride["start_date"],
        "elapsedTime": ride["elapsed_time"],
        "movingTime": ride["moving_time"],
        "distance": str(ride["distance"]),
        "source": "strava",
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }

    if ride.get("name"):
        record["title"] = ride["name"]

    if ride.get("total_elevation_gain") is not None:
        record["elevationGain"] = str(ride["total_elevation_gain"])

    if ride.get("average_speed") is not None:
        record["avgSpeed"] = str(ride["average_speed"])

    if ride.get("max_speed") is not None:
        record["maxSpeed"] = str(ride["max_speed"])

    if ride.get("average_heartrate") is not None:
        record["avgHeartRate"] = round(ride["average_heartrate"])

    if ride.get("max_heartrate") is not None:
        record["maxHeartRate"] = round(ride["max_heartrate"])

    if ride.get("average_watts") is not None:
        record["avgPower"] = round(ride["average_watts"])

    if ride.get("device_name"):
        record["device"] = ride["device_name"]

    polyline = ride.get("map", {}).get("summary_polyline")
    if polyline:
        record["polyline"] = polyline

    return record


def main():
    parser = argparse.ArgumentParser(
        description="Convert Strava rides JSON to activity records"
    )
    parser.add_argument("input", help="Path to Strava rides JSON file")
    parser.add_argument(
        "-o", "--output", default="records", help="Output directory (default: records)"
    )
    args = parser.parse_args()

    with open(args.input) as f:
        rides = json.load(f)

    os.makedirs(args.output, exist_ok=True)

    for ride in rides:
        record = convert_ride(ride)
        filename = f"{ride['id']}.json"
        filepath = os.path.join(args.output, filename)
        with open(filepath, "w") as f:
            json.dump(record, f, indent=2)
        print(f"  {filename}")

    print(f"\nConverted {len(rides)} rides to {args.output}/")


if __name__ == "__main__":
    main()
