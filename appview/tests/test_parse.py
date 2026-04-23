from pathlib import Path

import pytest

from app.parse import parse_file, parse_fit, parse_gpx, parse_tcx

FIXTURES = Path(__file__).parent / "fixtures"


def get_fixture(filename):
    path = FIXTURES / filename
    if not path.exists():
        pytest.skip(f"Fixture file not found: {filename}")
    return path.read_bytes()


class TestParseFile:
    def test_unsupported_format(self):
        with pytest.raises(ValueError, match="Unsupported file format"):
            parse_file("activity.xyz", b"")

    def test_no_extension(self):
        with pytest.raises(ValueError, match="Unsupported file format"):
            parse_file("activity", b"")

    def test_dispatches_to_fit(self):
        data = get_fixture("ride.fit")
        result = parse_file("ride.fit", data)
        assert result["source"] == "fit-file"

    def test_dispatches_to_gpx(self):
        data = get_fixture("ride.gpx")
        result = parse_file("ride.gpx", data)
        assert result["source"] == "gpx-file"

    def test_dispatches_to_tcx(self):
        data = get_fixture("ride.tcx")
        result = parse_file("ride.tcx", data)
        assert result["source"] == "tcx-file"


class TestParseFit:
    @pytest.fixture()
    def ride(self):
        return parse_fit(get_fixture("ride.fit"))

    def test_required_fields_present(self, ride):
        for field in ["sport_type", "started_at", "elapsed_time", "moving_time",
                      "distance", "source", "created_at"]:
            assert field in ride, f"Missing required field: {field}"

    def test_source_is_fit_file(self, ride):
        assert ride["source"] == "fit-file"

    def test_sport_type_is_string(self, ride):
        assert isinstance(ride["sport_type"], str)
        assert ride["sport_type"] != "unknown"

    def test_times_are_positive_integers(self, ride):
        assert isinstance(ride["elapsed_time"], int)
        assert isinstance(ride["moving_time"], int)
        assert ride["elapsed_time"] > 0
        assert ride["moving_time"] > 0

    def test_distance_is_string_encoded_number(self, ride):
        assert isinstance(ride["distance"], str)
        assert float(ride["distance"]) > 0

    def test_started_at_is_iso_format(self, ride):
        assert "T" in ride["started_at"]

    def test_optional_numeric_fields_have_correct_types(self, ride):
        str_fields = ["elevation_gain", "avg_speed", "max_speed"]
        int_fields = ["avg_heart_rate", "max_heart_rate", "avg_cadence",
                      "max_cadence", "avg_power", "max_power", "calories"]

        for field in str_fields:
            if field in ride:
                assert isinstance(ride[field], str), f"{field} should be a string"
                float(ride[field])  # should not raise

        for field in int_fields:
            if field in ride:
                assert isinstance(ride[field], int), f"{field} should be an int"

    def test_polyline_present_if_gps_data(self, ride):
        if "polyline" in ride:
            assert isinstance(ride["polyline"], str)
            assert len(ride["polyline"]) > 0

    def test_device_is_string_if_present(self, ride):
        if "device" in ride:
            assert isinstance(ride["device"], str)


class TestParseGpx:
    @pytest.fixture()
    def ride(self):
        return parse_gpx(get_fixture("ride.gpx"))

    def test_required_fields_present(self, ride):
        for field in ["sport_type", "started_at", "elapsed_time", "moving_time",
                      "distance", "source", "created_at"]:
            assert field in ride, f"Missing required field: {field}"

    def test_source_is_gpx_file(self, ride):
        assert ride["source"] == "gpx-file"

    def test_sport_type_from_track_type(self, ride):
        assert ride["sport_type"] == "cycling"

    def test_times_are_positive_integers(self, ride):
        assert isinstance(ride["elapsed_time"], int)
        assert isinstance(ride["moving_time"], int)
        assert ride["elapsed_time"] > 0
        assert ride["moving_time"] > 0

    def test_distance_is_string_encoded_number(self, ride):
        assert isinstance(ride["distance"], str)
        assert float(ride["distance"]) > 0

    def test_started_at_is_iso_format(self, ride):
        assert "T" in ride["started_at"]

    def test_elevation_gain_computed(self, ride):
        assert "elevation_gain" in ride
        assert isinstance(ride["elevation_gain"], str)
        assert float(ride["elevation_gain"]) > 0

    def test_polyline_present(self, ride):
        assert "polyline" in ride
        assert isinstance(ride["polyline"], str)
        assert len(ride["polyline"]) > 0

    def test_heart_rate_from_extensions(self, ride):
        assert "avg_heart_rate" in ride
        assert isinstance(ride["avg_heart_rate"], int)
        assert "max_heart_rate" in ride
        assert isinstance(ride["max_heart_rate"], int)

    def test_cadence_from_extensions(self, ride):
        assert "avg_cadence" in ride
        assert isinstance(ride["avg_cadence"], int)
        assert "max_cadence" in ride
        assert isinstance(ride["max_cadence"], int)

    def test_optional_numeric_fields_have_correct_types(self, ride):
        str_fields = ["elevation_gain", "avg_speed", "max_speed"]
        int_fields = ["avg_heart_rate", "max_heart_rate", "avg_cadence",
                      "max_cadence", "avg_power", "max_power", "calories"]

        for field in str_fields:
            if field in ride:
                assert isinstance(ride[field], str), f"{field} should be a string"
                float(ride[field])

        for field in int_fields:
            if field in ride:
                assert isinstance(ride[field], int), f"{field} should be an int"


class TestParseTcx:
    @pytest.fixture()
    def ride(self):
        return parse_tcx(get_fixture("ride.tcx"))

    def test_required_fields_present(self, ride):
        for field in ["sport_type", "started_at", "elapsed_time", "moving_time",
                      "distance", "source", "created_at"]:
            assert field in ride, f"Missing required field: {field}"

    def test_source_is_tcx_file(self, ride):
        assert ride["source"] == "tcx-file"

    def test_sport_type_normalized(self, ride):
        # TCX uses "Biking" but we normalize to FIT's "cycling"
        assert ride["sport_type"] == "cycling"

    def test_times_from_lap_summary(self, ride):
        assert isinstance(ride["elapsed_time"], int)
        assert ride["elapsed_time"] == 900

    def test_distance_from_lap_summary(self, ride):
        assert isinstance(ride["distance"], str)
        assert float(ride["distance"]) == 5000.0

    def test_calories_from_lap_summary(self, ride):
        assert "calories" in ride
        assert ride["calories"] == 150

    def test_started_at_is_iso_format(self, ride):
        assert "T" in ride["started_at"]

    def test_elevation_gain_computed(self, ride):
        assert "elevation_gain" in ride
        assert isinstance(ride["elevation_gain"], str)
        assert float(ride["elevation_gain"]) > 0

    def test_polyline_present(self, ride):
        assert "polyline" in ride
        assert isinstance(ride["polyline"], str)
        assert len(ride["polyline"]) > 0

    def test_heart_rate_from_trackpoints(self, ride):
        assert "avg_heart_rate" in ride
        assert isinstance(ride["avg_heart_rate"], int)
        assert "max_heart_rate" in ride
        assert ride["max_heart_rate"] == 140

    def test_cadence_from_trackpoints(self, ride):
        assert "avg_cadence" in ride
        assert isinstance(ride["avg_cadence"], int)
        assert "max_cadence" in ride
        assert ride["max_cadence"] == 90

    def test_optional_numeric_fields_have_correct_types(self, ride):
        str_fields = ["elevation_gain", "avg_speed", "max_speed"]
        int_fields = ["avg_heart_rate", "max_heart_rate", "avg_cadence",
                      "max_cadence", "avg_power", "max_power", "calories"]

        for field in str_fields:
            if field in ride:
                assert isinstance(ride[field], str), f"{field} should be a string"
                float(ride[field])

        for field in int_fields:
            if field in ride:
                assert isinstance(ride[field], int), f"{field} should be an int"
