from pathlib import Path

import pytest

from app.parse import parse_file, parse_fit

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
