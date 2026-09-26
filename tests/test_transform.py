import json
from pathlib import Path


FIXTURE_FILE = Path("fixtures/test_university_chapters.json")


def load_fixture():
    with open(FIXTURE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def valid_coordinates(longitude, latitude):
    return (
        longitude is not None
        and latitude is not None
        and -180 <= longitude <= 180
        and -90 <= latitude <= 90
    )


def city_warning(city):
    if city is None:
        return "MISSING_OR_UNKNOWN_CITY"

    if city.strip() == "":
        return "MISSING_OR_UNKNOWN_CITY"

    if city.strip().upper() == "UNKNOWN":
        return "MISSING_OR_UNKNOWN_CITY"

    return ""


def test_valid_coordinates():
    assert valid_coordinates(80.0, 13.0)


def test_invalid_longitude():
    assert not valid_coordinates(200.0, 13.0)


def test_invalid_latitude():
    assert not valid_coordinates(80.0, 100.0)


def test_invalid_coordinates_are_quarantined():
    records = load_fixture()

    quarantine = [
        record
        for record in records
        if not valid_coordinates(
            record["longitude"],
            record["latitude"]
        )
    ]

    gold = [
        record
        for record in records
        if valid_coordinates(
            record["longitude"],
            record["latitude"]
        )
    ]

    assert len(quarantine) == 1
    assert quarantine[0]["chapter_id"] == "TEST-002"

    # Quarantined row must never appear in Gold
    assert "TEST-002" not in [
        record["chapter_id"]
        for record in gold
    ]


def test_warning_city_reaches_gold():
    records = load_fixture()

    gold = [
        record
        for record in records
        if valid_coordinates(
            record["longitude"],
            record["latitude"]
        )
    ]

    warning_records = [
        record
        for record in gold
        if city_warning(record["city"]) == "MISSING_OR_UNKNOWN_CITY"
    ]

    assert len(warning_records) == 2

    warning_ids = {
        record["chapter_id"]
        for record in warning_records
    }

    assert warning_ids == {"TEST-003", "TEST-004"}


def test_valid_city_has_no_warning():
    assert city_warning("Los Angeles") == ""


def test_unknown_city_has_warning():
    assert city_warning("UNKNOWN") == "MISSING_OR_UNKNOWN_CITY"


def test_blank_city_has_warning():
    assert city_warning("   ") == "MISSING_OR_UNKNOWN_CITY"