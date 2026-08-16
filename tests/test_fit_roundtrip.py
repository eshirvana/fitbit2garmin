"""FIT round-trip tests: generate a FIT file via output/fit_activity.py, decode it
back with fit-tool's own reader, and assert the values survive encoding -- this is
what would have caught the old repo's GPS-encoding and UINT16-overflow bug family
(git history: ab25e9e, 1861a57) before they shipped.
"""

import sqlite3

import pytest
from fit_tool.fit_file import FitFile
from fit_tool.profile.profile_type import Sport, SubSport

from fitbit2garmin.db.migrations import migrate
from fitbit2garmin.output.fit_activity import _ALT_MIN, _SPD_MAX, build_activity_fit


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = OFF")
    migrate(c)
    c.execute(
        "INSERT INTO ingest_file (file_path, source_group, content_hash, status) "
        "VALUES ('fixture.csv', 'user_exercises', 'x', 'ok')"
    )
    return c


def _insert_activity(conn, activity_uid, gps_source="none", gps_confidence="not_expected", **overrides):
    fields = dict(
        activity_uid=activity_uid,
        user_exercise_id=1,
        exercise_json_log_id=None,
        start_time_utc="2020-06-15T14:00:00Z",
        end_time_utc="2020-06-15T14:30:00Z",
        duration_s=1800,
        activity_name_raw="Outdoor Run",
        activity_type_id=90009,
        fit_sport=Sport.RUNNING.value,
        fit_sub_sport=SubSport.STREET.value,
        log_type="TRACKER",
        has_metrics=1,
        calories=250,
        steps=4000,
        distance_m=5000.0,
        avg_heart_rate=140,
        peak_heart_rate=170,
        elevation_gain_m=50.0,
        gps_source=gps_source,
        gps_confidence=gps_confidence,
        match_confidence="time_exact",
        reconciliation_notes=None,
    )
    fields.update(overrides)
    conn.execute(
        "INSERT INTO raw_user_exercise (exercise_id, source_file, exercise_start_utc, exercise_end_utc, activity_name, log_type) "
        "VALUES (1, 'fixture.csv', ?, ?, ?, ?)",
        (fields["start_time_utc"], fields["end_time_utc"], fields["activity_name_raw"], fields["log_type"]),
    )
    cols = ",".join(fields.keys())
    placeholders = ",".join("?" for _ in fields)
    conn.execute(f"INSERT INTO activity ({cols}) VALUES ({placeholders})", list(fields.values()))
    conn.commit()


def _insert_gps_points(conn, activity_uid, points):
    """points: list of (time_utc, lat, lon, altitude_m)"""
    for seq, (t, lat, lon, alt) in enumerate(points):
        cur = conn.execute(
            """INSERT INTO gps_point
               (source, source_file, source_key, point_time_utc, latitude, longitude, altitude_m, sequence_in_source)
               VALUES ('tcx', 'fixture.csv', 'k', ?, ?, ?, ?, ?)""",
            (t, lat, lon, alt, seq),
        )
        conn.execute(
            "INSERT INTO activity_gps_point (activity_uid, gps_point_id, seq) VALUES (?, ?, ?)",
            (activity_uid, cur.lastrowid, seq),
        )
    conn.commit()


def test_no_gps_activity_roundtrip(conn):
    _insert_activity(conn, "ue:1")
    fit_bytes, report = build_activity_fit(conn, "ue:1")

    decoded = FitFile.from_bytes(fit_bytes)
    sessions = [r.message for r in decoded.records if r.message.__class__.__name__ == "SessionMessage"]
    records = [r.message for r in decoded.records if r.message.__class__.__name__ == "RecordMessage"]

    assert len(sessions) == 1
    s = sessions[0]
    assert s.sport == Sport.RUNNING.value
    assert s.sub_sport == SubSport.STREET.value
    assert s.total_calories == 250
    assert s.total_strides == 2000  # steps // 2
    assert s.avg_heart_rate == 140
    assert s.max_heart_rate == 170
    assert s.total_distance == pytest.approx(5000.0, abs=1.0)

    # Minimal-session guarantee: exactly 2 bare records when there's no GPS.
    assert len(records) == 2
    assert report["points_written"] == 0


def test_gps_activity_roundtrip_lat_lon_precision(conn):
    _insert_activity(conn, "ue:2", gps_source="tcx", gps_confidence="exact")
    points = [
        ("2020-06-15T14:00:00Z", 45.123456, -73.654321, 100.0),
        ("2020-06-15T14:00:10Z", 45.123556, -73.654221, 101.0),
        ("2020-06-15T14:00:20Z", 45.123656, -73.654121, 102.0),
    ]
    _insert_gps_points(conn, "ue:2", points)

    fit_bytes, report = build_activity_fit(conn, "ue:2")
    decoded = FitFile.from_bytes(fit_bytes)
    records = [r.message for r in decoded.records if r.message.__class__.__name__ == "RecordMessage"]
    sessions = [r.message for r in decoded.records if r.message.__class__.__name__ == "SessionMessage"]

    assert report["points_written"] == 3
    assert report["points_skipped"] == 0
    assert len(records) == 3

    for (t, lat, lon, alt), rec in zip(points, records):
        # SINT32 semicircle round-trip precision is ~1e-7 degrees.
        assert rec.position_lat == pytest.approx(lat, abs=1e-4)
        assert rec.position_long == pytest.approx(lon, abs=1e-4)
        assert rec.altitude == pytest.approx(alt, abs=0.2)

    assert sessions[0].start_position_lat == pytest.approx(points[0][1], abs=1e-4)


def test_altitude_clamped_not_dropped(conn):
    # A GPS point with an implausibly low altitude (below the FIT UINT16+offset
    # encodable floor) must be clamped, not silently dropped or crash the file --
    # this is the exact bug class fixed in commit ab25e9e.
    _insert_activity(conn, "ue:3", gps_source="tcx", gps_confidence="exact")
    _insert_gps_points(conn, "ue:3", [
        ("2020-06-15T14:00:00Z", 45.0, -73.0, -1000.0),  # below _ALT_MIN
        ("2020-06-15T14:00:10Z", 45.001, -73.001, -1000.0),
    ])

    fit_bytes, report = build_activity_fit(conn, "ue:3")
    assert report["points_written"] == 2
    assert report["points_skipped"] == 0

    decoded = FitFile.from_bytes(fit_bytes)
    records = [r.message for r in decoded.records if r.message.__class__.__name__ == "RecordMessage"]
    for rec in records:
        assert rec.altitude >= _ALT_MIN - 0.5  # clamped, not the raw -1000


def test_high_speed_capped_not_dropped(conn):
    # Two points ~1km apart 1 second in real time -> would be a wildly
    # implausible speed exceeding the FIT UINT16 speed field's encodable max;
    # must be capped, not left to overflow/crash the encoder.
    _insert_activity(conn, "ue:4", gps_source="tcx", gps_confidence="exact")
    _insert_gps_points(conn, "ue:4", [
        ("2020-06-15T14:00:00Z", 45.0, -73.0, 100.0),
        ("2020-06-15T14:00:01Z", 45.01, -73.0, 100.0),  # ~1.1km in 1s
    ])

    fit_bytes, report = build_activity_fit(conn, "ue:4")
    assert report["points_skipped"] == 0

    decoded = FitFile.from_bytes(fit_bytes)
    records = [r.message for r in decoded.records if r.message.__class__.__name__ == "RecordMessage"]
    assert records[1].speed <= _SPD_MAX + 0.01


def test_gps_location_csv_fallback_source_has_no_cumulative_distance(conn):
    # gps_location_csv points have no built-in cumulative distance (unlike TCX) --
    # fit_activity must fall back to Haversine-computed distance, not crash or
    # leave distance at 0 for the whole track.
    _insert_activity(conn, "ue:5", gps_source="gps_location_csv", gps_confidence="windowed")
    for seq, (t, lat, lon) in enumerate([
        ("2020-06-15T14:00:00Z", 45.0, -73.0),
        ("2020-06-15T14:00:10Z", 45.001, -73.001),
        ("2020-06-15T14:00:20Z", 45.002, -73.002),
    ]):
        cur = conn.execute(
            """INSERT INTO gps_point
               (source, source_file, source_key, point_time_utc, latitude, longitude, sequence_in_source)
               VALUES ('gps_location_csv', 'fixture.csv', '2020-06-15', ?, ?, ?, ?)""",
            (t, lat, lon, seq),
        )
        conn.execute(
            "INSERT INTO activity_gps_point (activity_uid, gps_point_id, seq) VALUES (?, ?, ?)",
            ("ue:5", cur.lastrowid, seq),
        )
    conn.commit()

    fit_bytes, report = build_activity_fit(conn, "ue:5")
    decoded = FitFile.from_bytes(fit_bytes)
    records = [r.message for r in decoded.records if r.message.__class__.__name__ == "RecordMessage"]

    assert report["points_written"] == 3
    assert records[-1].distance > 0


def test_activity_detail_fields_roundtrip_for_walking(conn):
    # Regression test for a real bug: avg_running_cadence silently doesn't
    # survive encode/decode for non-RUNNING sports (confirmed by direct
    # testing against fit-tool) -- must use the generic avg_cadence field.
    _insert_activity(
        conn, "ue:10",
        activity_name_raw="Outdoor Walk",
        fit_sport=Sport.WALKING.value, fit_sub_sport=SubSport.CASUAL_WALKING.value,
        distance_m=2000.0, duration_s=1600,
        avg_speed_ms=1.25, avg_cadence=48,
        time_in_hr_zone_json='[0, 240, 900, 720]',
        source_device="Blaze",
    )
    fit_bytes, _ = build_activity_fit(conn, "ue:10")
    decoded = FitFile.from_bytes(fit_bytes)
    session = [r.message for r in decoded.records if r.message.__class__.__name__ == "SessionMessage"][0]
    file_id = [r.message for r in decoded.records if r.message.__class__.__name__ == "FileIdMessage"][0]

    assert session.avg_speed == pytest.approx(1.25, abs=0.01)
    assert session.avg_cadence == 48
    assert session.time_in_hr_zone == [0.0, 240.0, 900.0, 720.0]
    assert file_id.product_name == "Blaze"


def test_activity_detail_fields_roundtrip_for_running(conn):
    _insert_activity(
        conn, "ue:11",
        activity_name_raw="Outdoor Run",
        fit_sport=Sport.RUNNING.value, fit_sub_sport=SubSport.STREET.value,
        avg_speed_ms=2.8, avg_cadence=81,
    )
    fit_bytes, _ = build_activity_fit(conn, "ue:11")
    decoded = FitFile.from_bytes(fit_bytes)
    session = [r.message for r in decoded.records if r.message.__class__.__name__ == "SessionMessage"][0]

    assert session.avg_speed == pytest.approx(2.8, abs=0.01)
    assert session.avg_cadence == 81


def test_no_source_device_falls_back_to_tool_name(conn):
    _insert_activity(conn, "ue:12", source_device=None)
    fit_bytes, _ = build_activity_fit(conn, "ue:12")
    decoded = FitFile.from_bytes(fit_bytes)
    file_id = [r.message for r in decoded.records if r.message.__class__.__name__ == "FileIdMessage"][0]
    assert file_id.product_name == "Fitbit2Garmin"


def test_corrupt_optional_field_does_not_drop_the_whole_point(conn):
    # A code-review-caught regression: a bad value in an OPTIONAL field
    # (heart_rate here) must not skip the point/RecordMessage entirely -- FIT
    # record count for a GPS activity must always equal the activity_gps_point
    # row count. Required fields (timestamp/lat/lon) are NOT NULL in the schema
    # and always written by this project's own ingest code, so only optional
    # fields are realistic failure points.
    _insert_activity(conn, "ue:6", gps_source="tcx", gps_confidence="exact")
    for seq, (t, lat, lon, hr) in enumerate([
        ("2020-06-15T14:00:00Z", 45.0, -73.0, "not-a-number"),
        ("2020-06-15T14:00:10Z", 45.001, -73.001, 140),
    ]):
        cur = conn.execute(
            """INSERT INTO gps_point
               (source, source_file, source_key, point_time_utc, latitude, longitude, heart_rate, sequence_in_source)
               VALUES ('tcx', 'fixture.csv', 'k', ?, ?, ?, ?, ?)""",
            (t, lat, lon, hr, seq),
        )
        conn.execute(
            "INSERT INTO activity_gps_point (activity_uid, gps_point_id, seq) VALUES (?, ?, ?)",
            ("ue:6", cur.lastrowid, seq),
        )
    conn.commit()

    fit_bytes, report = build_activity_fit(conn, "ue:6")
    decoded = FitFile.from_bytes(fit_bytes)
    records = [r.message for r in decoded.records if r.message.__class__.__name__ == "RecordMessage"]

    # Both points got a RecordMessage despite the first having an unparseable HR.
    assert report["points_written"] == 2
    assert len(records) == 2
    assert records[1].heart_rate == 140
