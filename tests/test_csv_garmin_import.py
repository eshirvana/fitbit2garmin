"""Tests for output/csv_garmin_import.py -- locks in real bugs found and fixed
against actual Garmin import attempts: the Body/Activities marker line, no-blank-
fields requirement, LF-only line endings (mixed CRLF/LF broke a real import), and
the distance unit conversion (source is meters, confirmed via Fitbit's own
readme; an earlier version of this module treated the raw daily sum as km)."""

import sqlite3

import pytest

from fitbit2garmin.db.migrations import migrate
from fitbit2garmin.output.csv_garmin_import import write_daily_totals_csv, write_weight_csv


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = OFF")
    migrate(c)
    c.execute(
        "INSERT INTO ingest_file (file_path, source_group, content_hash, status) "
        "VALUES ('fixture.json', 'weight_json', 'x', 'ok')"
    )
    return c


def test_weight_csv_has_body_marker_and_no_blank_fields(conn, tmp_path):
    conn.execute(
        "INSERT INTO weight_entry (source_file, log_id, entry_date, weight_kg, bmi, body_fat_pct) "
        "VALUES ('fixture.json', 1, '2020-06-15', 75.0, 24.0, NULL)"
    )
    conn.commit()

    path = write_weight_csv(conn, tmp_path / "weight.csv")
    content = path.read_text()
    lines = content.split("\n")

    assert lines[0] == "Body"
    assert lines[1] == "Date,Weight,BMI,Fat"
    assert lines[2].endswith(",0")  # Fat is 0, never blank, for a NULL body_fat_pct
    assert "\r" not in content  # the real mixed-line-ending bug


def test_weight_csv_iso_date_passthrough(conn, tmp_path):
    conn.execute(
        "INSERT INTO weight_entry (source_file, log_id, entry_date, weight_kg) "
        "VALUES ('fixture.json', 1, '2020-06-15', 75.0)"
    )
    conn.commit()

    path = write_weight_csv(conn, tmp_path / "weight.csv", locale="iso")
    content = path.read_text()
    assert "2020-06-15," in content


def test_daily_totals_csv_has_activities_marker_and_no_mixed_line_endings(conn, tmp_path):
    conn.execute(
        "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) "
        "VALUES ('steps_daily', 'fixture.json', '2020-06-15T00:00:00Z', 10000)"
    )
    conn.commit()

    path, n = write_daily_totals_csv(conn, tmp_path / "daily.csv")
    content = path.read_text()

    assert n == 1
    assert content.startswith("Activities\n")
    assert "Date,Calories Burned,Steps,Distance,Floors," in content
    assert "\r" not in content


def test_daily_totals_distance_unit_conversion(conn, tmp_path):
    # Confirmed via Physical Activity_GoogleData/distance_readme.txt: source
    # values are in METERS. A prior version of this module treated the raw daily
    # sum as already being km, overstating distance by 1000x.
    conn.execute(
        "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) "
        "VALUES ('distance_daily', 'fixture.json', '2020-06-15T00:00:00Z', 8000)"  # 8000 m = 8 km
    )
    conn.execute(
        "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) "
        "VALUES ('steps_daily', 'fixture.json', '2020-06-15T00:00:00Z', 10000)"  # needed: rows are filtered to steps > 0
    )
    conn.commit()

    path, _ = write_daily_totals_csv(conn, tmp_path / "daily.csv", units="metric")
    line = [l for l in path.read_text().split("\n") if l.startswith("2020-06-15")][0]
    distance_field = line.split(",")[3]
    assert float(distance_field) == pytest.approx(8.0, abs=0.01)  # km, not 8000 or 0.008

    path2, _ = write_daily_totals_csv(conn, tmp_path / "daily_imperial.csv", units="imperial")
    line2 = [l for l in path2.read_text().split("\n") if l.startswith("2020-06-15")][0]
    distance_field2 = line2.split(",")[3]
    assert float(distance_field2) == pytest.approx(8.0 * 0.621371, abs=0.01)  # miles


def test_daily_totals_excludes_zero_step_days(conn, tmp_path):
    # Confirmed via simonepri/fitbit2garmin's real source: rows are filtered to
    # steps > 0 before being written. A real Garmin upload failure was traced to
    # this exact gap -- an incomplete first-tracking-day row (steps=0, but other
    # fields present) was included where the reference tool would have excluded
    # it, and Garmin's importer rejected the whole file generically.
    conn.execute(
        "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) "
        "VALUES ('sedentary_minutes', 'fixture.json', '2020-06-14T00:00:00Z', 1009)"
    )
    conn.execute(
        "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) "
        "VALUES ('steps_daily', 'fixture.json', '2020-06-14T00:00:00Z', 0)"
    )
    conn.execute(
        "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) "
        "VALUES ('steps_daily', 'fixture.json', '2020-06-15T00:00:00Z', 5000)"
    )
    conn.commit()

    path, n = write_daily_totals_csv(conn, tmp_path / "daily.csv")
    content = path.read_text()

    assert n == 1
    assert "2020-06-14" not in content
    assert "2020-06-15" in content


def test_daily_totals_sample_days_limits_to_earliest_dates(conn, tmp_path):
    for date in ("2020-06-10", "2020-06-11", "2020-06-12", "2020-06-20"):
        conn.execute(
            "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) "
            "VALUES ('steps_daily', 'fixture.json', ?, 5000)",
            (f"{date}T00:00:00Z",),
        )
    conn.commit()

    path, n = write_daily_totals_csv(conn, tmp_path / "daily.csv", sample_days=2)
    content = path.read_text()

    assert n == 2
    assert "2020-06-10" in content
    assert "2020-06-11" in content
    assert "2020-06-12" not in content
    assert "2020-06-20" not in content
