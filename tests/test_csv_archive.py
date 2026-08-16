"""Tests for output/csv_archive.py -- the replacement for the FIT monitoring
path after it was confirmed broken against a real Garmin Connect account (sleep
rejected outright, resting-HR/SpO2/HRV pollute the activity feed with fake
per-day activities). This archive is explicitly NOT Garmin-importable."""

import sqlite3

import pytest

from fitbit2garmin.db.migrations import migrate
from fitbit2garmin.output.csv_archive import write_all, write_daily_metric_csv, write_sleep_csv


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = OFF")
    migrate(c)
    c.execute(
        "INSERT INTO ingest_file (file_path, source_group, content_hash, status) "
        "VALUES ('fixture.json', 'monitoring_json:sleep', 'x', 'ok')"
    )
    return c


def test_write_sleep_csv(conn, tmp_path):
    conn.execute(
        """INSERT INTO sleep_entry
           (source_file, log_id, start_time_utc, end_time_utc, duration_ms, efficiency, minutes_asleep, minutes_awake, type)
           VALUES ('fixture.json', 1, '2020-06-15T23:00:00Z', '2020-06-16T07:00:00Z', 28800000, 95, 460, 20, 'stages')"""
    )
    conn.commit()

    path, n = write_sleep_csv(conn, tmp_path / "sleep.csv")
    content = path.read_text()

    assert n == 1
    assert "log_id,start_time_utc" in content
    assert "480.0" in content  # 28800000ms -> 480 minutes


def test_write_daily_metric_csv(conn, tmp_path):
    conn.execute(
        "INSERT INTO monitoring_metric (metric_type, source_file, ts_utc, value) "
        "VALUES ('resting_heart_rate', 'fixture.json', '2020-06-15T00:00:00Z', 58.5)"
    )
    conn.commit()

    path, n = write_daily_metric_csv(conn, "resting_heart_rate", tmp_path / "hr.csv", "resting_hr_bpm")
    content = path.read_text()

    assert n == 1
    assert content.startswith("date,resting_hr_bpm\n")
    assert "2020-06-15,58.5" in content


def test_write_all_returns_all_four_categories(conn, tmp_path):
    results = write_all(conn, tmp_path)
    assert set(results.keys()) == {"sleep", "resting_heart_rate", "spo2_daily", "hrv_daily"}
    for path, n in results.values():
        assert n == 0  # empty DB, but should not error
        assert path.exists()
