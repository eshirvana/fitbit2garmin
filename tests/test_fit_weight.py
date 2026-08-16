"""Tests for output/fit_weight.py -- the primary (user-confirmed working against a
real Garmin Connect account) weight export path. Locks in the noon-UTC timestamp
convention and kg storage, replicated exactly from a prior confirmed-working file.
"""

import sqlite3
from datetime import datetime, timezone

from fit_tool.fit_file import FitFile

from fitbit2garmin.db.migrations import migrate
from fitbit2garmin.output.fit_weight import write_weight_fit


def _conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = OFF")
    migrate(c)
    c.execute(
        "INSERT INTO ingest_file (file_path, source_group, content_hash, status) "
        "VALUES ('fixture.json', 'weight_json', 'x', 'ok')"
    )
    return c


def test_weight_fit_uses_noon_utc_timestamp(tmp_path):
    conn = _conn()
    conn.execute(
        "INSERT INTO weight_entry (source_file, log_id, entry_date, weight_kg, bmi, body_fat_pct) "
        "VALUES ('fixture.json', 1, '2020-06-15', 75.5, 24.1, NULL)"
    )
    conn.commit()

    path, n = write_weight_fit(conn, tmp_path / "weight.fit")
    assert n == 1

    decoded = FitFile.from_bytes(path.read_bytes())
    ws = [r.message for r in decoded.records if r.message.__class__.__name__ == "WeightScaleMessage"][0]

    expected_ms = int(datetime(2020, 6, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert ws.timestamp == expected_ms
    assert ws.weight == 75.5


def test_weight_fit_orders_chronologically_and_carries_body_fat(tmp_path):
    conn = _conn()
    conn.execute(
        "INSERT INTO weight_entry (source_file, log_id, entry_date, weight_kg, body_fat_pct) "
        "VALUES ('fixture.json', 2, '2020-06-20', 76.0, 18.5)"
    )
    conn.execute(
        "INSERT INTO weight_entry (source_file, log_id, entry_date, weight_kg, body_fat_pct) "
        "VALUES ('fixture.json', 3, '2020-06-10', 74.0, NULL)"
    )
    conn.commit()

    path, n = write_weight_fit(conn, tmp_path / "weight.fit")
    assert n == 2

    decoded = FitFile.from_bytes(path.read_bytes())
    rows = [r.message for r in decoded.records if r.message.__class__.__name__ == "WeightScaleMessage"]

    assert rows[0].timestamp < rows[1].timestamp
    assert rows[0].weight == 74.0
    assert rows[1].weight == 76.0
    assert rows[1].percent_fat == 18.5
