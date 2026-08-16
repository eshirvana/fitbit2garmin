"""Tests for reconcile/activity_matcher.py -- covers the matching tiers, the
claim-guard conflict case, and the sparse-fragment minimal-session guarantee
(decision #11 in the project plan: AUTO_DETECTED rows with blank tracker fields
must still produce a valid activity, never be dropped or crash the batch)."""

import sqlite3

import pytest

from fitbit2garmin.db.migrations import migrate
from fitbit2garmin.reconcile.activity_matcher import reconcile_all


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = OFF")  # test rows reference a synthetic ingest_file
    migrate(c)
    c.execute(
        "INSERT INTO ingest_file (file_path, source_group, content_hash, status) "
        "VALUES ('fixture.csv', 'user_exercises', 'x', 'ok')"
    )
    c.execute(
        "INSERT INTO ingest_file (file_path, source_group, content_hash, status) "
        "VALUES ('fixture.json', 'exercise_json', 'x', 'ok')"
    )
    return c


def _insert_ue(conn, exercise_id, start, end, activity_name="Outdoor Walk", log_type="TRACKER"):
    conn.execute(
        """INSERT INTO raw_user_exercise
           (exercise_id, source_file, exercise_start_utc, exercise_end_utc, activity_name, log_type)
           VALUES (?, 'fixture.csv', ?, ?, ?, ?)""",
        (exercise_id, start, end, activity_name, log_type),
    )


def _insert_ej(conn, log_id, start, activity_type_id=90013, has_gps=0):
    conn.execute(
        """INSERT INTO raw_exercise_json
           (log_id, source_file, start_time_utc, activity_type_id, has_gps, calories, steps)
           VALUES (?, 'fixture.json', ?, ?, ?, 100, 1000)""",
        (log_id, start, activity_type_id, has_gps),
    )


def test_exact_time_match(conn):
    _insert_ue(conn, 1, "2020-01-01T10:00:00Z", "2020-01-01T10:30:00Z")
    _insert_ej(conn, 101, "2020-01-01T10:00:02Z")  # 2s delta -> exact tier
    conn.commit()

    reconcile_all(conn)

    row = conn.execute("SELECT * FROM activity WHERE user_exercise_id=1").fetchone()
    assert row["match_confidence"] == "time_exact"
    assert row["exercise_json_log_id"] == 101
    assert row["calories"] == 100


def test_fuzzy_time_match(conn):
    _insert_ue(conn, 2, "2020-01-01T10:00:00Z", "2020-01-01T10:30:00Z")
    _insert_ej(conn, 102, "2020-01-01T10:01:00Z")  # 60s delta -> fuzzy tier only
    conn.commit()

    reconcile_all(conn)

    row = conn.execute("SELECT * FROM activity WHERE user_exercise_id=2").fetchone()
    assert row["match_confidence"] == "time_fuzzy"
    assert row["exercise_json_log_id"] == 102


def test_no_match_beyond_tolerance(conn):
    _insert_ue(conn, 3, "2020-01-01T10:00:00Z", "2020-01-01T10:30:00Z")
    _insert_ej(conn, 103, "2020-01-01T11:00:00Z")  # 1hr delta -> no match
    conn.commit()

    reconcile_all(conn)

    row = conn.execute("SELECT * FROM activity WHERE user_exercise_id=3").fetchone()
    assert row["match_confidence"] == "user_exercises_only"
    assert row["exercise_json_log_id"] is None
    # exercise_json row itself should show up as an orphan, not silently vanish
    ej = conn.execute("SELECT used_by_activity_uid FROM raw_exercise_json WHERE log_id=103").fetchone()
    assert ej["used_by_activity_uid"] is None


def test_double_claim_conflict_first_come_first_served(conn):
    # Two UserExercise rows both close enough to the same exercise_json record;
    # the earlier-starting one should claim it, the later one is left unmatched
    # rather than both claiming it or the conflict being silently dropped.
    _insert_ue(conn, 4, "2020-01-01T10:00:00Z", "2020-01-01T10:30:00Z")
    _insert_ue(conn, 5, "2020-01-01T10:00:01Z", "2020-01-01T10:30:00Z")
    _insert_ej(conn, 104, "2020-01-01T10:00:00Z")
    conn.commit()

    stats = reconcile_all(conn)

    first = conn.execute("SELECT * FROM activity WHERE user_exercise_id=4").fetchone()
    second = conn.execute("SELECT * FROM activity WHERE user_exercise_id=5").fetchone()
    assert first["exercise_json_log_id"] == 104
    assert second["exercise_json_log_id"] is None
    assert second["match_confidence"] == "user_exercises_only"

    # The conflict must be visible in the audit log, not silently resolved.
    conflict_rows = conn.execute(
        "SELECT * FROM reconciliation_log WHERE user_exercise_id=5 AND candidate_log_id=104"
    ).fetchall()
    assert any("claimed by an earlier activity" in (r["reason"] or "") for r in conflict_rows)


def test_sparse_auto_detected_fragment_still_produces_valid_activity(conn):
    # No matching exercise_json at all, and (per real data) an AUTO_DETECTED row
    # has every tracker_* field NULL -- this must still produce a minimally valid
    # activity row (non-null start/end/duration/fit_sport), never be dropped.
    _insert_ue(conn, 6, "2020-01-01T10:00:00Z", "2020-01-01T10:05:00Z",
               activity_name="Outdoor Walk", log_type="AUTO_DETECTED")
    conn.commit()

    reconcile_all(conn)

    row = conn.execute("SELECT * FROM activity WHERE user_exercise_id=6").fetchone()
    assert row is not None
    assert row["start_time_utc"] == "2020-01-01T10:00:00Z"
    assert row["end_time_utc"] == "2020-01-01T10:05:00Z"
    assert row["duration_s"] == 300
    assert row["fit_sport"] is not None
    assert row["has_metrics"] == 0
    assert row["calories"] is None


def test_corrupt_row_is_skipped_not_crashed(conn):
    _insert_ue(conn, 7, "2020-01-01T10:00:00Z", "2020-01-01T09:00:00Z")  # end before start
    conn.commit()

    stats = reconcile_all(conn)

    assert stats["skipped"] == 1
    row = conn.execute("SELECT * FROM activity WHERE user_exercise_id=7").fetchone()
    assert row is None
    skipped = conn.execute("SELECT * FROM skipped_activity WHERE user_exercise_id=7").fetchone()
    assert skipped is not None


def test_reconcile_is_idempotent(conn):
    _insert_ue(conn, 8, "2020-01-01T10:00:00Z", "2020-01-01T10:30:00Z")
    _insert_ej(conn, 108, "2020-01-01T10:00:00Z")
    conn.commit()

    stats1 = reconcile_all(conn)
    stats2 = reconcile_all(conn)

    assert stats1 == stats2
    assert conn.execute("SELECT count(*) AS n FROM activity").fetchone()["n"] == 1


def test_zero_heart_rate_is_treated_as_not_measured(conn):
    # Confirmed against real data: tracker_peak_heart_rate is 0 (not NULL) when
    # the device didn't capture HR -- 0 must not be written as a real reading.
    conn.execute(
        """INSERT INTO raw_user_exercise
           (exercise_id, source_file, exercise_start_utc, exercise_end_utc, activity_name,
            log_type, tracker_avg_heart_rate, tracker_peak_heart_rate)
           VALUES (9, 'fixture.csv', '2020-01-01T10:00:00Z', '2020-01-01T10:30:00Z',
                   'Outdoor Walk', 'TRACKER', 0, 0)"""
    )
    conn.commit()

    reconcile_all(conn)

    row = conn.execute("SELECT * FROM activity WHERE user_exercise_id=9").fetchone()
    assert row["avg_heart_rate"] is None
    assert row["peak_heart_rate"] is None
