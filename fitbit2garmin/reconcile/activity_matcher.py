"""Activity reconciliation: joins raw_user_exercise (driving set, ALL rows) against
raw_exercise_json (timestamp-matched, not ID-matched) and attaches GPS, producing
the canonical `activity` table. Implements the algorithm in the approved project
plan exactly -- see PROGRESS.md for the numbered steps this maps to.
"""

import bisect
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

from . import gps_attacher
from .activity_type_map import apply_gps_refinement, resolve_sport

logger = logging.getLogger(__name__)

_EXACT_TOLERANCE = timedelta(seconds=5)
_FUZZY_TOLERANCE = timedelta(seconds=120)

_DISTANCE_UNIT_TO_METERS = {
    "Kilometer": 1000.0,
    "Mile": 1609.34,
    "Meter": 1.0,
}


def _parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


class _ExerciseJsonIndex:
    """Sorted-by-start-time index over raw_exercise_json for windowed candidate lookup."""

    def __init__(self, conn: sqlite3.Connection):
        rows = conn.execute(
            """SELECT log_id, start_time_utc, duration_ms, has_gps, activity_type_id,
                      calories, steps, distance, distance_unit, average_heart_rate,
                      elevation_gain, heart_rate_zones_json
               FROM raw_exercise_json ORDER BY start_time_utc"""
        ).fetchall()
        self._rows = rows
        self._dts = [_parse_utc(r["start_time_utc"]) for r in rows]
        self._claimed: set[int] = set()

    def candidates(self, target: datetime) -> list[tuple[timedelta, sqlite3.Row]]:
        lo = bisect.bisect_left(self._dts, target - _FUZZY_TOLERANCE)
        hi = bisect.bisect_right(self._dts, target + _FUZZY_TOLERANCE)
        out = []
        for i in range(lo, hi):
            delta = self._dts[i] - target
            out.append((abs(delta), self._rows[i]))
        return out

    def is_claimed(self, log_id: int) -> bool:
        return log_id in self._claimed

    def claim(self, log_id: int) -> None:
        self._claimed.add(log_id)

    def unclaimed_log_ids(self) -> list[int]:
        return [r["log_id"] for r in self._rows if r["log_id"] not in self._claimed]


def _resolve_field(*candidates):
    for c in candidates:
        if c is not None:
            return c
    return None


def _resolve_heart_rate(*candidates):
    """Like _resolve_field, but treats 0 as 'not measured' rather than a real
    reading -- confirmed against real data: tracker_avg/peak_heart_rate is 0 (not
    NULL) on rows where the device simply didn't capture HR, and a 0bpm value would
    otherwise write an invalid (peak-below-average) heart rate into the FIT output."""
    for c in candidates:
        if c is not None and c != 0:
            return c
    return None


def _distance_to_meters(distance: float | None, unit: str | None) -> float | None:
    if distance is None:
        return None
    factor = _DISTANCE_UNIT_TO_METERS.get(unit, 1.0)
    return distance * factor


def _peak_hr_from_zones(zones_json: str | None) -> int | None:
    if not zones_json:
        return None
    import json

    try:
        zones = json.loads(zones_json)
    except (ValueError, TypeError):
        return None
    maxes = [z.get("max") for z in zones if z.get("max") is not None and z.get("minutes", 0) > 0]
    return max(maxes) if maxes else None


def reconcile_all(conn: sqlite3.Connection) -> dict:
    """Rebuild the activity table from scratch (idempotent -- safe to re-run after
    a matcher/mapping-table change without re-ingesting anything)."""
    conn.execute("DELETE FROM activity_gps_point")
    conn.execute("DELETE FROM activity")
    conn.execute("DELETE FROM reconciliation_log")
    conn.execute("DELETE FROM skipped_activity")
    conn.execute("UPDATE raw_exercise_json SET used_by_activity_uid = NULL")
    conn.commit()

    ej_index = _ExerciseJsonIndex(conn)

    user_exercises = conn.execute(
        "SELECT * FROM raw_user_exercise ORDER BY exercise_start_utc"
    ).fetchall()

    stats = {
        "total": 0, "skipped": 0,
        "match_time_exact": 0, "match_time_fuzzy": 0, "match_user_exercises_only": 0,
        "gps_exact": 0, "gps_windowed": 0, "gps_flagged_no_data": 0, "gps_not_expected": 0,
        "unmapped_type": 0,
    }

    for ue in user_exercises:
        stats["total"] += 1
        activity_uid = f"ue:{ue['exercise_id']}"

        try:
            start_dt = _parse_utc(ue["exercise_start_utc"])
            end_dt = _parse_utc(ue["exercise_end_utc"])
        except ValueError as exc:
            conn.execute(
                "INSERT INTO skipped_activity (user_exercise_id, reason) VALUES (?, ?)",
                (ue["exercise_id"], f"unparseable timestamp: {exc}"),
            )
            stats["skipped"] += 1
            continue
        if end_dt <= start_dt:
            conn.execute(
                "INSERT INTO skipped_activity (user_exercise_id, reason) VALUES (?, ?)",
                (ue["exercise_id"], f"end_time <= start_time ({ue['exercise_end_utc']} <= {ue['exercise_start_utc']})"),
            )
            stats["skipped"] += 1
            continue

        # --- steps 1-3: time-window match against raw_exercise_json, claim guard ---
        candidates = ej_index.candidates(start_dt)
        exact_candidates = [(d, r) for d, r in candidates if d <= _EXACT_TOLERANCE]
        pool = exact_candidates if exact_candidates else candidates
        tier = "time_exact" if exact_candidates else ("time_fuzzy" if candidates else None)

        pool.sort(key=lambda dr: (
            dr[0],
            0 if dr[1]["has_gps"] else 1,
            abs((dr[1]["duration_ms"] or 0) / 1000.0 - (end_dt - start_dt).total_seconds()),
        ))

        matched_ej = None
        for delta, cand in pool:
            already_claimed = ej_index.is_claimed(cand["log_id"])
            conn.execute(
                """INSERT INTO reconciliation_log
                   (user_exercise_id, candidate_log_id, delta_seconds, chosen, reason)
                   VALUES (?, ?, ?, 0, ?)""",
                (ue["exercise_id"], cand["log_id"], delta.total_seconds(),
                 "claimed by an earlier activity" if already_claimed else "not selected (not best candidate)"),
            )
            if matched_ej is None and not already_claimed:
                matched_ej = cand
        if not pool:
            conn.execute(
                """INSERT INTO reconciliation_log
                   (user_exercise_id, candidate_log_id, delta_seconds, chosen, reason)
                   VALUES (?, NULL, NULL, 0, 'no candidates within +/-120s window')""",
                (ue["exercise_id"],),
            )

        if matched_ej is not None:
            ej_index.claim(matched_ej["log_id"])
            conn.execute(
                "UPDATE reconciliation_log SET chosen=1, reason='chosen: best match' "
                "WHERE user_exercise_id=? AND candidate_log_id=?",
                (ue["exercise_id"], matched_ej["log_id"]),
            )
            match_confidence = tier
        else:
            match_confidence = "user_exercises_only"
        stats[f"match_{match_confidence}"] += 1

        # --- steps 5-7: type resolution + field-level fallback chains ---
        activity_type_id = matched_ej["activity_type_id"] if matched_ej is not None else None
        mapping, was_unmapped = resolve_sport(ue["activity_name"], activity_type_id)
        if was_unmapped:
            stats["unmapped_type"] += 1

        calories = _resolve_field(
            matched_ej["calories"] if matched_ej is not None else None,
            ue["tracker_total_calories"], ue["manually_logged_total_calories"],
        )
        steps = _resolve_field(
            matched_ej["steps"] if matched_ej is not None else None,
            ue["tracker_total_steps"], ue["manually_logged_total_steps"],
        )
        distance_m = _resolve_field(
            _distance_to_meters(matched_ej["distance"], matched_ej["distance_unit"]) if matched_ej is not None else None,
            (ue["tracker_total_distance_mm"] / 1000.0) if ue["tracker_total_distance_mm"] is not None else None,
            (ue["manually_logged_total_distance_mm"] / 1000.0) if ue["manually_logged_total_distance_mm"] is not None else None,
        )
        avg_hr = _resolve_heart_rate(
            matched_ej["average_heart_rate"] if matched_ej is not None else None,
            ue["tracker_avg_heart_rate"],
        )
        peak_hr = _resolve_heart_rate(
            ue["tracker_peak_heart_rate"],
            _peak_hr_from_zones(matched_ej["heart_rate_zones_json"]) if matched_ej is not None else None,
        )
        elevation_gain_m = _resolve_field(
            matched_ej["elevation_gain"] if matched_ej is not None else None,
            (ue["tracker_total_altitude_mm"] / 1000.0) if ue["tracker_total_altitude_mm"] is not None else None,
        )
        has_metrics = 1 if any(
            v is not None for v in (calories, steps, distance_m, avg_hr, peak_hr)
        ) else 0

        # --- step 9: GPS attachment ---
        has_gps_flag = bool(matched_ej["has_gps"]) if matched_ej is not None else False
        gps_result = gps_attacher.attach_gps(
            conn,
            exercise_json_log_id=matched_ej["log_id"] if matched_ej is not None else None,
            activity_name=ue["activity_name"],
            activity_type_id=activity_type_id,
            has_gps_flag=has_gps_flag,
            start_time_utc=ue["exercise_start_utc"],
            end_time_utc=ue["exercise_end_utc"],
        )
        stats[f"gps_{gps_result.gps_confidence}"] += 1

        # --- step 6 cont'd: GPS-informed sport refinement ---
        mapping = apply_gps_refinement(mapping, gps_attached=gps_result.gps_source != "none")

        conn.execute(
            """INSERT INTO activity
               (activity_uid, user_exercise_id, exercise_json_log_id, start_time_utc, end_time_utc,
                duration_s, activity_name_raw, activity_type_id, fit_sport, fit_sub_sport, log_type,
                has_metrics, calories, steps, distance_m, avg_heart_rate, peak_heart_rate,
                elevation_gain_m, gps_source, gps_confidence, match_confidence, reconciliation_notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                activity_uid, ue["exercise_id"], matched_ej["log_id"] if matched_ej is not None else None,
                ue["exercise_start_utc"], ue["exercise_end_utc"],
                int((end_dt - start_dt).total_seconds()),
                ue["activity_name"], activity_type_id,
                mapping.sport.value, mapping.sub_sport.value, ue["log_type"],
                has_metrics, calories, steps, distance_m, avg_hr, peak_hr, elevation_gain_m,
                gps_result.gps_source, gps_result.gps_confidence, match_confidence,
                "unmapped activity type -> GENERIC/GENERIC" if was_unmapped else None,
            ),
        )

        if gps_result.point_ids:
            conn.executemany(
                "INSERT INTO activity_gps_point (activity_uid, gps_point_id, seq) VALUES (?, ?, ?)",
                [(activity_uid, pid, seq) for seq, pid in enumerate(gps_result.point_ids)],
            )

        if matched_ej is not None:
            conn.execute(
                "UPDATE raw_exercise_json SET used_by_activity_uid=? WHERE log_id=?",
                (activity_uid, matched_ej["log_id"]),
            )

    orphans = ej_index.unclaimed_log_ids()
    stats["orphan_exercise_json_count"] = len(orphans)

    conn.commit()
    logger.info("Reconciliation complete: %s", stats)
    return stats
