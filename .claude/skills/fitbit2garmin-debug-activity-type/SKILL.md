---
name: fitbit2garmin-debug-activity-type
description: Diagnose why a specific fitbit2garmin activity has the wrong sport type, is missing GPS, or wasn't matched to its expected source record. Use when the user asks why an activity shows the wrong type in Garmin, is missing GPS/details, or names a specific activity (by date, name, or ID) that looks wrong.
---

# fitbit2garmin: debug one activity's type/GPS/matching

Every reconciliation decision is logged, not just the final result -- this skill
walks the audit trail rather than guessing.

## Step 1: find the activity

If given a date/description ("that CrossFit session from March 2023"):
```bash
sqlite3 fitbit2garmin.sqlite3 "SELECT activity_uid, activity_name_raw, start_time_utc, fit_sport, fit_sub_sport, gps_source, gps_confidence, match_confidence FROM activity WHERE start_time_utc LIKE '2023-03%' AND activity_name_raw LIKE '%CrossFit%'"
```
If given an `activity_uid` or `ue_<id>` from a filename, query directly:
```bash
sqlite3 fitbit2garmin.sqlite3 "SELECT * FROM activity WHERE activity_uid='ue:<id>'"
```

## Step 2: see every match candidate considered, not just the winner

```bash
sqlite3 fitbit2garmin.sqlite3 "SELECT * FROM reconciliation_log WHERE user_exercise_id=<exercise_id> ORDER BY chosen DESC, delta_seconds"
```
This shows every `raw_exercise_json` candidate within the time-matching window,
the delta in seconds, whether it was chosen, and why (or why not -- including
`'claimed by an earlier activity'` if a double-claim conflict occurred, which is
logged rather than silently resolved). `chosen=0, candidate_log_id=NULL` with
reason `'no candidates within +/-120s window'` means no classic exercise-log
record existed at all -- expected for activities outside 2018-2024 or for
fragments that were only ever auto-detected, not promoted to the classic log.

## Step 3: trace the exact type mapping used

```python
from fitbit2garmin.reconcile.activity_type_map import resolve_sport, apply_gps_refinement
mapping, was_unmapped = resolve_sport(activity_name_raw, activity_type_id)  # from the activity row
print(mapping, was_unmapped)
```
Precedence: `activity_type_id` (from a matched `raw_exercise_json` record) wins
over `activity_name_raw` when both are available -- check
`activity.exercise_json_log_id`; if it's NULL, only the name-based lookup ran.
If `was_unmapped`, the activity fell back to GENERIC/GENERIC -- the fix is a new
entry in `ACTIVITY_NAME_MAP`/`ACTIVITY_TYPE_ID_MAP` in
`fitbit2garmin/reconcile/activity_type_map.py`, not a workaround elsewhere.

Then check the GPS-refinement rule, applied *after* the table lookup:
`apply_gps_refinement(mapping, gps_attached=activity.gps_source != 'none')` --
this is what turns CYCLING/GENERIC into CYCLING/ROAD, etc. If the sport looks
right but the sub-sport seems off, this is almost always where to look.

## Step 4: for GPS-specific issues

- `gps_confidence='exact'`: matched `Activities/<log_id>.tcx` by exact filename.
  If the track still looks wrong, the TCX file itself may have a data problem
  (some real files are confirmed-empty stubs -- see PROGRESS.md Phase 0 notes)
  -- check `SELECT count(*) FROM gps_point WHERE source='tcx' AND source_key='<log_id>'`.
- `gps_confidence='windowed'`: recovered from `gps_location_csv` via a
  ±30s time-window search, not an exact per-activity file -- lower confidence by
  construction; a genuinely overlapping unrelated GPS trace on the same day is
  possible though not observed in practice.
- `gps_confidence='flagged_no_data'`: GPS was plausible (outdoor
  bike/run/walk/hike) but no coordinates were found in either source for that
  time window -- a real gap in the source data, not a bug. Common for
  `AUTO_DETECTED` activities, which frequently have no location data at all.
- `gps_confidence='not_expected'`: the activity type isn't GPS-plausible
  (indoor workout, etc.) -- no lookup was even attempted.

## Step 5: point to the fix, don't just describe the problem

End with either "this is expected behavior because X" (cite the specific
mechanism above) or a concrete file+line to change (almost always
`fitbit2garmin/reconcile/activity_type_map.py` for type issues,
`fitbit2garmin/reconcile/gps_attacher.py` for GPS matching issues,
`fitbit2garmin/reconcile/activity_matcher.py` for field-resolution issues). If a
mapping table is fixed, note that `fitbit2garmin reconcile <db_path>` must be
re-run (idempotent, safe) before re-generating output files.
