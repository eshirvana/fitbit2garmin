---
name: fitbit2garmin-validate-output
description: Decode a generated FIT/TCX/GPX file from fitbit2garmin's output and cross-check it against the source data in the SQLite staging DB. Use when the user asks to verify, check, validate, or double-check that a converted file's activity type, GPS track, distance, calories, or heart rate actually matches the source Fitbit data.
---

# fitbit2garmin: validate a generated output file

This checks that a generated `.fit`/`.tcx`/`.gpx` file actually matches its
source data in the SQLite staging DB (`fitbit2garmin.sqlite3` by default) --
the "does the decoded FIT match the source" loop that should run before trusting
a full-history conversion, not just visual inspection in Garmin Connect.

## Step 1: identify the activity

The filename encodes the activity_uid: `<ActivityName>_ue_<exercise_id>_<timestamp>.fit`.
The `ue_<exercise_id>` part maps directly to `activity_uid = 'ue:<exercise_id>'`.

```bash
sqlite3 fitbit2garmin.sqlite3 "SELECT * FROM activity WHERE activity_uid='ue:<exercise_id>'"
```

This is the ground truth: `fit_sport`/`fit_sub_sport` (integer FIT enum values --
cross-reference against `fitbit2garmin/reconcile/activity_type_map.py` for the
name), `distance_m`, `calories`, `avg_heart_rate`, `peak_heart_rate`,
`gps_source`/`gps_confidence` (whether GPS is expected at all, and from which
source), `start_time_utc`/`end_time_utc`/`duration_s`.

## Step 2: decode the generated file

```python
from fit_tool.fit_file import FitFile

f = FitFile.from_file("output/fit/<filename>.fit")
sessions = [r.message for r in f.records if r.message.__class__.__name__ == "SessionMessage"]
records = [r.message for r in f.records if r.message.__class__.__name__ == "RecordMessage"]
s = sessions[0]
print(s.sport, s.sub_sport, s.total_distance, s.total_calories, s.avg_heart_rate, s.max_heart_rate)
print(len(records), "GPS/record points")
```

Note: decoded `sport`/`sub_sport` come back as plain integers, not enum
instances -- compare against `Sport.X.value`/`SubSport.X.value`, not `Sport.X`
directly (an easy mistake -- see `tests/test_fit_roundtrip.py` for the pattern).

For TCX/GPX, they're just XML -- `xml.etree.ElementTree` or a quick grep for
`<Trackpoint>`/`<trkpt>` counts is enough; TCX's `Sport` attribute is expected to
often say "Other" even for GPS-tracked activities (TCX schema limitation, FIT is
authoritative for sport type -- this is not a bug to chase).

## Step 3: compare, and know what's EXPECTED to differ

- `sport`/`sub_sport`: should match the mapping in `activity_type_map.py` for
  the source `activity_type_id`/`activity_name_raw`, refined by the GPS rule
  (CYCLING/RUNNING GENERIC → ROAD/STREET, SWIMMING LAP → OPEN_WATER, only when
  `gps_source != 'none'`).
- `total_distance`: should match `activity.distance_m` almost exactly (within
  ~1m) if it was source-provided; if `activity.distance_m` was NULL and GPS is
  present, it's Haversine-computed from the track instead -- won't be exact but
  should be a plausible value for the activity's duration/type.
- Record count: should equal the number of `activity_gps_point` rows for that
  `activity_uid` if GPS-attached, or exactly 2 (bare start/end) if
  `gps_source='none'` -- this is a hard invariant, not a heuristic.
- `avg_heart_rate`/`max_heart_rate`: should match `activity.avg_heart_rate`/
  `peak_heart_rate` -- but remember these can legitimately be NULL (0bpm
  readings are treated as "not measured", not written) and `peak_heart_rate` is
  frequently a zone-boundary estimate from `heart_rate_zones_json`, not a true
  measured peak -- don't flag that as a bug, it's a known data-source limitation.
- `total_strides`: FIT has no `total_steps` field (a real bug found and fixed in
  this project -- see PROGRESS.md) -- steps are `activity.steps // 2`.

## Step 4: report mismatches precisely

State the field, the decoded value, the source value, and whether the
difference is expected (per the list above) or a real discrepancy. If it looks
like a real discrepancy, hand off to `fitbit2garmin-debug-activity-type` for the
type-mapping case, or point at the specific `output/fit_activity.py` code path
for anything else (GPS handling, field resolution).
