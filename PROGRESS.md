# fitbit2garmin rewrite — progress

Plan: see conversation / `.claude/plans` for full design doc. Branch: `rewrite/v2` (off `main`, old code untouched on `main`).

## Phase 0 — Foundation ✅ DONE
- [x] `db/schema.sql` + `db/connection.py` (WAL mode) + `db/migrations.py`
- [x] `ingest/file_registry.py` (sha256-based resume registry)
- [x] `fitbit2garmin ingest <takeout_dir>` CLI command
- [x] Verify: row counts match known totals — 3,916 UserExercises ✓ / 3,816 exercise_json ✓ / 136 tcx files ✓ (128 with real GPS points + 8 confirmed-empty stubs, e.g. `Activities/2319579604.tcx` has no `<Track>` at all — real data, not a bug)
- [x] Verify: idempotent re-run adds 0 new rows
- Note: fixed two real bugs found via real-data testing: `raw_user_exercise` INSERT had 33 placeholders for 34 columns; TCX namespace is `xmlschemas` (plural) not `xmlschema` (singular), and `<Time>`/`<Id>` carry real local UTC offsets (`-04:00` etc.) requiring actual conversion, not just relabeling as UTC like `exercise-*.json`'s naive strings.

## Phase 1 — Activities (primary deliverable)
- [ ] `ingest/user_exercises.py`, `ingest/exercise_json.py`, `ingest/tcx_activities.py`, `ingest/gps_location_csv.py`
- [ ] `reconcile/activity_matcher.py` (timestamp join, claim guard, field fallback chains)
- [ ] `reconcile/gps_attacher.py` (TCX exact match + gps_location_csv time-window fallback)
- [ ] `reconcile/activity_type_map.py` (16-name + 15-id tables, GPS-refinement rule)
- [ ] `output/fit_activity.py` (primary — day-one UINT16 clamping, per-point try/except, chunking)
- [ ] `output/tcx_activity.py`, `output/gpx_activity.py` (secondary)
- [ ] `--sample N` validation-batch mode + stratified selection
- [ ] Unit tests: matcher, type-map, FIT round-trip
- [ ] **Gate**: user manually validates sample batch in real Garmin Connect

## Phase 2 — Weight
- [ ] `ingest/weight_json.py`
- [ ] `output/csv_garmin_import.py` (weight/BMI/fat, `Date,Weight,BMI,Fat`, `--locale` flag)
- [ ] **Gate**: user confirms real Garmin "Import Data From Fitbit" wizard accepts a small slice

## Phase 3 — Everything else (best-effort)
- [ ] `output/csv_garmin_import.py` extended for daily totals (steps/calories/distance/floors/active-minutes) — format unverified, confirm against real importer first
- [ ] `ingest/monitoring_csv.py`, `ingest/monitoring_json.py`
- [ ] `output/fit_monitoring.py` (sleep/HR/SpO2/HRV, chunked at 65535 records)
- [ ] `output/csv_archive.py`
- [ ] Explicit best-effort messaging surfaced to user on completion

## Phase 4 — Claude Code Skills
- [ ] `.claude/skills/fitbit2garmin-convert/SKILL.md`
- [ ] `.claude/skills/fitbit2garmin-validate-output/SKILL.md`
- [ ] `.claude/skills/fitbit2garmin-debug-activity-type/SKILL.md`

## Phase 5 — Full history run
- [ ] Full unbounded 2016–2025 conversion
- [ ] Final QA report review
- [ ] Manual upload by user
