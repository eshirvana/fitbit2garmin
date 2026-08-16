# fitbit2garmin rewrite — progress

Plan: see conversation / `.claude/plans` for full design doc. Branch: `rewrite/v2` (off `main`, old code untouched on `main`).

## Phase 0 — Foundation ✅ DONE
- [x] `db/schema.sql` + `db/connection.py` (WAL mode) + `db/migrations.py`
- [x] `ingest/file_registry.py` (sha256-based resume registry)
- [x] `fitbit2garmin ingest <takeout_dir>` CLI command
- [x] Verify: row counts match known totals — 3,916 UserExercises ✓ / 3,816 exercise_json ✓ / 136 tcx files ✓ (128 with real GPS points + 8 confirmed-empty stubs, e.g. `Activities/2319579604.tcx` has no `<Track>` at all — real data, not a bug)
- [x] Verify: idempotent re-run adds 0 new rows
- Note: fixed two real bugs found via real-data testing: `raw_user_exercise` INSERT had 33 placeholders for 34 columns; TCX namespace is `xmlschemas` (plural) not `xmlschema` (singular), and `<Time>`/`<Id>` carry real local UTC offsets (`-04:00` etc.) requiring actual conversion, not just relabeling as UTC like `exercise-*.json`'s naive strings.

## Phase 1 — Activities (primary deliverable) — code complete, awaiting human gate
- [x] `ingest/user_exercises.py`, `ingest/exercise_json.py`, `ingest/tcx_activities.py`, `ingest/gps_location_csv.py`
- [x] `reconcile/activity_matcher.py` (timestamp join, claim guard, field fallback chains)
- [x] `reconcile/gps_attacher.py` (TCX exact match + gps_location_csv time-window fallback)
- [x] `reconcile/activity_type_map.py` (16-name + 15-id tables, GPS-refinement rule)
- [x] `output/fit_activity.py` (primary — day-one UINT16 clamping, per-point try/except, chunking guard)
- [x] `output/tcx_activity.py`, `output/gpx_activity.py` (secondary)
- [x] `--sample N` validation-batch mode + stratified selection (`fitbit2garmin convert ... --sample 15`)
- [x] Unit tests: matcher (8 tests), type-map (39 tests), FIT round-trip (5 tests) — 55 new tests, all passing; 122 total with the old suite, no regressions
- [x] **Gate**: user manually validated the sample batch in real Garmin Connect — confirmed OK. Also diagnosed a user question: distance is null for AUTO_DETECTED activities (3,097/3,097 have no distance) vs always present for TRACKER activities (320/320) — confirmed this is a genuine Fitbit source-data gap (SmartTrack auto-detection has no GPS/phone location running), not a pipeline bug.

Real-data results on the user's actual 3,916-activity export: 3,808 exact time-matches to the classic exercise log, 108 unmatched (UserExercises-only, expected — wider date range), 8 orphan exercise_json records (small, as hoped), 0 unmapped activity types. GPS attached to 222 activities (128 exact TCX match + 94 via the gps_location_csv day-window fallback — genuinely recovering GPS the old `hasGps` flag missed), 3,327 flagged as a real no-data gap, 367 correctly not GPS-plausible.

Two real bugs found and fixed via real-data testing (not hypothetical): (1) FIT's `SessionMessage`/`LapMessage` have no `total_steps` field — silently no-ops if set; steps must be written as `total_strides = steps // 2`, matching Garmin's own convention. (2) Fitbit's `tracker_avg/peak_heart_rate` uses `0` (not NULL) to mean "not measured" on some real rows — resolved via a dedicated `_resolve_heart_rate` helper that treats 0 as missing, so a bogus 0bpm peak-below-average never gets written.

### Phase 1 follow-up: activity detail fields (avg speed, cadence, HR-zone breakdown, device name)

User audit ("is there any detail missing from activities that's available in Fitbit data?") found four real fields captured at ingest but never reaching FIT output: avg/max speed, running/walking cadence, per-zone HR time breakdown, recording device name. Added via migration `db/migrations/002_add_activity_detail_fields.sql` (new `activity` columns: `avg_speed_ms`, `avg_cadence`, `time_in_hr_zone_json`, `source_device`) plus `reconcile/activity_matcher.py` computation and `output/fit_activity.py` FIT field mapping.

Two more real bugs found before shipping this:
1. **`tracker_avg_speed_mm_per_second`/`tracker_peak_speed_mm_per_second` don't hold what their names claim.** Cross-checked against distance/duration on real rows: the field actually named `avg_speed` is ~100-130x too large to be m/s (74624 vs a computed 583), and `peak_speed` is sometimes *smaller* than the real average — physically impossible. The reliable value (matches distance/duration exactly) turned out to live in `tracker_avg_pace_mm_per_second`, a field that's zero/missing on most rows anyway. Rather than trust any of the three, `avg_speed_ms` is derived as `distance_m / duration_s` (no unit ambiguity, matches Garmin's own definition); `max_speed` stays GPS-derived only, unset for non-GPS activities.
2. **`avg_running_cadence` silently doesn't survive encode/decode for non-RUNNING sports.** `fit-tool` accepts the assignment without error for `Sport.WALKING`, but the value comes back `None` on decode — confirmed by a direct round-trip test before and after the fix. The generic `avg_cadence` field round-trips correctly for both Walking and Running; switched to that. This is the same class of bug as the `total_steps` issue in Phase 1 (attribute exists and accepts assignment ≠ actually encodes) — worth remembering for any future FIT field additions: verify round-trip, not just `hasattr`.

Migration tested against the user's real, already-populated `fitbit2garmin.sqlite3` (not just a fresh DB) to confirm `ALTER TABLE` applies cleanly to existing data. Real-data coverage: 421/3,916 activities got avg_speed (limited by how often distance is known), 3,389/3,916 got cadence, 3,765/3,916 got HR-zone breakdown, 392/3,916 got a device name (only populated on real tracker-synced records, matching the TRACKER vs AUTO_DETECTED split already documented above). Full 3,916-activity conversion re-run end-to-end with 0 errors, 0 skipped GPS points. 6 new tests (3 reconciliation-level, 3 FIT round-trip, including a regression test specifically for the cadence-field bug).

## Phase 2 — Weight — DONE (FIT path confirmed working by user)
- [x] `ingest/weight_json.py` — 216 real entries ingested. **Found and fixed a real unit bug**: raw `weight` values are in **pounds**, not kg, despite Fitbit's API nominally being metric — confirmed via `Your Profile/Profile.csv`'s `weight_unit: en_US` field and the value range (141-210) matching the profile's own kg-equivalent reference weight. Stored canonically as `weight_kg`.
- [x] `output/fit_weight.py` — **PRIMARY path, user-confirmed working** against their real Garmin Connect account. Replicates a prior confirmed-working file's structure exactly (found in `output/weight.fit`, generated by the old codebase, decoded with fit-tool to reverse-engineer the exact convention): `FileIdMessage(type=WEIGHT)`, one `WeightScaleMessage` per entry, weight in kg, timestamped at **noon UTC of the entry's date** (not the actual logged time) so Garmin buckets it into the correct calendar day regardless of account timezone. New pipeline's output is byte-for-byte value-equivalent to the confirmed file (216/216 entries, same timestamps, same weights to 2dp). 2 regression tests added.
- [x] `output/csv_garmin_import.py` — kept as a **secondary/experimental** path (`--format csv`). Went through two real bug fixes chasing a generic "An error occurred with your upload" error, informed by reading the actual source of `simonepri/fitbit2garmin` (a maintained reference tool): (1) missing literal `Body` marker line before the header row, (2) blank BMI/Fat instead of `0`, (3) wrong date format (was guessing dash-separated MM-DD-YYYY; fixed to ISO `YYYY-MM-DD` passthrough, confirmed via the reference tool's source), (4) **mixed line endings** — `csv.writer` defaults to CRLF while the hand-written `Body` marker line used LF, so a line-based parser would see a stray `\r` glued to the last field of every row (e.g. header `Fat` → `Fat\r`), silently breaking field/number parsing. Rewrote to plain `\n`-only writes throughout, matching the reference tool's approach exactly. **Not yet re-tested by the user** since the FIT path worked first — CSV fixes are real and tested for internal consistency, but the actual Garmin import success is unconfirmed.
- [x] **Gate passed**: user tried `weight.fit` and confirmed it imports successfully into their real Garmin Connect account.

## Phase 3 — Everything else (best-effort) — code complete
- [x] `ingest/monitoring_csv.py`: steps/calories/distance/floors aggregated to **daily sums** (source is minute-level; storing raw would reproduce the old codebase's 19M-row HR memory problem for explicitly lowest-priority data). resting_heart_rate + heart_rate_variability are single unsharded files; oxygen_saturation is date-sharded (23 files) — confirmed by direct inspection, handled as two distinct ingest paths.
- [x] `ingest/monitoring_json.py`: active-minutes (sedentary/lightly/fairly/very) are already daily in `Global Export Data` (unlike steps/calories/distance in the same dir) — direct passthrough. Sleep (`sleep-*.json`) → `sleep_entry`/`sleep_stage`, including per-stage detail when present (2016-2025 mix of "classic" 3-stage and newer 4-stage sleep data).
- [x] `output/csv_garmin_import.py` extended: `write_daily_totals_csv` using the confirmed `Activities` marker format (same source as the weight CSV fix — `simonepri/fitbit2garmin`'s real code). **Found and fixed a real unit bug before it shipped**: `distance` readings are in **meters** (confirmed via `Physical Activity_GoogleData/distance_readme.txt`), but the aggregator was treating the raw daily sum as already being km — a 1000x error. Caught by sanity-checking a summed day's value (22,109 for one day is absurd as km, plausible as meters) before ever exporting. Fixed + regression-tested.
- [x] `output/fit_monitoring.py`: sleep.fit (MonitoringMessage per stage segment, matching the old codebase's confirmed-structurally-valid pattern), resting_hr.fit / spo2.fit (RecordMessage-based daily mini-sessions), hrv.fit (dedicated `HrvMessage.time` field — FIT's HRV message is built for raw R-R intervals, not a daily summary, so the daily RMSSD is stored as a single-element list in seconds; a known representational compromise carried forward from the old code, not reinvented). All chunked at 65535 from the start.
- [x] Explicit best-effort messaging in `export-monitoring` CLI output.
- **User-tested against a real Garmin Connect account, both confirmed broken**:
  - `sleep.fit` (`FileType.MONITORING_B`) is **rejected outright**: "Sorry, your upload failed. Register your device, and try again." Garmin's manual-upload validation checks monitoring-type files against registered devices, unlike activity/weight uploads. Root-caused, not guessed: forum threads confirm this exact error ties to FIT `FileIdMessage` manufacturer/serial-number validation. The only known workarounds spoof a real device's identity (tools like "FIT File Faker") — declined; that's circumventing a device-authenticity control, not fixing our format.
  - `resting_hr.fit`/`spo2.fit`/`hrv.fit` (`FileType.ACTIVITY`, one mini-session per daily reading) **upload successfully but each reading shows up as its own fake zero-duration activity**, polluting the user's real activity history. Confirmed worse than not working.
  - **Fix**: built `output/csv_archive.py` (personal-reference CSV, not Garmin-importable) as the new default for these four; `export-monitoring --include-fit-monitoring` re-enables the FIT path with explicit warnings for anyone who wants it despite the above. Daily-totals CSV (steps/calories/distance/floors/active-minutes, via Garmin's official CSV import) is unaffected — different mechanism, not implicated in either failure.
- 8 new tests (Body/Activities marker, no-blank-fields, no-mixed-line-endings, distance unit conversion, csv_archive coverage). 67 total tests passing (the old test_converter.py suite was removed in the cleanup pass — see below).
- Real-data run: 3,360 days of daily totals, 45,661 sleep stage records, 2,602 resting-HR days, 515 SpO2 days, 844 HRV days — now delivered as CSV archives by default.

## Phase 4 — Claude Code Skills — DONE
- [x] `.claude/skills/fitbit2garmin-convert/SKILL.md` — when to use --sample vs full run, never silently pass --allow-unmapped, surfaces QA summaries
- [x] `.claude/skills/fitbit2garmin-validate-output/SKILL.md` — decode FIT via fit-tool, cross-check against the `activity` SQLite row, documents what's expected to differ (peak HR estimate, Haversine-derived distance) vs a real bug
- [x] `.claude/skills/fitbit2garmin-debug-activity-type/SKILL.md` — walks `reconciliation_log` audit trail, traces the exact type-mapping precedence and GPS-refinement rule, points to the specific file/line to fix

## Phase 5 — Full history run — DONE
- [x] Full unbounded 2016–2025 conversion: `uv run fitbit2garmin convert <takeout_dir> --db ./fitbit2garmin.sqlite3 --output-dir ./output/full`
- [x] All 3,916 activities converted: 3,916 `.fit` (primary), 3,916 `.tcx` (secondary), 222 `.gpx` (GPS-attached only) — 0 GPS points skipped across 562,180 total points
- [x] Weight: 216/216 entries in `output/weight.fit` (confirmed working)
- [x] Monitoring (best-effort): 3,360 days of daily totals CSV, 45,661 sleep-stage records, 2,602 resting-HR/515 SpO2/844 HRV daily records
- [ ] **Manual upload by user** — this is the one step the tool cannot do itself (no Garmin API, by design). `output/full/fit/` is ready to upload.

## Test suite: 67 passing (all new; the old test_converter.py suite was removed when superseded old code was cleaned up)
