-- fitbit2garmin staging schema (v1)
-- Design: see PROGRESS.md / project plan. SQLite chosen as a staging layer between
-- raw Takeout parsing (ingest/) and file generation (output/) so both phases are
-- independently resumable and debuggable via plain SQL, without re-parsing 15GB+
-- of source files on every run.

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

-- Resume/audit registry: one row per source file, keyed by content hash so a
-- changed file (re-downloaded export, corrected data) is detected and re-ingested,
-- while an unchanged file is skipped on re-run.
CREATE TABLE IF NOT EXISTS ingest_file (
    file_path       TEXT PRIMARY KEY,   -- relative to Takeout root
    source_group    TEXT NOT NULL,      -- 'user_exercises','exercise_json','tcx','gps_location_csv',
                                         -- 'weight_json','monitoring_csv:<metric>','monitoring_json:<metric>'
    content_hash    TEXT NOT NULL,      -- sha256
    file_size_bytes INTEGER,
    mtime           REAL,
    ingested_at     TEXT,
    row_count       INTEGER,
    status          TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','ok','error')),
    error_message   TEXT
);
CREATE INDEX IF NOT EXISTS idx_ingest_file_group_status ON ingest_file(source_group, status);

-- Driving activity set (Health Fitness Data_GoogleData/UserExercises_*.csv).
-- ALL rows are kept, including sparse AUTO_DETECTED fragments with blank tracker
-- fields -- filtering happens nowhere in ingest, only (optionally) at output time.
CREATE TABLE IF NOT EXISTS raw_user_exercise (
    exercise_id INTEGER PRIMARY KEY,
    source_file TEXT NOT NULL REFERENCES ingest_file(file_path),
    exercise_start_utc TEXT NOT NULL,
    exercise_end_utc   TEXT NOT NULL,
    utc_offset TEXT,
    exercise_created_utc TEXT,
    exercise_last_updated_utc TEXT,
    activity_name TEXT NOT NULL,
    log_type TEXT NOT NULL,
    pool_length REAL,
    pool_length_unit TEXT,
    intervals TEXT,
    distance_units TEXT,
    tracker_total_calories REAL,
    tracker_total_steps INTEGER,
    tracker_total_distance_mm INTEGER,
    tracker_total_altitude_mm INTEGER,
    tracker_avg_heart_rate INTEGER,
    tracker_peak_heart_rate INTEGER,
    tracker_avg_pace_mm_per_second REAL,
    tracker_avg_speed_mm_per_second REAL,
    tracker_peak_speed_mm_per_second REAL,
    tracker_auto_stride_run_mm REAL,
    tracker_auto_stride_walk_mm REAL,
    tracker_swim_lengths INTEGER,
    tracker_pool_length REAL,
    tracker_pool_length_unit TEXT,
    tracker_cardio_load REAL,
    manually_logged_total_calories REAL,
    manually_logged_total_steps INTEGER,
    manually_logged_total_distance_mm INTEGER,
    manually_logged_pool_length REAL,
    manually_logged_pool_length_unit TEXT,
    events TEXT
);
CREATE INDEX IF NOT EXISTS idx_ue_start ON raw_user_exercise(exercise_start_utc);
CREATE INDEX IF NOT EXISTS idx_ue_source_file ON raw_user_exercise(source_file);

-- Classic Fitbit exercise log (Global Export Data/exercise-*.json). Matched to
-- raw_user_exercise by timestamp proximity during reconciliation, not by ID --
-- the two sources use unrelated numeric ID schemes.
CREATE TABLE IF NOT EXISTS raw_exercise_json (
    log_id INTEGER PRIMARY KEY,
    source_file TEXT NOT NULL REFERENCES ingest_file(file_path),
    start_time_utc TEXT NOT NULL,     -- parsed as UTC -- confirmed by cross-reference, see ingest/exercise_json.py
    last_modified_utc TEXT,
    activity_name TEXT,
    activity_type_id INTEGER,
    duration_ms INTEGER,
    active_duration_ms INTEGER,
    calories INTEGER,
    steps INTEGER,
    distance REAL,
    distance_unit TEXT,
    average_heart_rate INTEGER,
    elevation_gain REAL,
    has_gps INTEGER,                  -- 0/1, Fitbit's own flag -- treated as a hint, not authoritative
    log_type TEXT,
    source_device TEXT,
    activity_level_json TEXT,         -- raw passthrough blob
    heart_rate_zones_json TEXT,       -- raw passthrough blob
    used_by_activity_uid TEXT         -- set once claimed by an activity row during reconciliation
);
CREATE INDEX IF NOT EXISTS idx_ej_start ON raw_exercise_json(start_time_utc);
CREATE INDEX IF NOT EXISTS idx_ej_used_by ON raw_exercise_json(used_by_activity_uid);

-- GPS points from both sources: exact per-activity TCX files (Activities/*.tcx)
-- and day-scoped continuous location logs (Physical Activity_GoogleData/gps_location_*.csv).
CREATE TABLE IF NOT EXISTS gps_point (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL CHECK(source IN ('tcx','gps_location_csv')),
    source_file TEXT NOT NULL REFERENCES ingest_file(file_path),
    source_key TEXT NOT NULL,   -- tcx: logId as string; gps_location_csv: 'YYYY-MM-DD' of the day file
    point_time_utc TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    altitude_m REAL,
    distance_m REAL,            -- cumulative; from TCX <DistanceMeters> when present, else NULL
    heart_rate INTEGER,         -- embedded HR when the source carries it (TCX <HeartRateBpm>)
    sequence_in_source INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gps_source_key ON gps_point(source, source_key);
CREATE INDEX IF NOT EXISTS idx_gps_time ON gps_point(point_time_utc);

-- Canonical, reconciled activity entity -- one row per real workout, output/*.py
-- reads only from here (and activity_gps_point) to generate files.
CREATE TABLE IF NOT EXISTS activity (
    activity_uid TEXT PRIMARY KEY,           -- 'ue:{exercise_id}'
    user_exercise_id INTEGER NOT NULL REFERENCES raw_user_exercise(exercise_id),
    exercise_json_log_id INTEGER REFERENCES raw_exercise_json(log_id),
    start_time_utc TEXT NOT NULL,
    end_time_utc   TEXT NOT NULL,
    duration_s INTEGER NOT NULL,
    activity_name_raw TEXT NOT NULL,
    activity_type_id INTEGER,
    fit_sport INTEGER NOT NULL,
    fit_sub_sport INTEGER,
    log_type TEXT NOT NULL,
    has_metrics INTEGER NOT NULL DEFAULT 0,   -- 0 for sparse AUTO_DETECTED rows with no tracker data
    calories REAL,
    steps INTEGER,
    distance_m REAL,
    avg_heart_rate INTEGER,
    peak_heart_rate INTEGER,
    elevation_gain_m REAL,
    gps_source TEXT NOT NULL DEFAULT 'none' CHECK(gps_source IN ('tcx','gps_location_csv','none')),
    gps_confidence TEXT NOT NULL CHECK(gps_confidence IN ('exact','windowed','flagged_no_data','not_expected')),
    match_confidence TEXT NOT NULL CHECK(match_confidence IN ('time_exact','time_fuzzy','user_exercises_only')),
    reconciliation_notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_activity_start ON activity(start_time_utc);
CREATE INDEX IF NOT EXISTS idx_activity_gps ON activity(gps_source);

-- Ordered join of an activity to its GPS points (decouples raw point storage from
-- final per-activity assignment/ordering, since a day-file's points must be sliced
-- to the activity's time window, not assigned wholesale).
CREATE TABLE IF NOT EXISTS activity_gps_point (
    activity_uid TEXT NOT NULL REFERENCES activity(activity_uid),
    gps_point_id INTEGER NOT NULL REFERENCES gps_point(id),
    seq INTEGER NOT NULL,
    PRIMARY KEY (activity_uid, seq)
);

-- Full audit trail of every candidate considered during matching, not just the
-- winner -- backs the fitbit2garmin-debug-activity-type skill.
CREATE TABLE IF NOT EXISTS reconciliation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_exercise_id INTEGER NOT NULL,
    candidate_log_id INTEGER,
    delta_seconds REAL,
    chosen INTEGER NOT NULL DEFAULT 0,
    reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_reclog_ue ON reconciliation_log(user_exercise_id);

-- Rows that could not be turned into a valid activity at all (corrupt timestamps).
-- Distinct from unmapped-type or no-GPS cases, which still produce an activity row.
CREATE TABLE IF NOT EXISTS skipped_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_exercise_id INTEGER,
    reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS weight_entry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL REFERENCES ingest_file(file_path),
    log_id INTEGER UNIQUE,
    entry_date TEXT NOT NULL,   -- 'YYYY-MM-DD', local date as logged by Fitbit
    entry_time_utc TEXT,
    weight_kg REAL NOT NULL,
    bmi REAL,
    body_fat_pct REAL,
    source TEXT
);
CREATE INDEX IF NOT EXISTS idx_weight_date ON weight_entry(entry_date);

-- Generic wide table for lower-priority monitoring metrics (HR, steps, floors,
-- calories, distance, SpO2, HRV, altitude, ...). Row-per-reading; large table,
-- deliberately narrow schema so one ingest module can serve many metric_types.
CREATE TABLE IF NOT EXISTS monitoring_metric (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_type TEXT NOT NULL,
    source_file TEXT NOT NULL REFERENCES ingest_file(file_path),
    ts_utc TEXT NOT NULL,
    value REAL,
    value2 REAL,
    unit TEXT
);
CREATE INDEX IF NOT EXISTS idx_mm_type_time ON monitoring_metric(metric_type, ts_utc);

CREATE TABLE IF NOT EXISTS sleep_entry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL REFERENCES ingest_file(file_path),
    log_id INTEGER UNIQUE,
    start_time_utc TEXT,
    end_time_utc TEXT,
    duration_ms INTEGER,
    efficiency INTEGER,
    minutes_asleep INTEGER,
    minutes_awake INTEGER,
    type TEXT
);

CREATE TABLE IF NOT EXISTS sleep_stage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sleep_entry_id INTEGER NOT NULL REFERENCES sleep_entry(id),
    stage TEXT NOT NULL,
    start_time_utc TEXT NOT NULL,
    duration_s INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sleep_stage_entry ON sleep_stage(sleep_entry_id);
