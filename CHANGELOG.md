# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-08-15

Complete from-scratch rewrite, built and validated against a real ~3,900-activity
Takeout export and a real Garmin Connect account. See `PROGRESS.md` for the full
build history and `README.md` for real bugs found during development.

### Added
- SQLite-based staging architecture (`ingest` -> `reconcile` -> `output`),
  replacing the old in-memory Pydantic pipeline — idempotent, resumable via
  per-file content hash, and directly queryable for debugging
- Activity reconciliation across three previously-unhandled overlapping Fitbit
  data sources (`UserExercises_*.csv`, `exercise-*.json`, two independent GPS
  sources), with a full audit trail of every match candidate considered
- Activity-type mapping table derived from real data actually present in a
  Takeout export, not a generic 80-entry ID list
- `export-weight`: FIT (confirmed working against a real account) and Garmin's
  official CSV import format (needed multiple real bug fixes)
- `export-monitoring`: sleep/resting-HR/SpO2/HRV as a personal-reference CSV
  archive (not Garmin-importable — see Fixed below) + daily-totals CSV,
  explicitly scoped to daily granularity to stay memory-proportionate
- `batch-output`: splits large output folders into upload-sized batches —
  Garmin's web importer is confirmed unreliable with very large single-shot
  uploads
- `--sample`/`--sample-days` validation-batch modes on every export command
- Three Claude Code Skills for running conversions, validating output against
  source data, and debugging activity-type/GPS issues
- 64 new tests grounded in real data shapes and real bugs found, replacing the
  old synthetic-only suite

### Removed
- The old Fitbit API GPS-fetch path (`fetch-gps`) — Fitbit's legacy Web API is
  being shut down by Google in September 2026, and it turned out unnecessary:
  Takeout already includes GPS via `Activities/*.tcx` and a day-level location
  log, no live API access needed
- `analyze`/`debug-activities` commands — superseded by direct SQLite queries
  and the `fitbit2garmin-debug-activity-type` skill
- CSV daily-metrics export as a personal-archive-only feature — daily totals
  now go through Garmin's official import CSV instead
- `parser.py`, `converter.py`, `exporter.py`, `utils.py`, `gps_fetcher.py`,
  `heart_rate_zones.py`, `models.py` and their Pydantic/pandas/ijson/orjson/
  psutil/tqdm/requests/tcxreader dependencies — fully superseded by the new
  architecture

### Fixed (real bugs, found via testing against real data/a real account)
- FIT's `total_steps` field doesn't exist on Session/Lap messages — was
  silently dropping step counts; now written as `total_strides`
- Fitbit encodes "heart rate not measured" as `0`, not null, on some rows —
  was producing an impossible peak-below-average heart rate
- Fitbit Takeout weight values are in pounds despite the API being nominally
  metric
- Garmin's weight-CSV importer requires a literal `Body` marker line and
  non-blank fields, and broke on mixed CRLF/LF line endings
- Daily `distance` readings are in meters, not km — an early draft of the
  daily-totals exporter overstated distance 1000×
- `sleep.fit` is rejected outright by Garmin Connect's manual import ("Sorry,
  your upload failed. Register your device, and try again.") — monitoring-type
  FIT files are validated against registered devices, unlike activity/weight
  uploads. `resting_hr.fit`/`spo2.fit`/`hrv.fit` upload successfully but each
  daily reading pollutes the user's real activity history as a fake
  zero-duration activity. Both confirmed against a real account; fixed by
  defaulting `export-monitoring` to a personal-reference CSV archive instead
  (`--include-fit-monitoring` re-enables the FIT path for anyone who wants it
  despite the above)

## [1.1.0] - 2025-07-15

### Added
- **Advanced Heart Rate Zone Analysis**: Age-based zone calculations with multiple formulas (Tanaka, Fox, Gellish, Nes)
- **Heart Rate Reserve Calculations**: Karvonen method for more accurate zone boundaries
- **Smart Zone Recalculation**: Automatically estimates user profile from historical data
- **Multiple Zone Systems**: Support for Garmin Standard, 5-Zone System, and Fitbit mapping
- **Enhanced GPS Processing**: Speed calculation using Haversine formula and elevation data
- **Comprehensive Sleep Analysis**: REM, light, deep sleep stage detection with biometric data
- **FIT File Format Support**: Native Garmin format with comprehensive sensor data
- **Memory Management**: Intelligent memory monitoring and resource cleanup
- **Activity Type Expansion**: Support for 25+ activity types with proper Garmin mapping
- **Debug Tools**: `debug-activities` command for activity type analysis
- **Parallel Processing**: Chunked processing with automatic fallback for large datasets

### Enhanced
- **Heart Rate Zone Export**: Detailed CSV export with zone breakdowns and calculations
- **TCX Files**: Now include heart rate zone extensions for better Garmin Connect compatibility
- **FIT Files**: Include heart rate zone time distribution and comprehensive activity data
- **Error Handling**: Better error recovery and data validation throughout the pipeline
- **User Experience**: Improved progress tracking and informative status messages

### Fixed
- **Memory Issues**: Resolved excessive memory usage during parallel processing of large datasets
- **Resource Leaks**: Fixed multiprocessing resource cleanup and semaphore leaks
- **Package Installation**: Resolved setuptools conflicts and dependency issues
- **Data Type Errors**: Fixed heart rate parsing errors with non-dictionary data
- **Activity Type Mapping**: Comprehensive mapping to reduce "Other" activities in Garmin Connect

### Technical Improvements
- **Dependencies**: Added `psutil`, `orjson`, and `ijson` for better performance and monitoring
- **Code Quality**: Comprehensive code formatting and type safety improvements
- **Documentation**: Enhanced README with detailed feature descriptions and troubleshooting
- **Performance**: Optimized JSON parsing and memory usage for large datasets
- **Reliability**: Added timeouts, fallback mechanisms, and robust error handling

### Breaking Changes
- None - this is a backward-compatible release

## [1.0.0] - 2025-07-15

### Added
- Initial release of Fitbit to Garmin migration tool
- Basic data parsing for activities, sleep, daily metrics, and heart rate
- CSV, TCX, and GPX export formats
- Command-line interface with multiple commands
- Resume capability for interrupted conversions
- Parallel processing for large datasets
- Basic heart rate zone support
- Activity type mapping and recognition
- Sleep data extraction with basic metrics
- Body composition and health metrics export