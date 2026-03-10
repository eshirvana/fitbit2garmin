# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Python CLI tool (v1.1.0) that converts Fitbit Google Takeout data to Garmin Connect compatible formats. Entry point: `fitbit2garmin.cli:main`.

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Development installation (editable)
pip install -e .

# Run the tool
fitbit2garmin convert path/to/Takeout
fitbit2garmin convert path/to/Takeout --output-dir ./output --format csv --format tcx --format gpx --format fit
fitbit2garmin analyze path/to/Takeout
fitbit2garmin debug-activities path/to/Takeout  # Debug activity type mapping

# Code formatting
black fitbit2garmin/
flake8 fitbit2garmin/
```

No tests currently exist in the repo.

## Architecture

### Module Responsibilities

- **models.py**: Pydantic v2 data models — `ActivityType` enum (27 types), `SleepStage` enum, and dataclasses for all metric types. `FitbitUserData` is the master container passed between pipeline stages.
- **parser.py** (`FitbitParser`): Largest file (1500+ lines). Discovers and parses all Google Takeout JSON files. Handles the 14+ possible Fitbit directory names, maps 50+ Fitbit activity type IDs to `ActivityType` enum, and enhances HR zones via `heart_rate_zones.py`.
- **converter.py** (`DataConverter`): Generates TCX, GPX, and FIT files per activity. TCX includes Garmin HR zone extensions. FIT uses `fit-tool`. Activity types are mapped to Garmin-recognized sport categories.
- **exporter.py** (`GarminExporter`): Pandas-based CSV export — produces separate files per metric type (steps, distance, calories, sleep, body composition, HR zones, etc.).
- **heart_rate_zones.py** (`HeartRateZoneCalculator`): Supports 4 max-HR estimation formulas (Tanaka, Fox, Gellish, Nes), Karvonen/percentage zone methods, and 3→5 zone mapping from Fitbit to Garmin zones.
- **utils.py**: `ParallelProcessor` (ProcessPoolExecutor with memory monitoring and fallback to sequential) and `ResumeManager` (MD5-hash-based state persistence in `.fitbit2garmin_cache/`).
- **cli.py**: Click commands — `convert`, `analyze`, `debug-activities`, `info`.

### Data Flow

```
Google Takeout directory
    → FitbitParser.parse_all_data()       # discovers dirs, streams JSON, maps types
    → FitbitUserData (models.py)          # validated Pydantic container
    → DataConverter.batch_convert_activities()  # → .tcx / .gpx / .fit files
    → GarminExporter.export_all_data()         # → CSV files per metric
```

### Performance Features

- **Streaming JSON** (ijson): activated for files >10 MB; falls back to orjson then stdlib `json`
- **Parallel processing**: ProcessPoolExecutor, max 8 workers, 30–120s timeouts per file, falls back to sequential on failure
- **Memory limits**: 1 GB warning, enforced via psutil; GC triggered every 1000 items
- **Resume**: file hashes stored in `.fitbit2garmin_cache/` allow skipping already-processed files (`--resume` flag)

### Adding a New Data Type

1. Add model to `models.py` and field to `FitbitUserData`
2. Add `_parse_<type>()` method in `parser.py`; call it from `parse_all_data()`
3. Add `_export_<type>_csv()` in `exporter.py`; call from `export_all_data()`
4. Update CLI help text and output summary in `cli.py`

### Activity Type Mapping

Fitbit uses numeric IDs (e.g., `90009` → `RUN`, `20049` → `TREADMILL`) plus string name fallbacks. The mapping table lives in `parser.py:_map_activity_type()`. Converter maps these to TCX sport strings and FIT sport values in `converter.py`.
