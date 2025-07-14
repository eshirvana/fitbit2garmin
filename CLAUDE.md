# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based tool for converting Fitbit Google Takeout data to Garmin Connect compatible formats. The tool maximizes data preservation across all available metrics including activities, sleep, heart rate, body composition, and daily metrics.

## Development Commands

### Setup and Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Development installation
pip install -e .
```

### Running the Tool
```bash
# Basic usage
fitbit2garmin convert path/to/Takeout

# With options
fitbit2garmin convert path/to/Takeout --output-dir ./output --format csv --format tcx

# Analyze data
fitbit2garmin analyze path/to/Takeout

# Get help
fitbit2garmin --help
```

### Testing and Quality
```bash
# Run tests (when implemented)
python -m pytest tests/

# Code formatting
black fitbit2garmin/
flake8 fitbit2garmin/
```

## Architecture

### Core Components
- **models.py**: Pydantic data models for all Fitbit data types
- **parser.py**: JSON parser for Google Takeout data structure
- **converter.py**: Converts data to TCX/GPX formats for activities
- **exporter.py**: Exports data to CSV formats compatible with Garmin Connect
- **cli.py**: Command-line interface with Click framework

### Data Flow
1. Parse Fitbit JSON files from Google Takeout
2. Convert to structured data models
3. Export to multiple formats (CSV, TCX, GPX)
4. Generate Garmin Connect import-ready files

### Key Features
- Supports 15+ Fitbit data types
- Handles years of historical data
- Multiple export formats (CSV, TCX, GPX, FIT planned)
- Comprehensive data validation
- Progress tracking and error handling

## Dependencies

### Core Libraries
- `gpxpy`: GPX file generation
- `fit-tool`: FIT file creation (planned)
- `tcxreader`: TCX file handling
- `pandas`: Data manipulation
- `pydantic`: Data validation
- `click`: CLI framework
- `tqdm`: Progress bars

### Data Types Supported
- Activities (GPS tracks, exercise sessions)
- Daily metrics (steps, calories, distance, floors)
- Sleep data (duration, stages, efficiency)
- Heart rate data (continuous monitoring)
- Body composition (weight, BMI, body fat)
- Heart rate variability (limited)
- Stress data (limited)

## Common Tasks

### Adding New Data Type Support
1. Add data model to `models.py`
2. Add parser method to `parser.py`
3. Add export method to `exporter.py`
4. Update CLI help text

### Fixing Data Parsing Issues
- Check JSON structure in Google Takeout data
- Verify date/time parsing with different formats
- Handle missing or null values gracefully

### Improving Export Formats
- Check Garmin Connect import specifications
- Validate generated files with external tools
- Test with real Garmin Connect uploads