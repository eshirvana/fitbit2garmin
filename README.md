# Fitbit to Garmin Migration Tool

A comprehensive Python tool for converting Fitbit Google Takeout data to Garmin Connect compatible formats, maximizing data preservation across all available metrics.

## Features

- **Comprehensive Data Support**: Converts 15+ data types from Fitbit including activities, sleep, heart rate, body composition, and daily metrics
- **Multiple Export Formats**: Supports CSV, TCX, GPX, and FIT formats
- **Correct Sport Type Mapping**: 30+ activity types mapped to the right Garmin sport in FIT files (Tennis, Basketball, Soccer, Golf, Rowing, Boxing, and more)
- **GPS Data Extraction**: Reads GPS tracks from Fitbit's `Activities/*.tcx` files and embeds them in exported TCX/GPX/FIT files
- **Advanced Heart Rate Zone Analysis**: Age-based zone calculations with Garmin compatibility
- **Batch Processing**: Handles years of historical data efficiently with parallel processing
- **Resume Capability**: Skip already-processed files on interrupted conversions
- **Command Line Interface**: Easy-to-use CLI with progress tracking

## Supported Data Types

### ✅ Fully Supported
- **Activities**: GPS tracks, exercise sessions with enhanced data (TCX/GPX/FIT export)
- **Heart Rate Zones**: Advanced zone calculation and recalibration with age-based formulas
- **Daily Metrics**: Steps, calories, distance, floors (CSV export)
- **Body Composition**: Weight, BMI, body fat percentage (CSV export)
- **Sleep Data**: Duration, stages, sleep score, REM/deep sleep analysis (CSV export)
- **Heart Rate**: Continuous HR, resting HR, zone analysis (CSV export)
- **GPS Data**: Extracted from Fitbit's `Activities/` TCX files with speed and elevation

### ⚠️ Partially Supported
- **Heart Rate Variability**: Sleep HRV data
- **Stress Scores**: Daily stress levels
- **Active Zone Minutes**: Cardio/peak minutes
- **Temperature Data**: Skin temperature variations
- **SpO2 Data**: Blood oxygen levels

## Activity Type Mapping

The tool maps 30+ Fitbit activity types to Garmin sport categories. **FIT files carry the full, correct sport type** — TCX files are limited to Running, Walking, Biking, Swimming, and Other by the TCX schema.

| Fitbit Activity | Garmin Sport (FIT) | TCX Sport |
|---|---|---|
| Running | Running | Running |
| Walking | Walking | Walking |
| Biking | Cycling - Road | Biking |
| Indoor Cycling | Cycling - Indoor | Biking |
| Hiking | Hiking | Walking |
| Swimming | Swimming - Lap | Swimming |
| Treadmill | Running - Treadmill | Running |
| Tennis | **Tennis** | Other ⚠️ |
| Basketball | **Basketball** | Other ⚠️ |
| Soccer | **Soccer** | Other ⚠️ |
| Golf | **Golf** | Other ⚠️ |
| Rowing | **Rowing** | Other ⚠️ |
| Boxing | **Boxing** | Other ⚠️ |
| Rock Climbing | **Rock Climbing** | Other ⚠️ |
| Yoga | Training - Yoga | Other |
| Pilates | Training - Pilates | Other |
| Weights / Abs | Training - Strength | Other |
| HIIT / Aerobic | Training - Cardio | Other |
| Elliptical | Elliptical | Other |
| Stair Climbing | Stair Climbing | Other |
| Alpine Skiing | Alpine Skiing | Other ⚠️ |
| Snowboarding | Snowboarding | Other ⚠️ |

> ⚠️ **These sports require FIT format** to appear with the correct sport type in Garmin Connect. Import `.fit` files, not `.tcx`, for these activities.

The `debug-activities` command shows both TCX and FIT sport for every activity in your dataset.

## Installation

### Requirements
- Python 3.9 or higher

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Development Installation
```bash
pip install -e .
```

## Usage

### 1. Export Your Fitbit Data
1. Go to [Google Takeout](https://takeout.google.com/)
2. Select only "Fitbit" data
3. Choose your export format (ZIP recommended)
4. Download and extract the archive

### 2. Convert Your Data

#### Basic Conversion
```bash
fitbit2garmin convert path/to/Takeout
```

#### Specify Output Directory
```bash
fitbit2garmin convert path/to/Takeout --output-dir ./my-garmin-data
```

#### Choose Export Formats
```bash
fitbit2garmin convert path/to/Takeout --format csv --format tcx --format gpx --format fit
```

#### Export Only Activities
```bash
fitbit2garmin convert path/to/Takeout --activities-only
```

#### Export Only Daily Metrics
```bash
fitbit2garmin convert path/to/Takeout --daily-only
```

### 3. Analyze Your Data (Optional)
```bash
fitbit2garmin analyze path/to/Takeout
```

### 4. Debug Activity Types
```bash
fitbit2garmin debug-activities path/to/Takeout
```

Shows TCX sport and FIT sport side-by-side for every activity type detected in your data, flagging any that need FIT format for correct sport display.

### 5. Get Help
```bash
fitbit2garmin --help
fitbit2garmin convert --help
fitbit2garmin info
```

## Output Files

### CSV Files (Personal Data Archive)
These files cannot be imported into Garmin Connect — Garmin Connect has no CSV import for daily health metrics. They are useful as a personal archive, for analysis in spreadsheet tools, or as input for third-party scripts.

- `fitbit_steps.csv` - Daily step counts
- `fitbit_distance.csv` - Daily distance traveled
- `fitbit_calories.csv` - Daily calories burned
- `fitbit_sleep.csv` - Sleep duration and quality
- `fitbit_heart_rate.csv` - Daily heart rate summaries
- `fitbit_heart_rate_zones.csv` - Heart rate zone analysis per activity
- `fitbit_body_composition.csv` - Weight and body metrics
- `fitbit_activities.csv` - Activity summaries and metrics
- `garmin_connect_import.csv` - Combined summary (reference only)

### Activity Files
- `{type}_{logid}_{timestamp}.tcx` - Individual activities in TCX format
- `activity_{logid}_{timestamp}.gpx` - GPS activities in GPX format
- `{type}_{logid}_{timestamp}.fit` - Native Garmin FIT format (**recommended**)

## Importing to Garmin Connect

> **Important**: Garmin Connect only supports importing **activity files** (FIT, TCX, GPX). It has no import mechanism for daily health metrics such as steps, sleep, body weight, or heart rate. Those data points are synced from Garmin devices — there is no way to bulk-import them from CSV files.

### Uploading Activities
1. Log into [Garmin Connect](https://connect.garmin.com/)
2. Go to **Import Data** in the menu (or drag-and-drop on the web app)
3. Upload **FIT files** — recommended for all activities; carries the correct sport type for all 30+ activity types
4. If FIT upload fails for a specific activity, try the **TCX** or **GPX** file instead

### Recommended Format Priority
1. **FIT files** — correct sport types for all activities, intraday HR embedded, GPS bounding box
2. **TCX files** — fallback; sport limited to Running/Walking/Biking/Swimming/Other
3. **GPX files** — GPS track only, no HR or distance metadata

## Advanced Heart Rate Zone Features

### Smart Zone Recalculation
- **Age-based formulas**: Uses Tanaka, Fox, Gellish, and Nes formulas for max HR estimation
- **Karvonen method**: Calculates zones using heart rate reserve when resting HR is available
- **User profile estimation**: Automatically determines fitness level from activity patterns
- **Zone validation**: Checks for overlaps, gaps, and realistic heart rate ranges

### Multiple Zone Systems
- **Garmin Standard**: 5-zone system (Active Recovery, Aerobic Base, Aerobic, Lactate Threshold, Neuromuscular)
- **5-Zone System**: Traditional zones (Recovery, Aerobic, Tempo, Threshold, Anaerobic)
- **Fitbit Mapping**: Converts Fitbit's 3-zone system to Garmin's 5-zone system

## Data Quality and Limitations

### What Transfers to Garmin Connect (via FIT/TCX/GPX upload)
- ✅ Activity GPS tracks (extracted from Fitbit's `Activities/` directory)
- ✅ Exercise duration, calories, distance
- ✅ Per-activity heart rate (intraday data embedded in FIT records)
- ✅ Heart rate zones with age-based recalculation
- ✅ 30+ activity types with correct Garmin sport mapping
- ✅ Distance unit normalization (miles → km for US accounts)

### What Is Exported as CSV (archive/analysis only — not importable into Garmin Connect)
- 📁 Daily step counts and distance
- 📁 Sleep duration, efficiency, and sleep stages (REM, light, deep)
- 📁 Body weight and composition
- 📁 Daily heart rate summaries and resting HR

### Known Limitations
- ⚠️ TCX format only supports Running/Walking/Biking/Swimming/Other — import `.fit` files for Tennis, Basketball, Soccer, etc.
- ⚠️ Heart rate variability data is limited to sleep periods
- ⚠️ Stress data may not be fully compatible with Garmin Connect
- ⚠️ Some Fitbit-specific metrics don't have Garmin equivalents
- ⚠️ Time zones may need manual adjustment
- ⚠️ Historical heart rate zones use estimated max HR (unless actual max HR is recorded)

## Troubleshooting

### Common Issues

**"Fitbit data not found"**
- Ensure you've extracted the Google Takeout archive
- Check that the path contains a `Fitbit/` directory inside the Takeout folder

**"No data found to convert"**
- Verify your Fitbit account had data in the export period
- Check that JSON files exist in the Fitbit directory

**"Import failed in Garmin Connect"**
- Try uploading FIT files first (best compatibility)
- Try uploading files in smaller batches
- Ensure date formats match your Garmin Connect region settings

**"Activities show as 'Other' in Garmin Connect"**
- This is a TCX format limitation — TCX only supports Running, Walking, Biking, Swimming, and Other
- **Solution**: Import the `.fit` files instead of `.tcx` files — FIT carries the correct sport type for all 30+ activity types (Tennis, Basketball, Soccer, Golf, Rowing, Boxing, etc.)
- Run `fitbit2garmin debug-activities path/to/Takeout` to see which activities need FIT format
- The convert command also prints a summary of activities that require FIT format

**"GPS data missing from activities"**
- GPS is stored by Fitbit in separate `.tcx` files under `Activities/` in your Takeout export
- The tool automatically matches and extracts GPS from these files
- If an activity still lacks GPS, the original Fitbit recording may not have had GPS enabled

**"Distance looks wrong"**
- Fitbit exports distance in the account's locale unit (miles for US accounts, km for others)
- The tool automatically detects the unit and normalizes to km

**"Heart rate zones seem incorrect"**
- Review the `fitbit_heart_rate_zones.csv` file for zone calculations
- Tool estimates max HR from age if not recorded in your data
- Consider manually setting heart rate zones in Garmin Connect if needed

### Getting Help
- Use `--verbose` for detailed logging
- Use `fitbit2garmin analyze` to inspect your data
- Use `fitbit2garmin debug-activities` to debug activity type issues

## Development

### Project Structure
```
fitbit2garmin/
├── models.py             # Pydantic data models and enums
├── parser.py             # Fitbit JSON parser; GPS extraction from Activities/ TCX files
├── converter.py          # TCX, GPX, and FIT file generation
├── exporter.py           # CSV exporters (Pandas-based)
├── heart_rate_zones.py   # Heart rate zone calculations (Tanaka, Fox, Gellish, Nes)
├── cli.py                # Click CLI commands
├── utils.py              # Parallel processing and resume manager
└── __init__.py           # Package initialization
```

### Code Style
```bash
black fitbit2garmin/
flake8 fitbit2garmin/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Acknowledgments

- Fitbit for providing data export functionality
- Garmin for supporting data import
- The open-source community for GPX, TCX, and FIT libraries

## Disclaimer

This tool is not affiliated with Fitbit or Garmin. Use at your own risk and always backup your data before migration.
