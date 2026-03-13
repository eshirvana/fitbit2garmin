# Fitbit to Garmin Migration Tool

A comprehensive Python tool for converting Fitbit Google Takeout data to Garmin Connect compatible formats, maximizing data preservation across all available metrics.

## Features

- **Comprehensive Data Support**: Converts 15+ data types from Fitbit including activities, sleep, heart rate, body composition, and daily metrics
- **Multiple Export Formats**: Supports CSV, TCX, GPX, and FIT formats
- **Correct Sport Type Mapping**: 30+ activity types mapped to the right Garmin sport in FIT files (Tennis, Basketball, Soccer, Golf, Rowing, Boxing, and more)
- **GPS Data Extraction**: Reads GPS tracks from Fitbit's `Activities/*.tcx` files and embeds them in exported TCX/GPX/FIT files
- **GPS Download**: Fetch GPS tracks directly from the Fitbit API for activities whose GPS was not included in the Takeout
- **Health Data FIT Export**: Weight, steps, sleep, SpO2, and HRV data exported as FIT files
- **Advanced Heart Rate Zone Analysis**: Age-based zone calculations with Garmin compatibility
- **Batch Processing**: Handles years of historical data efficiently with parallel processing
- **Resume Capability**: Skip already-processed files on interrupted conversions
- **Command Line Interface**: Easy-to-use CLI with progress tracking

## Supported Data Types

### ✅ Activities (FIT / TCX / GPX)
- **Activities**: GPS tracks, exercise sessions with enhanced data
- **Intraday Heart Rate**: Per-second HR embedded in FIT records from Fitbit's intraday data files
- **GPS with elevation and speed**: Extracted from `Activities/` TCX files, including computed speed between trackpoints
- **Distance & elevation**: JSON distance preferred; GPS-derived fallback if JSON is absent

### ✅ Health Data FIT Files (importable)
| Output file | Content | Garmin FIT type |
|---|---|---|
| `weight.fit` | Weight (kg), body fat %, muscle mass, bone mass, hydration | `weight_scale` (message 30) |
| `monitoring.fit` | Daily steps, distance, calories, active minutes | `monitoring` (message 55) |
| `sleep.fit` | Sleep stage segments (deep/light/REM/wake) with durations | `monitoring` (MONITORING_B) |
| `spo2.fit` | Daily SpO2 % (`saturated_hemoglobin_percent`) | `record` in ACTIVITY file |
| `hrv.fit` | Daily RMSSD (ms) stored as HRV time field | `hrv` (message 78) in ACTIVITY |

> **Note**: Garmin Connect's manual upload portal accepts activity FIT files and weight FIT files. Sleep, SpO2, and HRV FIT files are valid FIT format and can be parsed by FIT-compatible tools, but Garmin Connect currently does not surface them in its dashboard from manual upload.

### ✅ CSV Archive (personal data, not importable to Garmin Connect)
- **Daily Metrics**: Steps, calories, distance, floors
- **Body Composition**: Weight, BMI, body fat percentage
- **Sleep Data**: Duration, stages, sleep score, REM/deep/light analysis
- **Heart Rate**: Continuous HR, resting HR, zone analysis
- **HRV**: Daily RMSSD values
- **SpO2**: Blood oxygen readings
- **Active Zone Minutes**: Cardio/peak zone minutes

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

### 2. (Optional) Download GPS Tracks from Fitbit API

Fitbit's Google Takeout does **not** include GPS track data — it only stores a `tcxLink` URL per GPS-enabled activity. To get actual GPS coordinates embedded in your FIT files, you must download them from the Fitbit API first.

#### Get a Fitbit Access Token (one-time setup)

1. Go to [https://dev.fitbit.com/apps/new](https://dev.fitbit.com/apps/new)
   - Application Type: **Personal**
   - Redirect URL: `https://localhost`
2. Note your **Client ID**.
3. Open this URL in a browser (replace `YOUR_CLIENT_ID`):
   ```
   https://www.fitbit.com/oauth2/authorize?response_type=token
     &client_id=YOUR_CLIENT_ID
     &redirect_uri=https%3A%2F%2Flocalhost
     &scope=activity%20location
     &expires_in=604800
   ```
   > ⚠️ Both `activity` **and** `location` scopes are required. Using only `activity` will result in HTTP 403 errors.
4. Approve access. Copy the `access_token` value from the redirect URL.
5. Run:
   ```bash
   fitbit2garmin fetch-gps path/to/Takeout --token <your_token>
   ```

GPS TCX files are saved to `<Fitbit>/Activities/` inside your Takeout directory. Re-run `convert` afterwards to embed the GPS in your FIT files.

### 3. Convert Your Data

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
fitbit2garmin convert path/to/Takeout --format fit
fitbit2garmin convert path/to/Takeout --format csv --format fit
fitbit2garmin convert path/to/Takeout --format csv --format tcx --format gpx --format fit
```

> Only the requested formats are generated. Specifying `--format fit` will **not** also create TCX files.

#### Export Only Activities
```bash
fitbit2garmin convert path/to/Takeout --activities-only
```

#### Export Only Daily Metrics
```bash
fitbit2garmin convert path/to/Takeout --daily-only
```

### 4. Analyze Your Data (Optional)
```bash
fitbit2garmin analyze path/to/Takeout
```

### 5. Debug Activity Types
```bash
fitbit2garmin debug-activities path/to/Takeout
```

Shows TCX sport and FIT sport side-by-side for every activity type detected in your data, flagging any that need FIT format for correct sport display.

### 6. Get Help
```bash
fitbit2garmin --help
fitbit2garmin convert --help
fitbit2garmin fetch-gps --help
fitbit2garmin info
```

## Output Files

### Activity Files (importable into Garmin Connect)
- `{type}_{logid}_{timestamp}.fit` — Native Garmin FIT format (**recommended**)
- `{type}_{logid}_{timestamp}.tcx` — TCX format (sport limited to 5 types)
- `activity_{logid}_{timestamp}.gpx` — GPS activities in GPX format

### Health Data FIT Files
- `weight.fit` — Body composition history (weight, fat %, muscle, bone)
- `monitoring.fit` — Daily steps, distance, calories, active minutes
- `sleep.fit` — Sleep stage history (deep / light / REM / wake segments)
- `spo2.fit` — Daily blood oxygen saturation
- `hrv.fit` — Daily HRV (RMSSD) values

### CSV Files (personal data archive)
These files are useful as a personal archive or for analysis in spreadsheet tools.

- `fitbit_steps.csv` — Daily step counts
- `fitbit_distance.csv` — Daily distance traveled
- `fitbit_calories.csv` — Daily calories burned
- `fitbit_sleep.csv` — Sleep duration and quality
- `fitbit_heart_rate.csv` — Daily heart rate summaries
- `fitbit_heart_rate_zones.csv` — Heart rate zone analysis per activity
- `fitbit_body_composition.csv` — Weight and body metrics
- `fitbit_activities.csv` — Activity summaries and metrics
- `garmin_connect_import.csv` — Combined summary (reference only)

## Importing to Garmin Connect

### Activities
1. Log into [Garmin Connect](https://connect.garmin.com/)
2. Go to **Import Data** (or drag-and-drop on the web app)
3. Upload **FIT files** — recommended; carries the correct sport type for all 30+ activity types
4. If FIT upload fails for a specific activity, try **TCX** or **GPX** instead

### Weight Data
Upload `weight.fit` via Garmin Connect's **Import Data** button. Your weight history, body fat %, muscle mass, and bone mass will populate in the Health Snapshot / Body Composition section.

### Steps / Daily Activity
Upload `monitoring.fit` via **Import Data**. Results may vary — Garmin Connect's upload portal is primarily designed for activity files.

### Sleep, SpO2, HRV
Upload `sleep.fit`, `spo2.fit`, and `hrv.fit` via **Import Data**. These are valid FIT files using standard message types (monitoring, activity record, HRV). Garmin Connect may not surface them in its health dashboard — they are primarily useful for FIT-compatible third-party analysis tools.

### Recommended Format Priority for Activities
1. **FIT files** — correct sport types, intraday HR embedded, GPS data included
2. **TCX files** — fallback; sport limited to Running/Walking/Biking/Swimming/Other
3. **GPX files** — GPS track only, no HR or distance metadata

## GPS Data

### How GPS Works in This Tool
Fitbit's Google Takeout includes `tcxLink` URLs for GPS activities but **not the actual GPS track data**. The GPS pipeline has two stages:

1. **Local matching** (automatic): If `Activities/*.tcx` files exist in your Takeout, they are matched to activities by log ID or start time and embedded automatically.
2. **API download** (manual, one-time): Use `fetch-gps` to download GPS TCX files from Fitbit's API. Requires an OAuth token with both `activity` and `location` scopes.

### Distance and Elevation Fallback
If the JSON activity record has distance, it takes priority. Otherwise, distance is computed from GPS trackpoints. Elevation gain from the JSON (barometric altimeter) takes priority over GPS-derived elevation (which is noisier).

## Advanced Heart Rate Zone Features

### Smart Zone Recalculation
- **Age-based formulas**: Uses Tanaka, Fox, Gellish, and Nes formulas for max HR estimation
- **Karvonen method**: Calculates zones using heart rate reserve when resting HR is available
- **User profile estimation**: Automatically determines fitness level from activity patterns

### Multiple Zone Systems
- **Garmin Standard**: 5-zone system (Active Recovery, Aerobic Base, Aerobic, Lactate Threshold, Neuromuscular)
- **Fitbit Mapping**: Converts Fitbit's 3-zone system to Garmin's 5-zone system

## Data Quality and Limitations

### What Transfers to Garmin Connect via Activity Upload
- ✅ Activity GPS tracks (local TCX files or downloaded via `fetch-gps`)
- ✅ Exercise duration, calories, distance
- ✅ Per-activity heart rate (intraday data embedded in FIT records)
- ✅ Heart rate zones with age-based recalculation
- ✅ 30+ activity types with correct Garmin sport mapping
- ✅ Elevation gain (barometric altimeter from JSON, GPS-derived fallback)

### Known Limitations
- ⚠️ TCX format only supports Running/Walking/Biking/Swimming/Other — import `.fit` files for Tennis, Basketball, Soccer, etc.
- ⚠️ GPS tracks not included in Takeout — use `fetch-gps` to download them
- ⚠️ HRV FIT file stores daily RMSSD, not raw R-R intervals
- ⚠️ Some Fitbit-specific metrics have no Garmin equivalent and are CSV-only

## Troubleshooting

### Common Issues

**"GPS data missing from activities"**
- GPS is not included in Google Takeout exports — it must be downloaded separately
- Run `fitbit2garmin fetch-gps path/to/Takeout --token <token>` then re-run `convert`
- See [Get a Fitbit Access Token](#2-optional-download-gps-tracks-from-fitbit-api) above

**"HTTP 403 when running fetch-gps"**
- Your OAuth token was generated with only the `activity` scope
- GPS download requires **both** `activity` and `location` scopes
- Generate a new token using the URL shown in `fitbit2garmin fetch-gps --help`

**"Activities show as 'Other' in Garmin Connect"**
- TCX format limitation — TCX only supports Running, Walking, Biking, Swimming, and Other
- **Solution**: Import the `.fit` files instead
- Run `fitbit2garmin debug-activities path/to/Takeout` to see which activities need FIT format

**"Distance looks wrong for cycling/walking"**
- Distance comes from the Fitbit JSON activity record; if absent it falls back to GPS-derived distance
- Activities without GPS and without a JSON distance field will show no distance in Garmin Connect

**"Fitbit data not found"**
- Ensure you've extracted the Google Takeout archive
- Check that the path contains a `Fitbit/` directory inside the Takeout folder

**"Import failed in Garmin Connect"**
- Try uploading FIT files first (best compatibility)
- Upload files in smaller batches if bulk upload fails

**"Heart rate zones seem incorrect"**
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
├── converter.py          # TCX, GPX, FIT activity files + health data FIT files
├── exporter.py           # CSV exporters (Pandas-based)
├── gps_fetcher.py        # Fitbit API GPS downloader (fetch-gps command)
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
