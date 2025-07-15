# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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