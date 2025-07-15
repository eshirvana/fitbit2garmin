"""
Fitbit to Garmin Data Migration Tool

A comprehensive Python tool for converting Fitbit Google Takeout data
to Garmin Connect compatible formats, maximizing data preservation.
"""

__version__ = "1.1.0"
__author__ = "Fitbit2Garmin Team"
__email__ = "support@fitbit2garmin.com"

from .parser import FitbitParser
from .converter import DataConverter
from .exporter import GarminExporter

__all__ = ["FitbitParser", "DataConverter", "GarminExporter"]
