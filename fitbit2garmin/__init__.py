"""
fitbit2garmin: migrate a Fitbit Google Takeout export to Garmin Connect.

Architecture: ingest (raw Takeout files -> SQLite staging DB) -> reconcile
(-> canonical `activity` table) -> output (-> Garmin-importable FIT/TCX/GPX/CSV
files). See PROGRESS.md and CLAUDE.md for full details.
"""

__version__ = "2.0.0"
