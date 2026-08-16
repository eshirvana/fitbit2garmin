-- Adds detail fields to `activity` that are available in the source data but
-- weren't being carried through: avg speed (computed, not from the source's
-- unreliable tracker_avg/peak_speed_mm_per_second fields -- see
-- reconcile/activity_matcher.py for why those are distrusted), cadence,
-- per-zone HR time breakdown, and the recording device name.

ALTER TABLE activity ADD COLUMN avg_speed_ms REAL;
ALTER TABLE activity ADD COLUMN avg_cadence INTEGER;
ALTER TABLE activity ADD COLUMN time_in_hr_zone_json TEXT;
ALTER TABLE activity ADD COLUMN source_device TEXT;
