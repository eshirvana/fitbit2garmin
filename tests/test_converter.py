"""
Unit tests for DataConverter — verify that output FIT files preserve input values.

Each test creates known input data, runs the converter, reads the resulting FIT
file back using fit-tool, and asserts that the values round-trip correctly.

FIT encoding precision notes (used for assertAlmostEqual tolerances):
  - Weight / muscle / bone mass : UINT16, scale=100  → 0.01 kg
  - Body fat / hydration %      : UINT16, scale=100  → 0.01 %
  - GPS lat/lon                 : SINT32, scale=11930464.711 → ~1e-7 degrees
  - Altitude                    : UINT16, scale=5, offset=500 → 0.2 m
  - Distance                    : UINT32, scale=100 → 0.01 m
  - Speed                       : UINT16, scale=1000 → 0.001 m/s
  - SpO2 %                      : UINT16, scale=10  → 0.1 %
  - HRV time (s)                : UINT16, scale=1000 → 0.001 s
  - Steps / HR / calories       : integer → exact
"""

import tempfile
from datetime import datetime, date, timezone as _tz
from pathlib import Path

import pytest

from fitbit2garmin.models import (
    ActivityData,
    ActivityType,
    BodyComposition,
    DailyMetrics,
    HeartRateVariability,
    SpO2Data,
    SleepData,
)
from fitbit2garmin.converter import DataConverter

# ── helpers ────────────────────────────────────────────────────────────────────

def _read_fit(path: str):
    """Return all DataMessage objects from a FIT file, keyed by message class name."""
    from fit_tool.fit_file import FitFile
    from fit_tool.data_message import DataMessage
    fit = FitFile.from_file(path)
    messages = []
    for record in fit.records:
        if isinstance(record.message, DataMessage):
            messages.append(record.message)
    return messages


def _msgs_of_type(messages, cls):
    return [m for m in messages if type(m).__name__ == cls.__name__]


def _utc_ms(year, month, day, hour=0, minute=0, second=0):
    return int(datetime(year, month, day, hour, minute, second, tzinfo=_tz.utc).timestamp() * 1000)


# ── fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def outdir(tmp_path):
    return tmp_path


@pytest.fixture
def converter(outdir):
    return DataConverter(output_dir=outdir)


# ══════════════════════════════════════════════════════════════════════════════
# 1. Weight / Body Composition → weight.fit
# ══════════════════════════════════════════════════════════════════════════════

class TestWeightFit:
    """convert_body_composition_to_fit round-trips all weight fields."""

    def _make_entry(self, weight_kg=75.5, fat_pct=18.3, water_pct=55.0,
                    muscle_kg=60.2, bone_kg=3.1, d=date(2024, 3, 15)):
        return BodyComposition(
            date=d,
            weight=weight_kg,
            body_fat_percentage=fat_pct,
            water_percentage=water_pct,
            muscle_mass=muscle_kg,
            bone_mass=bone_kg,
        )

    def test_weight_kg_value(self, converter):
        """Weight stored in kg is written and read back within 0.01 kg."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        entry = self._make_entry(weight_kg=82.3)
        path = converter.convert_body_composition_to_fit([entry])
        assert path is not None
        msgs = _read_fit(path)
        ws_msgs = _msgs_of_type(msgs, WeightScaleMessage)
        assert len(ws_msgs) == 1
        assert abs(ws_msgs[0].weight - 82.3) < 0.02

    def test_weight_lbs_converted_to_kg(self, converter):
        """Weight > 100 (lbs) is converted to kg before writing."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        lbs = 165.0
        expected_kg = round(lbs * 0.453592, 2)
        entry = self._make_entry(weight_kg=lbs)
        path = converter.convert_body_composition_to_fit([entry])
        assert path is not None
        msgs = _read_fit(path)
        ws = _msgs_of_type(msgs, WeightScaleMessage)[0]
        assert abs(ws.weight - expected_kg) < 0.02

    def test_body_fat_percentage(self, converter):
        """Body fat % round-trips within 0.02 %."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        entry = self._make_entry(fat_pct=22.7)
        path = converter.convert_body_composition_to_fit([entry])
        ws = _msgs_of_type(_read_fit(path), WeightScaleMessage)[0]
        assert abs(ws.percent_fat - 22.7) < 0.02

    def test_hydration_percentage(self, converter):
        """Water / hydration % round-trips within 0.02 %."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        entry = self._make_entry(water_pct=60.5)
        path = converter.convert_body_composition_to_fit([entry])
        ws = _msgs_of_type(_read_fit(path), WeightScaleMessage)[0]
        assert abs(ws.percent_hydration - 60.5) < 0.02

    def test_muscle_and_bone_mass(self, converter):
        """Muscle mass and bone mass round-trip within 0.02 kg."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        entry = self._make_entry(muscle_kg=58.4, bone_kg=2.9)
        path = converter.convert_body_composition_to_fit([entry])
        ws = _msgs_of_type(_read_fit(path), WeightScaleMessage)[0]
        assert abs(ws.muscle_mass - 58.4) < 0.02
        assert abs(ws.bone_mass - 2.9) < 0.02

    def test_multiple_entries_all_written(self, converter):
        """All entries are written; count of WeightScaleMessages matches input."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        entries = [
            self._make_entry(weight_kg=75.0, d=date(2024, 1, 1)),
            self._make_entry(weight_kg=74.8, d=date(2024, 1, 2)),
            self._make_entry(weight_kg=74.5, d=date(2024, 1, 3)),
        ]
        path = converter.convert_body_composition_to_fit(entries)
        ws_msgs = _msgs_of_type(_read_fit(path), WeightScaleMessage)
        assert len(ws_msgs) == 3

    def test_entries_sorted_by_date(self, converter):
        """Entries are written in ascending date order regardless of input order."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        entries = [
            self._make_entry(weight_kg=74.0, d=date(2024, 1, 3)),
            self._make_entry(weight_kg=76.0, d=date(2024, 1, 1)),
            self._make_entry(weight_kg=75.0, d=date(2024, 1, 2)),
        ]
        path = converter.convert_body_composition_to_fit(entries)
        ws_msgs = _msgs_of_type(_read_fit(path), WeightScaleMessage)
        # Read weights back; they should be in date order (76, 75, 74)
        weights = [ws.weight for ws in ws_msgs]
        assert abs(weights[0] - 76.0) < 0.02
        assert abs(weights[1] - 75.0) < 0.02
        assert abs(weights[2] - 74.0) < 0.02

    def test_lbs_muscle_and_bone_also_converted(self, converter):
        """When weight is in lbs, muscle_mass and bone_mass are also converted."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        # 170 lbs body, 120 lbs muscle, 8 lbs bone
        entry = BodyComposition(
            date=date(2024, 6, 1),
            weight=170.0,
            muscle_mass=120.0,
            bone_mass=8.0,
        )
        path = converter.convert_body_composition_to_fit([entry])
        ws = _msgs_of_type(_read_fit(path), WeightScaleMessage)[0]
        assert abs(ws.weight - round(170.0 * 0.453592, 2)) < 0.02
        assert abs(ws.muscle_mass - round(120.0 * 0.453592, 2)) < 0.02
        assert abs(ws.bone_mass - round(8.0 * 0.453592, 2)) < 0.02

    def test_empty_list_returns_none(self, converter):
        assert converter.convert_body_composition_to_fit([]) is None

    def test_all_none_weight_returns_none(self, converter):
        entry = BodyComposition(date=date(2024, 1, 1), weight=None)
        assert converter.convert_body_composition_to_fit([entry]) is None


# ══════════════════════════════════════════════════════════════════════════════
# 2. Daily Metrics → monitoring.fit
# ══════════════════════════════════════════════════════════════════════════════

class TestMonitoringFit:
    """convert_daily_steps_to_fit round-trips steps, distance, calories, activity minutes."""

    def _make_entry(self, steps=8500, distance_km=6.2, calories=420,
                    lightly=30, fairly=20, very=15, d=date(2024, 3, 10)):
        return DailyMetrics(
            date=d,
            steps=steps,
            distance=distance_km,
            calories_burned=calories,
            active_minutes=lightly + fairly + very,
            lightly_active_minutes=lightly,
            fairly_active_minutes=fairly,
            very_active_minutes=very,
        )

    def test_steps_exact(self, converter):
        """Steps are stored as integers and must match exactly."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        entry = self._make_entry(steps=12_345)
        path = converter.convert_daily_steps_to_fit([entry])
        assert path is not None
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert len(msgs) >= 1
        assert msgs[0].steps == 12_345

    def test_distance_km_to_meters(self, converter):
        """Distance is converted from km to metres and round-trips within 1 m."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        entry = self._make_entry(distance_km=5.0)
        path = converter.convert_daily_steps_to_fit([entry])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        # Input: 5.0 km → 5000.0 m expected
        assert abs(msgs[0].distance - 5000.0) < 1.0

    def test_calories_exact(self, converter):
        """Calorie count is stored as integer and must match exactly."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        entry = self._make_entry(calories=735)
        path = converter.convert_daily_steps_to_fit([entry])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert msgs[0].calories == 735

    def test_moderate_activity_minutes(self, converter):
        """Moderate activity = lightly_active + fairly_active minutes."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        entry = self._make_entry(lightly=25, fairly=15, very=10)
        path = converter.convert_daily_steps_to_fit([entry])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert msgs[0].moderate_activity_minutes == 40   # 25 + 15

    def test_vigorous_activity_minutes(self, converter):
        """Vigorous activity = very_active_minutes."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        entry = self._make_entry(lightly=20, fairly=10, very=30)
        path = converter.convert_daily_steps_to_fit([entry])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert msgs[0].vigorous_activity_minutes == 30

    def test_multiple_days_written(self, converter):
        """All days are written; record count matches input count."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        entries = [
            self._make_entry(steps=7000, d=date(2024, 1, 1)),
            self._make_entry(steps=9000, d=date(2024, 1, 2)),
            self._make_entry(steps=11000, d=date(2024, 1, 3)),
        ]
        path = converter.convert_daily_steps_to_fit(entries)
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert len(msgs) == 3

    def test_zero_steps_skipped(self, converter):
        """Entries with 0 or None steps are skipped."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        entries = [
            DailyMetrics(date=date(2024, 1, 1), steps=0),
            DailyMetrics(date=date(2024, 1, 2), steps=None),
            self._make_entry(steps=5000, d=date(2024, 1, 3)),
        ]
        path = converter.convert_daily_steps_to_fit(entries)
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert len(msgs) == 1
        assert msgs[0].steps == 5000

    def test_empty_returns_none(self, converter):
        assert converter.convert_daily_steps_to_fit([]) is None


# ══════════════════════════════════════════════════════════════════════════════
# 3. HRV → hrv.fit
# ══════════════════════════════════════════════════════════════════════════════

class TestHrvFit:
    """convert_hrv_to_fit: RMSSD (ms) → stored as seconds, round-trips within 1 ms."""

    def _make_entry(self, rmssd_ms=42.5, d=date(2024, 4, 10), ts=None):
        return HeartRateVariability(date=d, rmssd=rmssd_ms, timestamp=ts)

    def test_rmssd_stored_as_seconds(self, converter):
        """RMSSD written as rmssd/1000 seconds; reading back and multiplying by 1000 gives original ms."""
        from fit_tool.profile.messages.hrv_message import HrvMessage
        rmssd_ms = 55.0
        entry = self._make_entry(rmssd_ms=rmssd_ms)
        paths = converter.convert_hrv_to_fit([entry])
        assert paths
        msgs = _msgs_of_type(_read_fit(paths[0]), HrvMessage)
        assert len(msgs) == 1
        # time is stored as a list; first element is rmssd in seconds
        stored_s = msgs[0].time[0]
        assert abs(stored_s * 1000.0 - rmssd_ms) < 1.0   # within 1 ms

    def test_rmssd_values_distinct(self, converter):
        """Multiple entries write distinct RMSSD values in the correct order."""
        from fit_tool.profile.messages.hrv_message import HrvMessage
        entries = [
            self._make_entry(rmssd_ms=30.0, d=date(2024, 1, 1)),
            self._make_entry(rmssd_ms=45.0, d=date(2024, 1, 2)),
            self._make_entry(rmssd_ms=60.0, d=date(2024, 1, 3)),
        ]
        paths = converter.convert_hrv_to_fit(entries)
        msgs = _msgs_of_type(_read_fit(paths[0]), HrvMessage)
        assert len(msgs) == 3
        stored = [m.time[0] * 1000.0 for m in msgs]
        assert abs(stored[0] - 30.0) < 1.0
        assert abs(stored[1] - 45.0) < 1.0
        assert abs(stored[2] - 60.0) < 1.0

    def test_explicit_timestamp_used(self, converter):
        """When an explicit timestamp is provided it is written to the Event messages."""
        from fit_tool.profile.messages.event_message import EventMessage
        ts = datetime(2024, 5, 1, 8, 30, 0, tzinfo=_tz.utc)
        entry = self._make_entry(rmssd_ms=40.0, ts=ts)
        paths = converter.convert_hrv_to_fit([entry])
        msgs = _read_fit(paths[0])
        events = _msgs_of_type(msgs, EventMessage)
        expected_ms = int(ts.timestamp() * 1000)
        # Timer-start event should have the expected timestamp (within 1 second)
        start_ts = events[0].timestamp
        assert abs(start_ts - expected_ms) < 1000

    def test_zero_rmssd_skipped(self, converter):
        """Entries with rmssd=0 or None are excluded."""
        from fit_tool.profile.messages.hrv_message import HrvMessage
        entries = [
            HeartRateVariability(date=date(2024, 1, 1), rmssd=0.0),
            HeartRateVariability(date=date(2024, 1, 2), rmssd=None),
            self._make_entry(rmssd_ms=38.0, d=date(2024, 1, 3)),
        ]
        paths = converter.convert_hrv_to_fit(entries)
        assert paths
        msgs = _msgs_of_type(_read_fit(paths[0]), HrvMessage)
        assert len(msgs) == 1
        assert abs(msgs[0].time[0] * 1000.0 - 38.0) < 1.0

    def test_all_invalid_returns_empty(self, converter):
        entries = [
            HeartRateVariability(date=date(2024, 1, 1), rmssd=None),
        ]
        assert not converter.convert_hrv_to_fit(entries)

    def test_empty_returns_empty(self, converter):
        assert not converter.convert_hrv_to_fit([])


# ══════════════════════════════════════════════════════════════════════════════
# 4. SpO2 → spo2.fit
# ══════════════════════════════════════════════════════════════════════════════

class TestSpO2Fit:
    """convert_spo2_to_fit: saturation % round-trips within 0.2 %."""

    def _make_entry(self, pct=97.5, d=date(2024, 5, 20), ts=None):
        return SpO2Data(date=d, spo2_percentage=pct, timestamp=ts)

    def test_spo2_value(self, converter):
        """SpO2 % is written and read back within 0.2 % (scale=10 encoding)."""
        from fit_tool.profile.messages.record_message import RecordMessage
        entry = self._make_entry(pct=96.0)
        paths = converter.convert_spo2_to_fit([entry])
        assert paths
        msgs = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        assert len(msgs) >= 1
        assert abs(msgs[0].saturated_hemoglobin_percent - 96.0) < 0.2

    def test_multiple_days_written(self, converter):
        """One RecordMessage per valid SpO2 entry."""
        from fit_tool.profile.messages.record_message import RecordMessage
        entries = [
            self._make_entry(pct=96.5, d=date(2024, 1, 1)),
            self._make_entry(pct=97.0, d=date(2024, 1, 2)),
            self._make_entry(pct=98.0, d=date(2024, 1, 3)),
        ]
        paths = converter.convert_spo2_to_fit(entries)
        msgs = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        assert len(msgs) == 3

    def test_spo2_values_match_input_order(self, converter):
        """SpO2 values in the FIT file match input values in sorted-date order."""
        from fit_tool.profile.messages.record_message import RecordMessage
        entries = [
            self._make_entry(pct=98.0, d=date(2024, 1, 3)),
            self._make_entry(pct=96.0, d=date(2024, 1, 1)),
            self._make_entry(pct=97.0, d=date(2024, 1, 2)),
        ]
        paths = converter.convert_spo2_to_fit(entries)
        msgs = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        values = [m.saturated_hemoglobin_percent for m in msgs]
        # Should be sorted by date: 96, 97, 98
        assert abs(values[0] - 96.0) < 0.2
        assert abs(values[1] - 97.0) < 0.2
        assert abs(values[2] - 98.0) < 0.2

    def test_none_spo2_skipped(self, converter):
        """Entries with spo2_percentage=None are skipped."""
        from fit_tool.profile.messages.record_message import RecordMessage
        entries = [
            SpO2Data(date=date(2024, 1, 1), spo2_percentage=None),
            self._make_entry(pct=95.5, d=date(2024, 1, 2)),
        ]
        paths = converter.convert_spo2_to_fit(entries)
        msgs = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        assert len(msgs) == 1
        assert abs(msgs[0].saturated_hemoglobin_percent - 95.5) < 0.2

    def test_empty_returns_empty(self, converter):
        assert not converter.convert_spo2_to_fit([])


# ══════════════════════════════════════════════════════════════════════════════
# 5. Sleep → sleep.fit
# ══════════════════════════════════════════════════════════════════════════════

class TestSleepFit:
    """convert_sleep_to_fit: stage activity levels and durations are preserved."""

    def _make_sleep(
        self,
        log_id=1001,
        d=date(2024, 2, 14),
        stages=None,
    ):
        start = datetime(2024, 2, 14, 22, 0, 0, tzinfo=_tz.utc)
        end   = datetime(2024, 2, 15,  6, 0, 0, tzinfo=_tz.utc)
        if stages is None:
            stages = [
                {"dateTime": "2024-02-14T22:00:00", "level": "light",  "seconds": 1800},
                {"dateTime": "2024-02-14T22:30:00", "level": "deep",   "seconds": 3600},
                {"dateTime": "2024-02-14T23:30:00", "level": "rem",    "seconds": 5400},
                {"dateTime": "2024-02-15T01:00:00", "level": "wake",   "seconds": 600},
                {"dateTime": "2024-02-15T01:10:00", "level": "light",  "seconds": 7200},
            ]
        return SleepData(
            log_id=log_id,
            date_of_sleep=d,
            start_time=start,
            end_time=end,
            duration_ms=8 * 3600 * 1000,
            sleep_stages=stages,
            type="stages",
        )

    def test_correct_number_of_monitoring_messages(self, converter):
        """One MonitoringMessage is written per sleep stage segment."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        sleep = self._make_sleep()
        path = converter.convert_sleep_to_fit([sleep])
        assert path is not None
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert len(msgs) == 5   # five stage segments defined above

    def test_stage_durations_preserved(self, converter):
        """The active_time (seconds) of each stage matches the input."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        sleep = self._make_sleep()
        path = converter.convert_sleep_to_fit([sleep])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        durations = [m.active_time for m in msgs]
        expected = [1800, 3600, 5400, 600, 7200]
        for actual, exp in zip(durations, expected):
            assert abs(actual - exp) < 2   # within 2 seconds (encoding rounding)

    def test_deep_stage_maps_to_low_activity(self, converter):
        """Deep sleep maps to ActivityLevel.LOW (value 0)."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        from fit_tool.profile.profile_type import ActivityLevel
        sleep = self._make_sleep(stages=[
            {"dateTime": "2024-02-14T22:00:00", "level": "deep", "seconds": 3600}
        ])
        path = converter.convert_sleep_to_fit([sleep])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        # activity_level reads back as int; compare against .value
        assert msgs[0].activity_level == ActivityLevel.LOW.value

    def test_wake_stage_maps_to_high_activity(self, converter):
        """Wake stage maps to ActivityLevel.HIGH (value 2)."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        from fit_tool.profile.profile_type import ActivityLevel
        sleep = self._make_sleep(stages=[
            {"dateTime": "2024-02-14T22:00:00", "level": "wake", "seconds": 600}
        ])
        path = converter.convert_sleep_to_fit([sleep])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert msgs[0].activity_level == ActivityLevel.HIGH.value

    def test_light_and_rem_map_to_medium_activity(self, converter):
        """Light and REM sleep map to ActivityLevel.MEDIUM (value 1)."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        from fit_tool.profile.profile_type import ActivityLevel
        sleep = self._make_sleep(stages=[
            {"dateTime": "2024-02-14T22:00:00", "level": "light", "seconds": 1800},
            {"dateTime": "2024-02-14T22:30:00", "level": "rem",   "seconds": 1800},
        ])
        path = converter.convert_sleep_to_fit([sleep])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        for msg in msgs:
            assert msg.activity_level == ActivityLevel.MEDIUM.value

    def test_multiple_sleep_records(self, converter):
        """Multiple nights produce one MonitoringMessage per stage across all nights."""
        from fit_tool.profile.messages.monitoring_message import MonitoringMessage
        night1 = self._make_sleep(log_id=1, d=date(2024, 1, 10))
        night2 = self._make_sleep(log_id=2, d=date(2024, 1, 11))
        path = converter.convert_sleep_to_fit([night1, night2])
        msgs = _msgs_of_type(_read_fit(path), MonitoringMessage)
        assert len(msgs) == 10   # 5 stages × 2 nights

    def test_empty_returns_none(self, converter):
        assert converter.convert_sleep_to_fit([]) is None


# ══════════════════════════════════════════════════════════════════════════════
# 6. Activity (non-GPS) → .fit
# ══════════════════════════════════════════════════════════════════════════════

class TestActivityFitNoGps:
    """_generate_fit_file for a non-GPS activity preserves session-level fields."""

    def _make_activity(self):
        return ActivityData(
            log_id=99001,
            activity_name="Morning Run",
            activity_type=ActivityType.RUN,
            start_time=datetime(2024, 6, 1, 7, 0, 0, tzinfo=_tz.utc),
            duration_ms=30 * 60 * 1000,   # 30 min
            distance=5.0,                  # 5 km
            calories=320,
            steps=4200,
            average_heart_rate=145,
            max_heart_rate=172,
            elevation_gain=85.0,
        )

    def test_fit_file_created(self, converter):
        """A FIT file is produced for a valid non-GPS activity."""
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        assert len(paths) == 1
        assert Path(paths[0]).exists()

    def test_filename_contains_log_id_and_timestamp(self, converter):
        """Filename follows pattern: {type}_{log_id}_{YYYYMMDD_HHMMSS}.fit"""
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        name = Path(paths[0]).name
        assert "99001" in name
        assert name.endswith(".fit")
        assert "running" in name.lower()

    def test_session_distance(self, converter):
        """Session total_distance matches activity.distance (km → m)."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        msgs = _msgs_of_type(_read_fit(paths[0]), SessionMessage)
        assert len(msgs) == 1
        # 5 km → 5000 m; allow 1 m tolerance for FIT encoding
        assert abs(msgs[0].total_distance - 5000.0) < 1.0

    def test_session_calories(self, converter):
        """Session total_calories matches input exactly."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        msgs = _msgs_of_type(_read_fit(paths[0]), SessionMessage)
        assert msgs[0].total_calories == 320

    def test_session_avg_heart_rate(self, converter):
        """Session avg_heart_rate matches input."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        msgs = _msgs_of_type(_read_fit(paths[0]), SessionMessage)
        assert msgs[0].avg_heart_rate == 145

    def test_session_max_heart_rate(self, converter):
        """Session max_heart_rate matches input."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        msgs = _msgs_of_type(_read_fit(paths[0]), SessionMessage)
        assert msgs[0].max_heart_rate == 172

    def test_session_duration(self, converter):
        """Session total_elapsed_time matches duration_ms / 1000 within 1 s."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        msgs = _msgs_of_type(_read_fit(paths[0]), SessionMessage)
        assert abs(msgs[0].total_elapsed_time - 1800.0) < 1.0

    def test_session_elevation(self, converter):
        """Session total_ascent matches elevation_gain within 1 m."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        msgs = _msgs_of_type(_read_fit(paths[0]), SessionMessage)
        assert abs(msgs[0].total_ascent - 85.0) < 1.0

    def test_sport_type_running(self, converter):
        """Running activity maps to FIT Sport.RUNNING."""
        from fit_tool.profile.messages.session_message import SessionMessage
        from fit_tool.profile.profile_type import Sport
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        msgs = _msgs_of_type(_read_fit(paths[0]), SessionMessage)
        # sport reads back as int from FIT file; compare with .value
        assert msgs[0].sport == Sport.RUNNING.value

    def test_lap_matches_session(self, converter):
        """Lap message mirrors the session's key fields."""
        from fit_tool.profile.messages.lap_message import LapMessage
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        msgs = _read_fit(paths[0])
        lap = _msgs_of_type(msgs, LapMessage)[0]
        session = _msgs_of_type(msgs, SessionMessage)[0]
        assert abs(lap.total_distance - session.total_distance) < 1.0
        assert lap.total_calories == session.total_calories
        assert lap.avg_heart_rate == session.avg_heart_rate

    def test_record_messages_present(self, converter):
        """At least one RecordMessage is written (time-based HR records)."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        assert len(records) >= 1

    def test_multiple_activities_produce_multiple_files(self, converter):
        """N activities → N FIT files."""
        activities = [
            ActivityData(
                log_id=i,
                activity_name="Walk",
                activity_type=ActivityType.WALK,
                start_time=datetime(2024, 1, i + 1, 8, 0, 0, tzinfo=_tz.utc),
                duration_ms=20 * 60 * 1000,
                calories=150,
            )
            for i in range(1, 4)
        ]
        paths = converter.convert_activities_to_fit(activities)
        assert len(paths) == 3
        assert all(Path(p).exists() for p in paths)


# ══════════════════════════════════════════════════════════════════════════════
# 7. Activity (GPS) → .fit
# ══════════════════════════════════════════════════════════════════════════════

class TestActivityFitWithGps:
    """_generate_fit_file embeds GPS trackpoints with correct coordinates."""

    # Three GPS points along a short route
    GPS_POINTS = [
        {"latitude": 37.7749, "longitude": -122.4194, "altitude": 10.0,
         "distance": 0.0,   "speed": 0.0,  "heart_rate": 130,
         "time": "2024-06-01T07:00:00.000Z"},
        {"latitude": 37.7760, "longitude": -122.4180, "altitude": 12.5,
         "distance": 150.0, "speed": 3.5,  "heart_rate": 145,
         "time": "2024-06-01T07:00:43.000Z"},
        {"latitude": 37.7775, "longitude": -122.4165, "altitude": 15.0,
         "distance": 350.0, "speed": 4.2,  "heart_rate": 155,
         "time": "2024-06-01T07:01:32.000Z"},
    ]

    def _make_gps_activity(self, gps_points=None):
        return ActivityData(
            log_id=88001,
            activity_name="GPS Run",
            activity_type=ActivityType.RUN,
            start_time=datetime(2024, 6, 1, 7, 0, 0, tzinfo=_tz.utc),
            duration_ms=92 * 1000,   # ~92 seconds
            distance=0.35,           # 0.35 km
            calories=18,
            average_heart_rate=143,
            max_heart_rate=155,
            gps_data=gps_points or self.GPS_POINTS,
        )

    def test_gps_file_created(self, converter):
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        assert len(paths) == 1
        assert Path(paths[0]).exists()

    def test_record_count_matches_gps_points(self, converter):
        """One RecordMessage per GPS point."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        assert len(records) == len(self.GPS_POINTS)

    def test_latitude_values(self, converter):
        """Latitude round-trips within 1e-5 degrees (~1 m accuracy)."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        for rec, pt in zip(records, self.GPS_POINTS):
            assert abs(rec.position_lat - pt["latitude"]) < 1e-5, (
                f"lat mismatch: {rec.position_lat} vs {pt['latitude']}"
            )

    def test_longitude_values(self, converter):
        """Longitude round-trips within 1e-5 degrees."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        for rec, pt in zip(records, self.GPS_POINTS):
            assert abs(rec.position_long - pt["longitude"]) < 1e-5, (
                f"lon mismatch: {rec.position_long} vs {pt['longitude']}"
            )

    def test_altitude_values(self, converter):
        """Altitude round-trips within 0.3 m (FIT UINT16 scale=5 → 0.2 m steps)."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        for rec, pt in zip(records, self.GPS_POINTS):
            assert abs(rec.altitude - pt["altitude"]) < 0.3, (
                f"altitude mismatch: {rec.altitude} vs {pt['altitude']}"
            )

    def test_cumulative_distance_values(self, converter):
        """Cumulative distance round-trips within 0.1 m."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        for rec, pt in zip(records, self.GPS_POINTS):
            assert abs(rec.distance - pt["distance"]) < 0.1, (
                f"distance mismatch: {rec.distance} vs {pt['distance']}"
            )

    def test_speed_values(self, converter):
        """Speed values round-trip within 0.005 m/s (FIT scale=1000)."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        for rec, pt in zip(records, self.GPS_POINTS):
            if pt["speed"] == 0.0:
                continue   # first point often has speed=0, may be omitted
            assert abs(rec.speed - pt["speed"]) < 0.005, (
                f"speed mismatch: {rec.speed} vs {pt['speed']}"
            )

    def test_heart_rate_embedded_in_gps_records(self, converter):
        """GPS-embedded heart rate values are preserved exactly."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        for rec, pt in zip(records, self.GPS_POINTS):
            assert rec.heart_rate == pt["heart_rate"], (
                f"HR mismatch: {rec.heart_rate} vs {pt['heart_rate']}"
            )

    def test_gps_coordinates_not_null(self, converter):
        """No GPS record has a null position_lat or position_long."""
        from fit_tool.profile.messages.record_message import RecordMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        for rec in records:
            assert rec.position_lat is not None
            assert rec.position_long is not None

    def test_session_has_gps_bounding_box(self, converter):
        """Session contains a non-None bounding box (nec_lat/swc_lat) from GPS stats."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        session = _msgs_of_type(_read_fit(paths[0]), SessionMessage)[0]
        assert session.nec_lat is not None
        assert session.swc_lat is not None
        assert session.nec_long is not None
        assert session.swc_long is not None

    def test_session_start_position(self, converter):
        """Session start position matches the first GPS point within 1e-5 degrees."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = self._make_gps_activity()
        paths = converter.convert_activities_to_fit([activity])
        session = _msgs_of_type(_read_fit(paths[0]), SessionMessage)[0]
        assert abs(session.start_position_lat - self.GPS_POINTS[0]["latitude"]) < 1e-5
        assert abs(session.start_position_long - self.GPS_POINTS[0]["longitude"]) < 1e-5

    def test_bad_altitude_clamped_not_dropped(self, converter):
        """A point with altitude below -499 m is clamped, not dropped."""
        from fit_tool.profile.messages.record_message import RecordMessage
        pts = list(self.GPS_POINTS)
        pts[1] = dict(pts[1], altitude=-600.0)   # below FIT valid range
        activity = self._make_gps_activity(gps_points=pts)
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        # All 3 records should be present (none dropped)
        assert len(records) == 3
        # The clamped altitude should be -499 or higher
        assert records[1].altitude >= -499.0

    def test_high_speed_capped_not_dropped(self, converter):
        """A point with speed > 65 m/s is capped at 65 m/s (not dropped)."""
        from fit_tool.profile.messages.record_message import RecordMessage
        pts = list(self.GPS_POINTS)
        pts[2] = dict(pts[2], speed=200.0)  # 720 km/h — GPS glitch
        activity = self._make_gps_activity(gps_points=pts)
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        assert len(records) == 3
        assert records[2].speed <= 65.1  # capped

    def test_sport_mapping_cycling(self, converter):
        """Biking activity maps to FIT Sport.CYCLING."""
        from fit_tool.profile.messages.session_message import SessionMessage
        from fit_tool.profile.profile_type import Sport
        activity = ActivityData(
            log_id=77001,
            activity_name="Bike Ride",
            activity_type=ActivityType.BIKE,
            start_time=datetime(2024, 7, 1, 9, 0, 0, tzinfo=_tz.utc),
            duration_ms=3600 * 1000,
            distance=25.0,
            gps_data=self.GPS_POINTS,
        )
        paths = converter.convert_activities_to_fit([activity])
        session = _msgs_of_type(_read_fit(paths[0]), SessionMessage)[0]
        assert session.sport == Sport.CYCLING.value

    def test_sport_mapping_hiking(self, converter):
        """Hiking activity maps to FIT Sport.HIKING."""
        from fit_tool.profile.messages.session_message import SessionMessage
        from fit_tool.profile.profile_type import Sport
        activity = ActivityData(
            log_id=77002,
            activity_name="Hike",
            activity_type=ActivityType.HIKE,
            start_time=datetime(2024, 8, 1, 8, 0, 0, tzinfo=_tz.utc),
            duration_ms=7200 * 1000,
            gps_data=self.GPS_POINTS,
        )
        paths = converter.convert_activities_to_fit([activity])
        session = _msgs_of_type(_read_fit(paths[0]), SessionMessage)[0]
        assert session.sport == Sport.HIKING.value


# ══════════════════════════════════════════════════════════════════════════════
# 8. Edge cases / cross-cutting
# ══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:

    def test_activity_without_optional_fields(self, converter):
        """Activity with only required fields (no HR, distance, GPS) generates a valid FIT."""
        from fit_tool.profile.messages.session_message import SessionMessage
        activity = ActivityData(
            log_id=55001,
            activity_name="Unknown",
            activity_type=ActivityType.SPORT,
            start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=_tz.utc),
            duration_ms=10 * 60 * 1000,
        )
        paths = converter.convert_activities_to_fit([activity])
        assert len(paths) == 1
        sessions = _msgs_of_type(_read_fit(paths[0]), SessionMessage)
        assert len(sessions) == 1

    def test_gps_activity_with_no_altitude(self, converter):
        """GPS points without altitude field are handled gracefully."""
        from fit_tool.profile.messages.record_message import RecordMessage
        pts = [
            {"latitude": 51.5074, "longitude": -0.1278, "distance": 0.0,
             "heart_rate": 120, "time": "2024-03-01T10:00:00.000Z"},
            {"latitude": 51.5080, "longitude": -0.1270, "distance": 80.0,
             "heart_rate": 125, "time": "2024-03-01T10:00:20.000Z"},
        ]
        activity = ActivityData(
            log_id=66001,
            activity_name="Walk",
            activity_type=ActivityType.WALK,
            start_time=datetime(2024, 3, 1, 10, 0, 0, tzinfo=_tz.utc),
            duration_ms=60 * 1000,
            gps_data=pts,
        )
        paths = converter.convert_activities_to_fit([activity])
        records = _msgs_of_type(_read_fit(paths[0]), RecordMessage)
        assert len(records) == 2
        # lat/lon still present
        assert records[0].position_lat is not None
        assert records[1].position_long is not None

    def test_single_hrv_entry(self, converter):
        """Single HRV entry produces exactly one HrvMessage."""
        from fit_tool.profile.messages.hrv_message import HrvMessage
        entry = HeartRateVariability(date=date(2024, 9, 15), rmssd=48.3)
        paths = converter.convert_hrv_to_fit([entry])
        msgs = _msgs_of_type(_read_fit(paths[0]), HrvMessage)
        assert len(msgs) == 1
        assert abs(msgs[0].time[0] * 1000.0 - 48.3) < 1.0

    def test_weight_timestamp_is_noon_utc(self, converter):
        """Weight timestamps are set to noon UTC of the measurement date."""
        from fit_tool.profile.messages.weight_scale_message import WeightScaleMessage
        target_date = date(2024, 7, 4)
        entry = BodyComposition(date=target_date, weight=70.0)
        path = converter.convert_body_composition_to_fit([entry])
        msgs = _msgs_of_type(_read_fit(path), WeightScaleMessage)
        expected_noon_ms = _utc_ms(2024, 7, 4, 12, 0, 0)
        # timestamp precision within 1 second
        assert abs(msgs[0].timestamp - expected_noon_ms) < 1000
