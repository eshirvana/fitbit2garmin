"""Central config: Takeout directory layout and default paths.

Folder names are the exact names observed in a real Google Takeout Fitbit export
(August 2026 format). Takeout's zip layout has historically varied, so these are
looked up by name under a discovered Fitbit root rather than assumed at a fixed
relative depth.
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TakeoutLayout:
    fitbit_root: Path

    @property
    def global_export_data(self) -> Path:
        return self.fitbit_root / "Global Export Data"

    @property
    def physical_activity_google_data(self) -> Path:
        return self.fitbit_root / "Physical Activity_GoogleData"

    @property
    def activities(self) -> Path:
        return self.fitbit_root / "Activities"

    @property
    def health_fitness_data_google_data(self) -> Path:
        return self.fitbit_root / "Health Fitness Data_GoogleData"


def discover_fitbit_root(takeout_root: Path) -> Path:
    """Find the 'Fitbit' folder under a Takeout export root, wherever it lives."""
    takeout_root = Path(takeout_root)
    if takeout_root.name == "Fitbit" and takeout_root.is_dir():
        return takeout_root
    direct = takeout_root / "Fitbit"
    if direct.is_dir():
        return direct
    for candidate in takeout_root.rglob("Fitbit"):
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        f"Could not find a 'Fitbit' folder under {takeout_root} -- "
        "pass the Takeout root or the Fitbit folder itself."
    )


DEFAULT_DB_FILENAME = "fitbit2garmin.sqlite3"
