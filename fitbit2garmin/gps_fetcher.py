"""
Download GPS TCX files from the Fitbit API for activities that have hasGps=True
but whose GPS data was not included in the Google Takeout export.

Fitbit's Google Takeout omits the actual GPS track data — it only provides a
tcxLink URL (e.g. https://www.fitbit.com/activities/exercise/12345?export=tcx).
To get the GPS data you must call the Fitbit Web API with a valid access token:

    GET https://api.fitbit.com/1/user/-/activities/{logId}.tcx
    Authorization: Bearer <access_token>

How to get a Fitbit access token (one-time setup):
  1. Go to https://dev.fitbit.com/apps/new
     - Application Name: anything (e.g. "My Data Export")
     - OAuth 2.0 Application Type: Personal
     - Redirect URL: https://localhost
     - Default Access Type: Read-Only
  2. Note your Client ID.
  3. Open this URL in a browser (replace YOUR_CLIENT_ID):
       https://www.fitbit.com/oauth2/authorize?response_type=token
         &client_id=YOUR_CLIENT_ID
         &redirect_uri=https%3A%2F%2Flocalhost
         &scope=activity%20location&expires_in=604800
     NOTE: Both 'activity' AND 'location' scopes are required.
           Using only 'activity' will result in HTTP 403 errors.
  4. Approve access. The browser will redirect to https://localhost#access_token=...
  5. Copy the access_token value from the URL fragment.
  6. Run:  fitbit2garmin fetch-gps path/to/Takeout --token <your_token>
"""

import json
import logging
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

FITBIT_API_TCX_URL = "https://api.fitbit.com/1/user/-/activities/{log_id}.tcx"


def _find_fitbit_path(takeout_path: Path) -> Optional[Path]:
    """Locate the Fitbit subdirectory inside a Google Takeout extract."""
    candidates = [
        takeout_path / "Takeout" / "Fitbit",
        takeout_path / "Takeout2" / "Fitbit",
        takeout_path / "Fitbit",
        takeout_path,
    ]
    for p in candidates:
        if p.is_dir() and any(p.iterdir()):
            return p
    return None


def collect_gps_activities(fitbit_path: Path) -> List[Dict[str, Any]]:
    """Scan all exercise JSON files and return activities where hasGps=True."""
    import orjson

    global_export = fitbit_path / "Global Export Data"
    if not global_export.exists():
        return []

    gps_acts: List[Dict[str, Any]] = []
    for json_file in sorted(global_export.glob("exercise-*.json")):
        try:
            with open(json_file, "rb") as f:
                data = orjson.loads(f.read())
            if not isinstance(data, list):
                continue
            for act in data:
                if act.get("hasGps") and act.get("logId"):
                    gps_acts.append(
                        {
                            "logId": act["logId"],
                            "activityName": act.get("activityName", ""),
                            "startTime": act.get("startTime", ""),
                            "tcxLink": act.get("tcxLink", ""),
                        }
                    )
        except Exception as e:
            logger.warning(f"Error reading {json_file.name}: {e}")

    return gps_acts


def fetch_gps_files(
    takeout_path: Path,
    token: str,
    output_activities_dir: Optional[Path] = None,
    max_retries: int = 3,
    delay_between_requests: float = 0.5,
) -> Tuple[int, int]:
    """Download GPS TCX files for all GPS-flagged activities.

    Returns (downloaded, failed) counts.
    Saves each TCX file as Activities/{logId}.tcx inside the Fitbit directory.
    """
    import requests
    from tqdm import tqdm

    fitbit_path = _find_fitbit_path(takeout_path)
    if fitbit_path is None:
        raise FileNotFoundError(
            f"Fitbit data directory not found under {takeout_path}"
        )

    # Determine where to save TCX files
    if output_activities_dir is None:
        output_activities_dir = fitbit_path / "Activities"
    output_activities_dir.mkdir(parents=True, exist_ok=True)

    gps_activities = collect_gps_activities(fitbit_path)
    if not gps_activities:
        print("No GPS activities found in exercise JSON files.")
        return 0, 0

    # Skip activities already downloaded
    pending = [
        a for a in gps_activities
        if not (output_activities_dir / f"{a['logId']}.tcx").exists()
    ]
    already_done = len(gps_activities) - len(pending)
    if already_done:
        print(f"  ⏭  {already_done} GPS files already downloaded, skipping.")

    if not pending:
        print("All GPS files already downloaded.")
        return 0, 0

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    downloaded = 0
    failed = 0
    failed_ids: List[int] = []

    with tqdm(total=len(pending), desc="  Downloading GPS TCX files") as pbar:
        for act in pending:
            log_id = act["logId"]
            dest = output_activities_dir / f"{log_id}.tcx"

            url = FITBIT_API_TCX_URL.format(log_id=log_id)
            last_error = None

            for attempt in range(1, max_retries + 1):
                try:
                    resp = session.get(url, timeout=30)

                    if resp.status_code == 200:
                        dest.write_bytes(resp.content)
                        downloaded += 1
                        last_error = None
                        break
                    elif resp.status_code == 401:
                        raise RuntimeError(
                            "Fitbit token is invalid or expired. "
                            "Please generate a new token and re-run."
                        )
                    elif resp.status_code == 403:
                        raise RuntimeError(
                            "Fitbit API returned 403 Forbidden. "
                            "Your token is missing the 'location' scope. "
                            "Re-authorize with both 'activity' AND 'location' scopes:\n"
                            "  scope=activity%20location"
                        )
                    elif resp.status_code == 429:
                        # Rate limit — back off
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        logger.warning(
                            f"Rate limited. Waiting {retry_after}s before retry."
                        )
                        time.sleep(retry_after)
                    elif resp.status_code == 404:
                        # Activity has no GPS track (hasGps flag was wrong)
                        logger.debug(f"Activity {log_id}: no GPS data (404)")
                        last_error = "404 no GPS"
                        break
                    else:
                        last_error = f"HTTP {resp.status_code}"
                        logger.warning(
                            f"Activity {log_id} attempt {attempt}: {last_error}"
                        )
                        time.sleep(2 ** attempt)

                except RuntimeError:
                    raise  # Token errors are fatal
                except Exception as e:
                    last_error = str(e)
                    logger.warning(
                        f"Activity {log_id} attempt {attempt}: {e}"
                    )
                    time.sleep(2 ** attempt)

            if last_error:
                failed += 1
                failed_ids.append(log_id)
                logger.warning(f"Failed to download GPS for activity {log_id}: {last_error}")

            pbar.update(1)
            pbar.set_description(
                f"  Downloading GPS TCX [{downloaded} done, {failed} failed]"
            )
            time.sleep(delay_between_requests)

    if failed_ids:
        logger.warning(
            f"Failed GPS downloads ({len(failed_ids)}): "
            + ", ".join(str(i) for i in failed_ids[:10])
            + (" ..." if len(failed_ids) > 10 else "")
        )

    return downloaded, failed
