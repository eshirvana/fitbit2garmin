"""
Utility functions for the Fitbit to Garmin migration tool.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import pickle
import hashlib
from datetime import datetime

logger = logging.getLogger(__name__)


class ParallelProcessor:
    """Handle parallel processing of files with progress tracking."""

    def __init__(self, max_workers: Optional[int] = None):
        """Initialize parallel processor."""
        self.max_workers = max_workers or min(
            cpu_count(), 8
        )  # Limit to 8 to avoid overwhelming
        logger.info(f"Initialized parallel processor with {self.max_workers} workers")

    def process_in_chunks(
        self,
        items: List[Any],
        process_func: Callable,
        chunk_size: int = 100,
        description: str = "Processing items",
    ) -> List[Any]:
        """Process items in parallel in chunks to manage memory."""
        if not items:
            return []

        all_results = []
        num_chunks = (len(items) + chunk_size - 1) // chunk_size

        from tqdm import tqdm

        for i in tqdm(range(num_chunks), desc=f"Processing in chunks", leave=False):
            chunk = items[i * chunk_size : (i + 1) * chunk_size]
            chunk_description = f"{description} (chunk {i+1}/{num_chunks})"

            # Use the existing parallel processing logic for the chunk
            chunk_results = self.process_files_parallel(
                chunk, process_func, chunk_description
            )
            all_results.extend(chunk_results)

        return all_results

    def process_files_parallel(
        self,
        files: List[Path],
        process_func: Callable,
        description: str = "Processing files",
    ) -> List[Any]:
        """Process files in parallel with progress tracking."""
        if not files:
            return []

        results = []

        # For small number of files, use sequential processing
        if len(files) <= 2:
            from tqdm import tqdm

            for file_path in tqdm(files, desc=description, leave=False):
                try:
                    result = process_func(file_path)
                    if result:
                        results.extend(result if isinstance(result, list) else [result])
                except Exception as e:
                    logger.warning(f"Error processing {file_path}: {e}")
            return results

        # Use parallel processing for larger file sets
        try:
            from tqdm import tqdm

            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_file = {
                    executor.submit(process_func, file_path): file_path
                    for file_path in files
                }

                # Process completed tasks with progress bar
                for future in tqdm(
                    as_completed(future_to_file),
                    total=len(files),
                    desc=description,
                    leave=False,
                ):
                    file_path = future_to_file[future]
                    try:
                        result = future.result()
                        if result:
                            # The worker function now returns a list of parsed items
                            results.extend(result)
                    except Exception as e:
                        logger.warning(f"Error processing {file_path}: {e}")

        except Exception as e:
            logger.warning(
                f"Parallel processing failed, falling back to sequential: {e}"
            )
            # Fallback to sequential processing
            from tqdm import tqdm

            for file_path in tqdm(
                files, desc=f"{description} (sequential)", leave=False
            ):
                try:
                    result = process_func(file_path)
                    if result:
                        results.extend(result if isinstance(result, list) else [result])
                except Exception as e:
                    logger.warning(f"Error processing {file_path}: {e}")

        return results


class ResumeManager:
    """Manage resume capability for interrupted conversions."""

    def __init__(self, output_dir: Path):
        """Initialize resume manager."""
        self.output_dir = Path(output_dir)
        self.cache_dir = self.output_dir / ".fitbit2garmin_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.processed_files_cache = self.cache_dir / "processed_files.json"
        self.conversion_state_cache = self.cache_dir / "conversion_state.pkl"

    def get_file_hash(self, file_path: Path) -> str:
        """Get a hash of the file for change detection."""
        try:
            stat = file_path.stat()
            # Use file size and modification time for hash
            content = f"{file_path.name}_{stat.st_size}_{stat.st_mtime}"
            return hashlib.md5(content.encode()).hexdigest()
        except:
            return ""

    def is_file_processed(self, file_path: Path) -> bool:
        """Check if a file has already been processed."""
        if not self.processed_files_cache.exists():
            return False

        try:
            with open(self.processed_files_cache, "r") as f:
                processed_files = json.load(f)

            file_key = str(file_path)
            current_hash = self.get_file_hash(file_path)

            return (
                file_key in processed_files
                and processed_files[file_key] == current_hash
            )
        except:
            return False

    def mark_file_processed(self, file_path: Path):
        """Mark a file as processed."""
        try:
            processed_files = {}
            if self.processed_files_cache.exists():
                with open(self.processed_files_cache, "r") as f:
                    processed_files = json.load(f)

            file_key = str(file_path)
            file_hash = self.get_file_hash(file_path)
            processed_files[file_key] = file_hash

            with open(self.processed_files_cache, "w") as f:
                json.dump(processed_files, f)
        except Exception as e:
            logger.warning(f"Could not mark file as processed: {e}")

    def save_conversion_state(self, state: Dict[str, Any]):
        """Save the current conversion state."""
        try:
            state["timestamp"] = datetime.now().isoformat()
            with open(self.conversion_state_cache, "wb") as f:
                pickle.dump(state, f)
        except Exception as e:
            logger.warning(f"Could not save conversion state: {e}")

    def load_conversion_state(self) -> Optional[Dict[str, Any]]:
        """Load the previous conversion state."""
        try:
            if self.conversion_state_cache.exists():
                with open(self.conversion_state_cache, "rb") as f:
                    return pickle.load(f)
        except Exception as e:
            logger.warning(f"Could not load conversion state: {e}")
        return None

    def clear_cache(self):
        """Clear all cached data."""
        try:
            if self.processed_files_cache.exists():
                self.processed_files_cache.unlink()
            if self.conversion_state_cache.exists():
                self.conversion_state_cache.unlink()
            logger.info("Cleared conversion cache")
        except Exception as e:
            logger.warning(f"Could not clear cache: {e}")

    def filter_unprocessed_files(self, files: List[Path]) -> List[Path]:
        """Filter out files that have already been processed."""
        unprocessed = []
        processed_count = 0

        for file_path in files:
            if self.is_file_processed(file_path):
                processed_count += 1
            else:
                unprocessed.append(file_path)

        if processed_count > 0:
            logger.info(f"Skipping {processed_count} already processed files")
            print(f"    ⏭️  Skipping {processed_count} already processed files")

        return unprocessed


def process_json_file_worker(file_path: Path) -> List[Dict[str, Any]]:
    """Worker function for processing JSON files in parallel."""
    try:
        import ijson

        file_size = file_path.stat().st_size

        # Use streaming parser for files larger than 10MB
        if file_size > 10 * 1024 * 1024:  # 10MB
            try:
                with open(file_path, "rb") as f:
                    items = []
                    parser = ijson.items(f, "item")
                    for item in parser:
                        items.append(item)
                    return items
            except (ijson.JSONError, ValueError):
                pass

        # Regular JSON parsing for smaller files or when streaming fails
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]

    except Exception as e:
        logger.warning(f"Error processing {file_path}: {e}")
        return []


def process_csv_file_worker(file_path: Path) -> List[Dict[str, Any]]:
    """Worker function for processing CSV files in parallel."""
    try:
        import pandas as pd

        df = pd.read_csv(file_path)
        return df.to_dict("records")
    except Exception as e:
        logger.warning(f"Error processing CSV {file_path}: {e}")
        return []
