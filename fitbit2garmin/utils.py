"""
Utility functions for the Fitbit to Garmin migration tool.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import concurrent.futures
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

    def process_files_parallel(
        self,
        files: List[Path],
        process_func: Callable,
        description: str = "Processing files",
    ) -> List[Any]:
        """Process files in parallel with progress tracking and memory management."""
        if not files:
            return []

        results = []
        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                from tqdm import tqdm
                import gc

                # Submit all tasks
                future_to_file = {
                    executor.submit(process_func, file_path): file_path
                    for file_path in files
                }

                # Process results as they complete
                for future in tqdm(
                    as_completed(future_to_file),
                    total=len(files),
                    desc=description,
                    leave=False,
                ):
                    file_path = future_to_file[future]
                    try:
                        result = future.result(timeout=30)  # 30 second timeout per file
                        if result:
                            results.extend(result)
                    except Exception as e:
                        logger.warning(f"Error processing {file_path}: {e}")
                    finally:
                        # Clean up the future to free memory
                        future_to_file.pop(future, None)
                        # Force garbage collection periodically
                        if len(results) % 500 == 0:
                            gc.collect()

        except Exception as e:
            logger.error(f"Parallel processing failed: {e}")
            # Fallback to sequential processing
            results = []
            for file_path in files:
                try:
                    result = process_func(file_path)
                    if result:
                        results.extend(result)
                except Exception as e:
                    logger.warning(f"Error processing {file_path}: {e}")

        return results

    def process_files_parallel_with_progress(
        self,
        files: List[Path],
        process_func: Callable,
        progress_bar,
        files_processed_so_far: int = 0,
    ) -> List[Any]:
        """Process files in parallel with external progress bar updates and health monitoring."""
        if not files:
            return []

        results = []
        failed_files = []
        
        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                import gc
                import time
                import psutil
                import signal

                # Submit all tasks
                future_to_file = {
                    executor.submit(process_func, file_path): file_path
                    for file_path in files
                }

                # Process results as they complete with individual progress updates
                completed_count = 0
                last_progress_time = time.time()
                
                for future in as_completed(future_to_file):
                    current_time = time.time()
                    file_path = future_to_file[future]
                    
                    try:
                        # Check if process is taking too long
                        if current_time - last_progress_time > 300:  # 5 minutes without progress
                            logger.warning("Processing appears stuck, attempting to continue...")
                            
                        result = future.result(timeout=120)  # Increased timeout to 120 seconds
                        if result:
                            results.extend(result)
                        
                        # Update progress bar immediately after each file
                        completed_count += 1
                        progress_bar.update(1)
                        last_progress_time = current_time
                        
                        # Update progress description with current file
                        progress_bar.set_description(f"    💓 Processing HR files [{completed_count}/{len(files)}]")
                        
                    except concurrent.futures.TimeoutError:
                        logger.warning(f"Timeout processing {file_path}, skipping...")
                        failed_files.append(file_path)
                        completed_count += 1
                        progress_bar.update(1)
                        
                    except Exception as e:
                        logger.warning(f"Error processing {file_path}: {e}")
                        failed_files.append(file_path)
                        completed_count += 1
                        progress_bar.update(1)
                        
                    finally:
                        # Clean up the future to free memory
                        future_to_file.pop(future, None)
                        # Force garbage collection periodically
                        if completed_count % 10 == 0:
                            gc.collect()
                            
                        # Check memory usage periodically
                        if completed_count % 50 == 0:
                            try:
                                process = psutil.Process()
                                memory_mb = process.memory_info().rss / 1024 / 1024
                                if memory_mb > 2048:  # 2GB memory warning
                                    logger.warning(f"High memory usage: {memory_mb:.1f}MB")
                            except:
                                pass

        except Exception as e:
            logger.error(f"Parallel processing failed: {e}")
            # Fallback to sequential processing with progress updates
            logger.info("Falling back to sequential processing...")
            progress_bar.set_description("    💓 Processing HR files (fallback)")
            
            results = []
            for i, file_path in enumerate(files):
                try:
                    result = process_func(file_path)
                    if result:
                        results.extend(result)
                except Exception as e:
                    logger.warning(f"Error processing {file_path}: {e}")
                    failed_files.append(file_path)
                finally:
                    progress_bar.update(1)
                    progress_bar.set_description(f"    💓 Processing HR files (fallback) [{i+1}/{len(files)}]")

        # Report on failed files
        if failed_files:
            logger.warning(f"Failed to process {len(failed_files)} files: {[f.name for f in failed_files[:5]]}")
            if len(failed_files) > 5:
                logger.warning(f"... and {len(failed_files) - 5} more failed files")

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
    """Worker function for processing JSON files in parallel with memory efficiency."""
    try:
        import orjson
        import gc
        import ijson

        # Check file size and use appropriate parsing strategy
        file_size = file_path.stat().st_size
        
        # Increased limit to 50MB for heart rate files
        if file_size > 50 * 1024 * 1024:  # 50MB limit
            logger.warning(f"Skipping very large file {file_path} ({file_size / (1024*1024):.1f}MB)")
            return []

        # Use streaming parser for files larger than 10MB
        if file_size > 10 * 1024 * 1024:
            logger.debug(f"Using streaming parser for large file {file_path} ({file_size / (1024*1024):.1f}MB)")
            try:
                with open(file_path, "rb") as f:
                    result = []
                    # Try to parse as array of items
                    try:
                        parser = ijson.items(f, "item")
                        for item in parser:
                            if isinstance(item, dict):
                                result.append(item)
                            # Limit memory usage
                            if len(result) > 10000:  # Process in chunks
                                break
                    except ijson.JSONError:
                        # If ijson fails, fall back to reading entire file
                        f.seek(0)
                        data = orjson.loads(f.read())
                        if isinstance(data, list):
                            result = [item for item in data if isinstance(item, dict)]
                        elif isinstance(data, dict):
                            result = [data]
                    
                    if len(result) > 1000:
                        gc.collect()
                    return result
                    
            except Exception as e:
                logger.warning(f"Streaming parser failed for {file_path}: {e}, trying standard parser")
                # Fall through to standard parsing

        # Standard parsing for smaller files
        with open(file_path, "rb") as f:
            data = orjson.loads(f.read())

            # Ensure we return a list of dictionaries
            if isinstance(data, list):
                # Filter out non-dictionary items
                result = [item for item in data if isinstance(item, dict)]
            elif isinstance(data, dict):
                result = [data]
            else:
                result = []

            # Force garbage collection for large results
            if len(result) > 1000:
                gc.collect()

            return result

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
