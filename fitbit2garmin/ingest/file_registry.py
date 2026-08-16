"""File-level resume registry, keyed by content hash.

Replaces the old MD5-filename-cache ResumeManager: content is hashed (sha256) so a
changed file is always re-ingested even if mtime/size look unchanged, and an
unchanged file is always skipped even across machines/cache-clears, since the
registry lives in the staging DB itself rather than a separate cache directory.
"""

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path


def hash_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class FileStatus:
    needs_ingest: bool
    content_hash: str


def check_file(conn: sqlite3.Connection, relative_path: str, abs_path: Path) -> FileStatus:
    """Decide whether a file needs (re-)ingesting, without mutating the registry."""
    content_hash = hash_file(abs_path)
    row = conn.execute(
        "SELECT content_hash, status FROM ingest_file WHERE file_path = ?",
        (relative_path,),
    ).fetchone()
    if row is not None and row["content_hash"] == content_hash and row["status"] == "ok":
        return FileStatus(needs_ingest=False, content_hash=content_hash)
    return FileStatus(needs_ingest=True, content_hash=content_hash)


def begin_ingest(
    conn: sqlite3.Connection,
    relative_path: str,
    source_group: str,
    abs_path: Path,
    content_hash: str,
) -> None:
    """Record that ingestion of this file is starting (status='pending')."""
    conn.execute(
        """
        INSERT INTO ingest_file (file_path, source_group, content_hash, file_size_bytes, mtime, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
        ON CONFLICT(file_path) DO UPDATE SET
            source_group = excluded.source_group,
            content_hash = excluded.content_hash,
            file_size_bytes = excluded.file_size_bytes,
            mtime = excluded.mtime,
            status = 'pending',
            error_message = NULL
        """,
        (relative_path, source_group, content_hash, abs_path.stat().st_size, abs_path.stat().st_mtime),
    )
    conn.commit()


def finish_ingest_ok(conn: sqlite3.Connection, relative_path: str, row_count: int) -> None:
    conn.execute(
        "UPDATE ingest_file SET status='ok', row_count=?, ingested_at=? WHERE file_path=?",
        (row_count, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), relative_path),
    )
    conn.commit()


def finish_ingest_error(conn: sqlite3.Connection, relative_path: str, error_message: str) -> None:
    conn.execute(
        "UPDATE ingest_file SET status='error', error_message=? WHERE file_path=?",
        (error_message, relative_path),
    )
    conn.commit()


def clear_prior_rows(conn: sqlite3.Connection, table: str, relative_path: str) -> None:
    """Delete this file's previously-ingested rows before re-ingesting it (idempotent re-run)."""
    conn.execute(f"DELETE FROM {table} WHERE source_file = ?", (relative_path,))
