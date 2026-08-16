"""Schema migration runner.

Migrations are plain .sql files under db/migrations/, named NNN_description.sql,
applied in order exactly once. schema.sql (loaded as migration 1) creates the
full initial schema; later migrations are additive ALTER/CREATE statements.
"""

import sqlite3
from pathlib import Path

_SCHEMA_DIR = Path(__file__).parent
_MIGRATIONS_DIR = _SCHEMA_DIR / "migrations"


def _migration_files() -> list[tuple[int, str, Path]]:
    files = [(1, "initial schema", _SCHEMA_DIR / "schema.sql")]
    if _MIGRATIONS_DIR.exists():
        for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
            version = int(path.stem.split("_", 1)[0])
            files.append((version, path.stem, path))
    return sorted(files, key=lambda f: f[0])


def current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchone()
    if row is None:
        return 0
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    return row["version"] if row else 0


def migrate(conn: sqlite3.Connection) -> int:
    """Apply all not-yet-applied migrations. Returns the resulting schema version."""
    applied = current_version(conn)
    for version, name, path in _migration_files():
        if version <= applied:
            continue
        sql = path.read_text()
        conn.executescript(sql)
        conn.execute("DELETE FROM schema_version")
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))
        conn.commit()
        applied = version
    return applied
