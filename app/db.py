"""SQLite persistence layer.

Design overview
---------------
The service keeps two kinds of history:

* ``batches``      -- one row per uploaded file / ingestion attempt.
* ``object_versions`` -- every distinct (object type, id, version) ever seen.

The object-version table is an append-only event log.  The state of the map
*as of* any batch B is reconstructed by taking, for each object, the highest
version whose ``batch_id <= B``.  This makes batches immutable: later uploads
append rows but never overwrite or erase what an earlier batch produced, so
the GeoJSON downloaded for batch 3 stays identical after batch 7 arrives.

A batch is fully applied inside one SQLite transaction.  If validation or
commit fails the transaction is rolled back and the batch is marked
``rejected`` -- partial state can never survive a failure or a process
restart (rows left in the ``processing`` state by a crash are recovered at
startup).

Several worker processes share this database.  A ``processing`` batch may
belong to a worker that is still parsing it, so startup recovery may only
take over batches owned by workers that are provably dead (see
``app.worker``); live workers' batches are left untouched.  Ownership and
the liveness evidence live in the ``workers`` table and on
``batches.worker_id``.
"""
from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from . import config
from .worker import worker_alive

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS workers (
    worker_id     TEXT    PRIMARY KEY,
    pid           INTEGER,
    hostname      TEXT,
    boot_id       TEXT,
    proc_start    TEXT,
    started_at    REAL    NOT NULL,
    heartbeat_at  REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT    NOT NULL,
    status      TEXT    NOT NULL CHECK (status IN ('processing', 'committed', 'rejected')),
    error       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    worker_id   TEXT
);

CREATE TABLE IF NOT EXISTS object_versions (
    object_type  TEXT    NOT NULL CHECK (object_type IN ('node', 'way')),
    object_id    INTEGER NOT NULL,
    version      INTEGER NOT NULL,
    visible      INTEGER NOT NULL,
    lat          REAL,
    lon          REAL,
    tags_json    TEXT    NOT NULL DEFAULT '{}',
    refs_json    TEXT,
    batch_id     INTEGER NOT NULL REFERENCES batches(id),
    owner_worker_id TEXT,
    PRIMARY KEY (object_type, object_id, version)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_object_versions_batch
    ON object_versions (batch_id);
CREATE INDEX IF NOT EXISTS idx_batches_status
    ON batches (status, id);
"""

# Columns added for multi-worker ownership. They are added in place on an
# existing (already populated) database, leaving batch ids and every stored
# row untouched -- upgrades keep their numbering and detail URLs.
MIGRATION_COLUMNS = (
    ("batches", "worker_id", "TEXT"),
    ("object_versions", "owner_worker_id", "TEXT"),
)

RECOVERY_MARKER = "[interrupted by restart; rolled back]"


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _main_db_file(conn: sqlite3.Connection) -> str | None:
    for row in conn.execute("PRAGMA database_list"):
        name, path = row[1], row[2]
        if name == "main" and path:
            return path
    return None


def _migrate(conn: sqlite3.Connection) -> None:
    """Add ownership columns in place; safe to run concurrently by workers."""
    for table, column, decl in MIGRATION_COLUMNS:
        if column in _column_names(conn, table):
            continue
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        except sqlite3.OperationalError as exc:
            # Another worker won the race and added it first.
            if "duplicate column name" not in str(exc).lower():
                raise
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_batches_processing "
        "ON batches (status, worker_id)"
    )


def get_db_path() -> Path:
    return Path(config.DB_PATH)


def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Open a connection with the pragmas the application relies on."""
    path = Path(db_path) if db_path is not None else get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        timeout=30.0,
        isolation_level=None,  # explicit transaction control
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    # Switching a brand-new file to WAL takes an exclusive lock and, unlike
    # ordinary statements, is not covered by busy_timeout; when several
    # workers open the database at the same instant it can report
    # "database is locked" immediately. Retry briefly.
    for attempt in range(10):
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 9:
                conn.close()
                raise
            time.sleep(0.05 * (attempt + 1))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


def init_db(db_path: Path | str | None = None) -> None:
    """Create/migrate schema and recover batches abandoned by dead workers.

    Safe to call concurrently from every worker process at startup: each
    recovery runs in its own ``BEGIN IMMEDIATE`` transaction and re-checks
    the batch status and owner *inside* that transaction, so a batch that a
    live worker commits meanwhile is never touched.
    """
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
        recover_orphaned_batches(conn)
    finally:
        conn.close()


def recover_orphaned_batches(conn: sqlite3.Connection) -> list[int]:
    """Reject ``processing`` batches whose owning worker is dead.

    Batches owned by a live worker (including this very process, or a worker
    whose heartbeat is fresh) are left completely alone -- neither their
    status nor any of their object rows are touched.  For dead owners the
    deletion of dangling object rows and the status flip happen in one
    transaction, conditioned on the batch still being ``processing`` and
    still owned by that dead worker.  Returns the ids rejected.
    """
    candidates = conn.execute(
        "SELECT id, worker_id FROM batches WHERE status = 'processing' ORDER BY id"
    ).fetchall()
    worker_rows = {
        row["worker_id"]: row
        for row in conn.execute("SELECT * FROM workers")
    }
    db_file = _main_db_file(conn)

    rejected: list[int] = []
    for batch in candidates:
        owner_id = batch["worker_id"]
        owner = worker_rows.get(owner_id) if owner_id else None
        if owner is not None and worker_alive(owner, db_file):
            continue  # live worker: never take over or clean its rows
        # No owner row / unreadable evidence also means the writer is gone
        # (a processing batch with no owner can only be a legacy leftover).
        reason = RECOVERY_MARKER

        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, worker_id FROM batches WHERE id = ?", (batch["id"],)
            ).fetchone()
            if (
                row is None
                or row["status"] != "processing"
                or row["worker_id"] != owner_id
            ):
                # Someone committed/rejected it, or a recovery in another
                # process already took it over: do nothing.
                conn.execute("ROLLBACK")
                continue
            conn.execute(
                "DELETE FROM object_versions WHERE batch_id = ?", (batch["id"],)
            )
            conn.execute(
                "UPDATE batches SET status = 'rejected', "
                "error = CASE WHEN error IS NULL OR error = '' "
                "             THEN ? ELSE error || ' ' || ? END "
                "WHERE id = ?",
                (reason, reason, batch["id"]),
            )
            conn.execute("COMMIT")
            rejected.append(batch["id"])
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    # Drop registry rows for dead workers that no longer own a processing
    # batch. Their lock files stay on disk; an unlocked file is simply
    # evidence of death and is reused/ignored safely.
    still_owned = {
        row["worker_id"]
        for row in conn.execute(
            "SELECT DISTINCT worker_id FROM batches WHERE status = 'processing'"
        )
    }
    for worker_id, owner in worker_rows.items():
        if not worker_id or worker_id in still_owned:
            continue
        if worker_alive(owner, db_file):
            continue
        try:
            conn.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))
        except sqlite3.Error:  # pragma: no cover - best-effort cleanup
            continue
        if db_file:
            try:
                os.unlink(f"{db_file}.worker-{worker_id}.lock")
            except OSError:
                pass
    return rejected


@contextmanager
def db_session(db_path: Path | str | None = None) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()
