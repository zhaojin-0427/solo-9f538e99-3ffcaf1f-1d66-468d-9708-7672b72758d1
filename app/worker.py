"""Worker identity, heartbeat lease and liveness checks.

The service runs several worker *processes* (gunicorn ``--workers 2``) that
share one SQLite database.  A batch row is created before its (possibly long)
parse starts and only flips to ``committed`` in the final write transaction,
so a ``processing`` row may belong to either

* a live worker that is still parsing/committing the batch, or
* a worker that died and left the batch behind.

Startup recovery must distinguish the two: taking over a live worker's batch
would fail the upload and, worse, delete object rows that its eventual commit
is about to make visible.

Each process therefore registers itself in ``workers`` with a unique id and
enough evidence to test liveness:

* same host + same boot id -> consult ``/proc`` (pid, state and process start
  time); this is immediate, immune to pid reuse, and works even when the
  heartbeat thread is blocked behind a long SQLite write transaction;
* otherwise (another container, or no ``/proc``) -> the kernel advisory lock
  (``flock``) held on ``<db>.worker-<id>.lock`` for the whole life of the
  process.  flock locks disappear with the owning file descriptor, so they
  work across containers sharing the volume even when hostnames and pids
  differ;
* last resort -> a heartbeat refreshed while batches are in flight, compared
  against ``WORKER_LEASE_SECONDS`` (for filesystems where advisory locks are
  unreliable).

A worker row with no usable liveness evidence is treated as dead.
"""
from __future__ import annotations

import errno
import os
import socket
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

try:  # POSIX advisory locks; absent on Windows (tests there use heartbeat).
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

from . import config

try:  # pragma: no cover - only unusual platforms lack /proc
    _BOOT_ID = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
except OSError:  # pragma: no cover
    _BOOT_ID = ""

_HOSTNAME = socket.gethostname()

_states: dict[str, "_WorkerState"] = {}
_states_lock = threading.Lock()


class BatchTakenOver(Exception):
    """Raised inside the commit transaction when recovery rejected the batch."""


@dataclass
class _WorkerState:
    db_path: str
    pid: int
    worker_id: str
    proc_start: str | None
    started_at: float
    lock: threading.Lock = field(default_factory=threading.Lock)
    active_batches: int = 0
    beat_stop: threading.Event | None = None
    beat_thread: threading.Thread | None = None
    lock_handle: object | None = None
    lock_held: bool = False


@dataclass
class BatchLease:
    """Handle returned when a batch starts; ``finish`` releases the heartbeat."""

    state: _WorkerState

    @property
    def worker_id(self) -> str:
        return self.state.worker_id

    def heartbeat_now(self, conn: sqlite3.Connection) -> None:
        _touch(conn, self.state.worker_id, time.time())

    def finish(self, conn: sqlite3.Connection | None = None) -> None:
        state = self.state
        with state.lock:
            if state.active_batches > 0:
                state.active_batches -= 1
            last = state.active_batches == 0
            if last and state.beat_stop is not None:
                state.beat_stop.set()
                state.beat_stop = None
                state.beat_thread = None
        if conn is not None:
            try:
                _touch(conn, state.worker_id, time.time())
            except sqlite3.Error:
                pass


def _main_db_path(conn: sqlite3.Connection) -> str:
    for row in conn.execute("PRAGMA database_list"):
        # (seq, name, file) -- access by name when possible.
        try:
            name, path = row["name"], row["file"]
        except (KeyError, IndexError):  # pragma: no cover - exotic row factory
            name, path = row[1], row[2]
        if name == "main" and path:
            return os.path.abspath(path)
    return ":memory:"


def _proc_start(pid: int) -> str | None:
    """Return Linux /proc/<pid>/stat field 22 (starttime) as a string."""
    info = _proc_info(pid)
    return info[1] if info is not None else None


def _proc_info(pid: int) -> tuple[str, str] | None:
    """Return ``(state, starttime)`` for a Linux pid, or None if unreadable.

    ``state`` is the single-letter /proc state ('R', 'S', 'Z', ...), which
    lets us treat a zombie (killed but not yet reaped, e.g. by a container
    runtime or a test parent) as dead even though ``kill(pid, 0)`` succeeds.
    """
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    # The comm field is parenthesised and may contain spaces/parentheses.
    try:
        fields = stat.rsplit(")", 1)[1].split()
        return fields[0], fields[19]  # state, field 22 overall
    except (IndexError, ValueError):  # pragma: no cover
        return None


def _state_for(conn: sqlite3.Connection) -> _WorkerState:
    path = _main_db_path(conn)
    pid = os.getpid()
    with _states_lock:
        state = _states.get(path)
        if state is None or state.pid != pid:
            # A forked child (gunicorn --preload) inherited the parent's dict
            # and thus its open lock file descriptors. Close them so the
            # child never keeps a dead parent's lease alive, then give the new
            # process its own identity.
            if state is not None and state.lock_handle is not None:
                try:
                    state.lock_handle.close()
                except OSError:  # pragma: no cover
                    pass
            state = _WorkerState(
                db_path=path,
                pid=pid,
                worker_id=uuid.uuid4().hex,
                proc_start=_proc_start(pid),
                started_at=time.time(),
            )
            _acquire_lock(state)
            _states[path] = state
    return state


def _lock_path(db_path: str, worker_id: str) -> str:
    return f"{db_path}.worker-{worker_id}.lock"


def _acquire_lock(state: _WorkerState) -> None:
    """Hold a lifetime flock on this worker's lock file."""
    if fcntl is None or state.db_path == ":memory:":
        return
    path = _lock_path(state.db_path, state.worker_id)
    try:
        handle = open(path, "a+b")
    except OSError:
        return
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        if exc.errno not in (errno.EAGAIN, errno.EACCES):  # pragma: no cover
            raise
        return  # someone else holds it (should not happen: id is unique)
    state.lock_handle = handle
    state.lock_held = True


def _lock_file_busy(path: str) -> bool | None:
    """True if another process holds the lock file, False if free, None if
    the file does not exist or locking is unsupported."""
    if fcntl is None or not os.path.exists(path):
        return None
    try:
        handle = open(path, "rb")
    except OSError:
        return None
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EACCES):
                return True
            return None
        # We got the lock: no live owner. Release it and report free.
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False
    finally:
        handle.close()


def _touch(conn: sqlite3.Connection, worker_id: str, now: float) -> None:
    conn.execute(
        "UPDATE workers SET heartbeat_at = ? WHERE worker_id = ?", (now, worker_id)
    )


def ensure_worker(conn: sqlite3.Connection) -> _WorkerState:
    """Register this process in ``workers`` (idempotent) and return its state."""
    state = _state_for(conn)
    now = time.time()
    conn.execute(
        """
        INSERT INTO workers
            (worker_id, pid, hostname, boot_id, proc_start, started_at, heartbeat_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(worker_id) DO UPDATE SET
            pid = excluded.pid,
            hostname = excluded.hostname,
            boot_id = excluded.boot_id,
            proc_start = excluded.proc_start,
            heartbeat_at = excluded.heartbeat_at
        """,
        (
            state.worker_id,
            state.pid,
            _HOSTNAME,
            _BOOT_ID,
            state.proc_start,
            state.started_at,
            now,
        ),
    )
    return state


def _heartbeat_loop(state: _WorkerState, stop: threading.Event) -> None:
    interval = max(0.5, float(config.WORKER_HEARTBEAT_SECONDS))
    while not stop.wait(interval):
        try:
            # Dedicated short-lived connection: the heartbeat must not queue
            # behind the ingestion connection's transactions for 30 seconds.
            conn = sqlite3.connect(state.db_path, timeout=5.0,
                                   isolation_level=None)
            try:
                _touch(conn, state.worker_id, time.time())
            finally:
                conn.close()
        except (sqlite3.Error, OSError):
            # A heartbeat failure is not fatal: same-host liveness is still
            # decided by the pid/proc-start evidence.
            continue


def _start_heartbeat(state: _WorkerState) -> None:
    stop = threading.Event()
    thread = threading.Thread(
        target=_heartbeat_loop,
        args=(state, stop),
        name=f"osm-heartbeat-{state.worker_id[:8]}",
        daemon=True,
    )
    state.beat_stop = stop
    state.beat_thread = thread
    thread.start()


def batch_started(conn: sqlite3.Connection) -> BatchLease:
    """Register the worker, mark a batch as in flight and start the heartbeat."""
    state = ensure_worker(conn)
    _touch(conn, state.worker_id, time.time())
    with state.lock:
        state.active_batches += 1
        if state.active_batches == 1:
            _start_heartbeat(state)
    return BatchLease(state)


def _pid_running(pid: int, expected_start: str | None) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass  # exists but belongs to another user
    except OSError:
        return False

    info = _proc_info(pid)
    if info is not None:
        state, actual_start = info
        if state == "Z":
            return False  # dead, only waiting to be reaped
        if expected_start and actual_start != expected_start:
            return False  # pid was recycled by an unrelated process
    return True


def worker_alive(worker: dict | sqlite3.Row, db_path: str | Path | None = None) -> bool:
    """Decide whether the worker described by a ``workers`` row is still alive.

    ``worker`` must expose ``worker_id``, ``pid``, ``hostname``, ``boot_id``,
    ``proc_start`` and ``heartbeat_at``.  Missing/unknown evidence means dead.
    """
    try:
        worker_id = worker["worker_id"]
    except (KeyError, IndexError):
        return False
    if not worker_id:
        return False

    pid = worker["pid"]
    if pid == os.getpid():
        # A row for this very process: alive as long as the recorded start
        # time matches (guards against stale rows after pid reuse).
        expected_start = worker["proc_start"]
        current_start = _proc_start(os.getpid())
        return expected_start is None or current_start is None or \
            expected_start == current_start

    if db_path is not None:
        busy = _lock_file_busy(_lock_path(os.fspath(db_path), worker_id))
        if busy is not None:
            # Kernel advisory lock, taken before the worker registers itself
            # and released only when its descriptor closes. This is
            # authoritative across pid namespaces (containers sharing a
            # hostname/boot id) and survives pid reuse; an existing but
            # unlocked file proves the owner is gone.
            return busy

    hostname = worker["hostname"]
    boot_id = worker["boot_id"]
    same_machine = hostname == _HOSTNAME and (not _BOOT_ID or boot_id == _BOOT_ID)
    if same_machine:
        # /proc check: a heartbeat thread stuck behind a long SQLite write
        # transaction must not make a live worker look dead.
        return _pid_running(pid, worker["proc_start"])

    heartbeat = worker["heartbeat_at"]
    if heartbeat is None:
        return False
    try:
        age = time.time() - float(heartbeat)
    except (TypeError, ValueError):
        return False
    return 0 <= age <= float(config.WORKER_LEASE_SECONDS)
