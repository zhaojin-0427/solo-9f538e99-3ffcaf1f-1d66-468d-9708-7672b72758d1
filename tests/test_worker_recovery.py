"""Multi-worker startup recovery, ownership and commit/recovery races."""
from __future__ import annotations

import io
import multiprocessing
import sqlite3
import time

import pytest

from app import db as db_module
from app.db import connect, init_db, recover_orphaned_batches
from app.ingestion import BatchTakenOver, ingest_stream
from app.roads import build_road_layer
from app.web import create_app
from app.worker import worker_alive
from tests.conftest import sample_bytes

CTX = multiprocessing.get_context("fork")


def _hold_processing_batch(db_path, ready, release, filename, node_id):
    """Child process: register as a worker, leave a processing batch open.

    Stays alive until ``release`` is set (or is killed), holding the batch
    exactly like a worker that is still parsing it.
    """
    from app.db import connect as child_connect
    from app.worker import ensure_worker

    conn = child_connect(db_path)
    state = ensure_worker(conn)
    cur = conn.execute(
        "INSERT INTO batches (filename, status, worker_id) "
        "VALUES (?, 'processing', ?)",
        (filename, state.worker_id),
    )
    bid = cur.lastrowid
    conn.execute(
        "INSERT INTO object_versions (object_type, object_id, version, visible, "
        "lat, lon, tags_json, refs_json, batch_id, owner_worker_id) "
        "VALUES ('node', ?, 1, 1, 1.0, 2.0, '{}', NULL, ?, ?)",
        (node_id, bid, state.worker_id),
    )
    conn.execute(
        "INSERT INTO object_versions (object_type, object_id, version, visible, "
        "lat, lon, tags_json, refs_json, batch_id, owner_worker_id) "
        "VALUES ('node', ?, 1, 1, 3.0, 4.0, '{}', NULL, ?, ?)",
        (node_id + 1, bid, state.worker_id),
    )
    ready.set()
    release.wait(30)
    conn.close()


def test_live_worker_batch_is_not_taken_over_by_new_startup(db_path):
    ready, release = CTX.Event(), CTX.Event()
    proc = CTX.Process(
        target=_hold_processing_batch,
        args=(str(db_path), ready, release, "in_flight.osc", 501),
    )
    proc.start()
    try:
        assert ready.wait(10), "child worker did not start"

        # Another worker process starts up (create_app is what a respawned
        # gunicorn worker runs): it must leave the in-flight batch and its
        # rows alone.
        create_app(db_path=str(db_path))
        conn = connect(db_path)
        batch = conn.execute("SELECT * FROM batches").fetchone()
        assert batch["status"] == "processing"
        rows = conn.execute(
            "SELECT COUNT(*) AS n FROM object_versions WHERE batch_id = ?",
            (batch["id"],),
        ).fetchone()["n"]
        assert rows == 2
        conn.close()

        # While parsing continues, another upload can still commit and the
        # download for an existing committed batch still works.
        conn = connect(db_path)
        result = ingest_stream(
            conn, "01_base_extract.osm", io.BytesIO(sample_bytes("01_base_extract.osm"))
        )
        roads, pending = build_road_layer(conn, result.batch_id)
        assert sorted(r.way_id for r in roads) == [100, 101]
        assert pending == []
        conn.close()
    finally:
        release.set()
        proc.join(10)
        if proc.is_alive():  # pragma: no cover
            proc.kill()
            proc.join(10)

    # The worker is gone now: a replacement worker's startup must reject the
    # abandoned batch as a whole and delete its residual object rows.
    init_db(db_path)
    conn = connect(db_path)
    batch = conn.execute(
        "SELECT * FROM batches WHERE filename = 'in_flight.osc'"
    ).fetchone()
    assert batch["status"] == "rejected"
    assert "rolled back" in (batch["error"] or "")
    leftover = conn.execute(
        "SELECT COUNT(*) AS n FROM object_versions WHERE batch_id = ?",
        (batch["id"],),
    ).fetchone()["n"]
    assert leftover == 0
    # The committed batch from the other worker is untouched.
    committed = conn.execute(
        "SELECT status FROM batches WHERE filename = '01_base_extract.osm'"
    ).fetchone()
    assert committed["status"] == "committed"
    assert len(build_road_layer(conn, result.batch_id)[0]) == 2
    conn.close()


def _slow_stream_ingest(db_path, ready, release, filename, payload):
    """Child process: real ingest whose parse window is held open.

    Returns the outcome through a queue: the main test starts a replacement
    worker while this parse is in flight, then releases it so the original
    worker commits normally.
    """
    import queue  # noqa: F401 - just for docs
    import time as _time

    from app.db import connect as child_connect
    from app.ingestion import ingest_stream

    class SlowStream(io.BytesIO):
        def read(self, *args, **kwargs):
            ready.set()
            release.wait(30)
            return super().read(*args, **kwargs)

        def readinto(self, *args, **kwargs):
            ready.set()
            release.wait(30)
            return super().readinto(*args, **kwargs)

    conn = child_connect(db_path)
    try:
        result = ingest_stream(conn, filename, SlowStream(payload))
        print(f"RESULT committed {result.batch_id}", flush=True)
    except Exception as exc:  # pragma: no cover - surfaced as failure
        print(f"RESULT error {exc!r}", flush=True)
    finally:
        conn.close()


def test_ingest_in_flight_when_another_worker_starts_still_commits(db_path):
    ready, release = CTX.Event(), CTX.Event()
    payload = sample_bytes("01_base_extract.osm")
    proc = CTX.Process(
        target=_slow_stream_ingest,
        args=(str(db_path), ready, release, "slow.osm", payload),
    )
    proc.start()
    try:
        assert ready.wait(10), "child did not enter the parse window"
        # A replacement worker boots while the parse is mid-flight.
        create_app(db_path=str(db_path))
        conn = connect(db_path)
        assert conn.execute(
            "SELECT status FROM batches WHERE filename = 'slow.osm'"
        ).fetchone()["status"] == "processing"
        conn.close()

        release.set()
        proc.join(30)
        assert proc.exitcode == 0
    finally:
        release.set()
        if proc.is_alive():  # pragma: no cover
            proc.kill()
            proc.join(10)

    conn = connect(db_path)
    batch = conn.execute(
        "SELECT * FROM batches WHERE filename = 'slow.osm'"
    ).fetchone()
    assert batch["status"] == "committed"
    roads, pending = build_road_layer(conn, batch["id"])
    assert sorted(r.way_id for r in roads) == [100, 101]
    assert pending == []
    conn.close()


def test_killed_worker_batch_is_rejected_on_next_startup(db_path):
    ready, release = CTX.Event(), CTX.Event()
    proc = CTX.Process(
        target=_hold_processing_batch,
        args=(str(db_path), ready, release, "crashed.osc", 601),
    )
    proc.start()
    assert ready.wait(10)
    proc.kill()
    proc.join(10)

    init_db(db_path)
    conn = connect(db_path)
    batch = conn.execute("SELECT * FROM batches").fetchone()
    assert batch["status"] == "rejected"
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM object_versions WHERE batch_id = ?",
        (batch["id"],),
    ).fetchone()["n"] == 0
    conn.close()


def test_recovery_recheck_leaves_a_batch_committed_in_the_meantime(db_path, monkeypatch):
    """Recovery lists a batch, then that batch commits before cleanup runs.

    The guarded transaction must see the new status and neither reject it nor
    delete its rows, so the successful batch's layer is unchanged.
    """
    conn = connect(db_path)
    # A batch owned by a worker row that recovery will judge dead.
    conn.execute(
        "INSERT INTO workers (worker_id, pid, hostname, boot_id, proc_start, "
        "started_at, heartbeat_at) VALUES ('dead-worker', 999999, 'elsewhere', "
        "'boot', NULL, 0, 0)"
    )
    bid = conn.execute(
        "INSERT INTO batches (filename, status, worker_id) "
        "VALUES ('late_commit.osm', 'processing', 'dead-worker')"
    ).lastrowid
    for kind, oid, lat, lon, tags, refs in [
        ("node", 1, 31.0, 121.0, "{}", None),
        ("node", 2, 31.1, 121.1, "{}", None),
        ("way", 100, None, None, '{"highway":"residential"}', "[1,2]"),
    ]:
        conn.execute(
            "INSERT INTO object_versions (object_type, object_id, version, visible, "
            "lat, lon, tags_json, refs_json, batch_id, owner_worker_id) "
            "VALUES (?, ?, 1, 1, ?, ?, ?, ?, ?, 'dead-worker')",
            (kind, oid, lat, lon, tags, refs, bid),
        )
    conn.close()

    committed = {"done": False}
    real_alive = db_module.worker_alive

    def alive_then_late_commit(worker, db_path_arg=None):
        # First liveness call happens after recovery selected the candidate
        # but before its cleanup transaction. Simulate the owning worker
        # finishing its commit right in that window from another connection.
        if not committed["done"]:
            committed["done"] = True
            side = connect(db_path)
            side.execute("BEGIN IMMEDIATE")
            side.execute(
                "UPDATE batches SET status = 'committed', error = NULL WHERE id = ?",
                (bid,),
            )
            side.execute("COMMIT")
            side.close()
        return real_alive(worker, db_path_arg)

    monkeypatch.setattr(db_module, "worker_alive", alive_then_late_commit)
    recover_orphaned_batches(connect(db_path))

    conn = connect(db_path)
    batch = conn.execute("SELECT status FROM batches WHERE id = ?", (bid,)).fetchone()
    assert batch["status"] == "committed"
    roads, pending = build_road_layer(conn, bid)
    assert [r.way_id for r in roads] == [100]
    assert pending == []
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM object_versions WHERE batch_id = ?", (bid,)
    ).fetchone()["n"] == 3
    conn.close()


def test_ingest_that_lost_its_batch_to_recovery_stays_a_complete_rejection(
    db_path, monkeypatch
):
    """Commit/recovery interleaving: recovery wins, ingest must not re-add rows."""
    import app.ingestion as ingestion_module

    real_parse = ingestion_module.parse_osm
    raced = {"done": False}

    def parse_and_race(stream, *args, **kwargs):
        elements = real_parse(stream, *args, **kwargs)
        if not raced["done"]:
            raced["done"] = True
            # Another worker's startup recovery considers this worker dead
            # (simulated) and rejects the still-processing batch.
            side = connect(db_path)
            monkeypatch.setattr(db_module, "worker_alive", lambda *a, **k: False)
            try:
                recover_orphaned_batches(side)
            finally:
                side.close()
        return elements

    monkeypatch.setattr(ingestion_module, "parse_osm", parse_and_race)

    conn = connect(db_path)
    with pytest.raises(BatchTakenOver):
        ingest_stream(
            conn, "01_base_extract.osm", io.BytesIO(sample_bytes("01_base_extract.osm"))
        )
    conn.close()

    conn = connect(db_path)
    statuses = conn.execute("SELECT status, error FROM batches").fetchall()
    assert [row["status"] for row in statuses] == ["rejected"]
    assert len(build_road_layer(conn, 1)[0]) == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM object_versions").fetchone()["n"] == 0
    conn.close()


def test_worker_liveness_lease_and_missing_evidence():
    now = time.time()
    base = {
        "worker_id": "w1",
        "pid": 999999,            # certainly not running
        "hostname": "other-host",
        "boot_id": "other-boot",
        "proc_start": None,
    }
    assert worker_alive({**base, "heartbeat_at": now}) is True
    assert worker_alive({**base, "heartbeat_at": now - 10_000}) is False
    assert worker_alive({**base, "heartbeat_at": None}) is False
    assert worker_alive({"worker_id": "", "heartbeat_at": now}) is False
    # Unknown registry row (owner id not in workers) counts as dead: the
    # caller passes None, not a row, in that case.


def test_foreign_host_worker_is_kept_alive_by_kernel_lock(db_path):
    """Cross-container view: pids/hostnames differ, the flock still proves life."""
    ready, release = CTX.Event(), CTX.Event()
    proc = CTX.Process(
        target=_hold_processing_batch,
        args=(str(db_path), ready, release, "remote.osc", 701),
    )
    proc.start()
    try:
        assert ready.wait(10)
        conn = connect(db_path)
        worker_id = conn.execute(
            "SELECT worker_id FROM batches WHERE filename = 'remote.osc'"
        ).fetchone()["worker_id"]
        # Pretend the owner lives in another container: /proc and heartbeat
        # cannot help here, only the kernel lock can.
        conn.execute(
            "UPDATE workers SET hostname = 'another-container', boot_id = 'x', "
            "pid = 1, proc_start = NULL, heartbeat_at = 0 WHERE worker_id = ?",
            (worker_id,),
        )
        conn.close()

        init_db(db_path)
        conn = connect(db_path)
        batch = conn.execute(
            "SELECT status FROM batches WHERE filename = 'remote.osc'"
        ).fetchone()
        assert batch["status"] == "processing"
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM object_versions WHERE owner_worker_id = ?",
            (worker_id,),
        ).fetchone()["n"] == 2
        conn.close()
    finally:
        release.set()
        proc.join(10)
        if proc.is_alive():  # pragma: no cover
            proc.kill()
            proc.join(10)


def test_upgrade_of_existing_volume_keeps_batch_ids_and_detail_urls(tmp_path):
    """A pre-ownership database upgrades in place without renumbering."""
    path = tmp_path / "legacy.db"
    raw = sqlite3.connect(path, isolation_level=None)
    raw.executescript(
        """
        CREATE TABLE batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('processing','committed','rejected')),
            error TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );
        CREATE TABLE object_versions (
            object_type TEXT NOT NULL CHECK (object_type IN ('node','way')),
            object_id INTEGER NOT NULL,
            version INTEGER NOT NULL,
            visible INTEGER NOT NULL,
            lat REAL, lon REAL,
            tags_json TEXT NOT NULL DEFAULT '{}',
            refs_json TEXT,
            batch_id INTEGER NOT NULL REFERENCES batches(id),
            PRIMARY KEY (object_type, object_id, version)
        ) WITHOUT ROWID;
        INSERT INTO batches (filename, status) VALUES ('legacy_one.osm','committed');
        INSERT INTO batches (filename, status) VALUES ('legacy_two.osc','rejected');
        INSERT INTO object_versions
            (object_type, object_id, version, visible, lat, lon, tags_json, refs_json, batch_id)
        VALUES ('node', 1, 1, 1, 31.0, 121.0, '{}', NULL, 1);
        INSERT INTO object_versions
            (object_type, object_id, version, visible, lat, lon, tags_json, refs_json, batch_id)
        VALUES ('node', 2, 1, 1, 31.1, 121.1, '{}', NULL, 1);
        INSERT INTO object_versions
            (object_type, object_id, version, visible, lat, lon, tags_json, refs_json, batch_id)
        VALUES ('way', 100, 1, 1, NULL, NULL,
                '{"highway":"residential"}', '[1,2]', 1);
        """
    )
    raw.close()

    app = create_app(db_path=str(path))
    app.config.update(TESTING=True)
    client = app.test_client()

    listing = client.get("/").data.decode()
    assert "#1" in listing and "legacy_one.osm" in listing
    detail = client.get("/batches/1")
    assert detail.status_code == 200 and "legacy_one.osm" in detail.data.decode()
    geo = client.get("/batches/1/geojson")
    assert geo.status_code == 200
    download = client.get("/batches/1/geojson").headers["Content-Disposition"]
    assert "batch_1_highways.geojson" in download
    assert client.get("/batches/2/geojson").status_code == 409

    conn = connect(path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(batches)")}
    assert {"worker_id"} <= cols
    assert conn.execute("SELECT MAX(id) AS m FROM batches").fetchone()["m"] == 2
    # New uploads continue numbering after the legacy ids.
    result = ingest_stream(
        conn, "02_missing_node.osm", io.BytesIO(sample_bytes("02_missing_node.osm"))
    )
    assert result.batch_id == 3
    conn.close()


def test_two_startups_racing_leave_no_processing_batch(db_path):
    """Two workers creating the app concurrently recover cleanly."""
    ready = CTX.Barrier(3)

    def startup():
        ready.wait(10)
        init_db(db_path)

    procs = [CTX.Process(target=startup) for _ in range(2)]
    for p in procs:
        p.start()
    ready.wait(10)
    for p in procs:
        p.join(15)
        assert p.exitcode == 0
