"""
Tests for Data Integrity, Bitrot Detection, Read-Repair, and Background Scrubber Auto-Healing.
"""

import asyncio
import hashlib
import pytest
from vault.repair import ActiveScrubber
from vault.engine import ObjectUnrecoverableError


@pytest.mark.asyncio
async def test_read_repair_on_bitrot(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    # 1. Put an object replicated across nodes
    payload = b"Important database backup chunk that must not suffer bitrot"
    rec = await engine.put_object(bucket="backups", key="db.chunk", data=payload)
    storage_key = f"backups/db.chunk/{rec.version_id}.blob"

    # 2. Inject bitrot corruption into node-1's copy on disk
    target_node_server = storage_nodes[0]
    blob_path = target_node_server.objects_dir / storage_key
    assert blob_path.exists()

    with open(blob_path, "r+b") as f:
        f.seek(0)
        f.write(b"CORRUPTED_BITROT_GARBAGE_DATA")

    # Verify that verify_object detects bitrot on node-1
    hasher = hashlib.sha256()
    with open(blob_path, "rb") as f:
        hasher.update(f.read())
    corrupted_sha = hasher.hexdigest()
    assert corrupted_sha != rec.sha256

    # 3. Read object via Engine:
    # Engine will detect checksum mismatch on corrupted node-1, fallback to node-2,
    # return healthy data, and trigger background Read-Repair!
    read_rec, read_data = await engine.get_object(bucket="backups", key="db.chunk")
    assert read_data == payload

    # Allow brief moment for read-repair async task
    await asyncio.sleep(0.5)

    # 4. Verify node-1 has been restored with the healthy data!
    with open(blob_path, "rb") as f:
        repaired_data = f.read()
    assert repaired_data == payload


@pytest.mark.asyncio
async def test_active_scrubber_auto_heal(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    # 1. Write two objects
    await engine.put_object(bucket="media", key="video1.mp4", data=b"Video1 payload bytes")
    rec2 = await engine.put_object(bucket="media", key="video2.mp4", data=b"Video2 payload bytes")

    # 2. Corrupt video2 on node-2
    target_node = storage_nodes[1]
    blob_path = target_node.objects_dir / f"media/video2.mp4/{rec2.version_id}.blob"
    if blob_path.exists():
        with open(blob_path, "wb") as f:
            f.write(b"CORRUPTED")

    # 3. Run Active Scrubber
    scrubber = ActiveScrubber(config, meta_store, cluster_mgr)
    await scrubber.start()
    report = await scrubber.run_scrub()

    # Verify scrubber detected the bitrot and healed the replica
    assert report["objects_scanned"] == 2
    assert report["corrupted_replicas"] >= 1
    assert report["repaired_replicas"] >= 1
    assert report["unrecoverable_count"] == 0

    await scrubber.stop()

    # Verify on-disk file is healed
    with open(blob_path, "rb") as f:
        assert f.read() == b"Video2 payload bytes"


@pytest.mark.asyncio
async def test_all_replicas_corrupted_reports_unrecoverable(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    rec = await engine.put_object(bucket="critical", key="lost.dat", data=b"Doomed data")
    storage_key = f"critical/lost.dat/{rec.version_id}.blob"

    # Corrupt on ALL nodes
    for s in storage_nodes:
        p = s.objects_dir / storage_key
        if p.exists():
            with open(p, "wb") as f:
                f.write(b"TOTAL_CORRUPTION")

    # Engine read must raise ObjectUnrecoverableError
    with pytest.raises(ObjectUnrecoverableError):
        await engine.get_object(bucket="critical", key="lost.dat")

    # Scrubber report must list it as unrecoverable
    scrubber = ActiveScrubber(config, meta_store, cluster_mgr)
    await scrubber.start()
    report = await scrubber.run_scrub()
    assert report["unrecoverable_count"] == 1
    await scrubber.stop()
