"""
Tests for large object multipart uploads, out-of-order parts, assembly, and abort cleanup.
"""

import hashlib
import pytest


@pytest.mark.asyncio
async def test_multipart_upload_lifecycle(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    # 1. Initiate Multipart
    mp = engine.initiate_multipart_upload(
        bucket="large-files",
        key="archive.tar",
        content_type="application/x-tar"
    )
    assert mp.upload_id is not None
    assert mp.state == "INITIATED"

    # 2. Upload 3 parts out-of-order (Part 3, Part 1, Part 2)
    part1_data = b"CHUNK_1_" * 2000
    part2_data = b"CHUNK_2_" * 3000
    part3_data = b"CHUNK_3_" * 1500

    # Part 3
    p3 = await engine.upload_part(mp.upload_id, 3, part3_data)
    assert p3.part_number == 3

    # Part 1
    p1 = await engine.upload_part(mp.upload_id, 1, part1_data)
    assert p1.part_number == 1

    # Part 2
    p2 = await engine.upload_part(mp.upload_id, 2, part2_data)
    assert p2.part_number == 2

    # 3. Complete Multipart
    committed = await engine.complete_multipart_upload(mp.upload_id)
    expected_full_data = part1_data + part2_data + part3_data
    expected_sha = hashlib.sha256(expected_full_data).hexdigest()

    assert committed.size_bytes == len(expected_full_data)
    assert committed.sha256 == expected_sha

    # 4. Verify Download
    rec, downloaded = await engine.get_object(bucket="large-files", key="archive.tar")
    assert downloaded == expected_full_data


@pytest.mark.asyncio
async def test_multipart_abort_cleanup(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    mp = engine.initiate_multipart_upload(bucket="temp", key="aborted.bin")
    await engine.upload_part(mp.upload_id, 1, b"PART1")

    # Abort
    await engine.abort_multipart_upload(mp.upload_id)
    record = meta_store.get_multipart_upload(mp.upload_id)
    assert record.state == "ABORTED"
    assert len(meta_store.list_parts(mp.upload_id)) == 0
