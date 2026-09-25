"""
Tests for concurrent reads and writes in Vault without silently losing updates.
"""

import asyncio
import pytest


@pytest.mark.asyncio
async def test_concurrent_writes_distinct_keys(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    num_objects = 20

    async def _write_obj(idx: int):
        data = f"Content for object {idx}".encode("utf-8")
        return await engine.put_object(
            bucket="concurrent-test",
            key=f"file_{idx}.txt",
            data=data
        )

    tasks = [_write_obj(i) for i in range(num_objects)]
    results = await asyncio.gather(*tasks)
    assert len(results) == num_objects

    # Verify all objects are readable
    async def _read_obj(idx: int):
        rec, data = await engine.get_object(bucket="concurrent-test", key=f"file_{idx}.txt")
        assert data == f"Content for object {idx}".encode("utf-8")

    read_tasks = [_read_obj(i) for i in range(num_objects)]
    await asyncio.gather(*read_tasks)


@pytest.mark.asyncio
async def test_concurrent_writes_same_key(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    num_writers = 10

    async def _write_version(idx: int):
        payload = f"version_{idx}_payload".encode("utf-8")
        return await engine.put_object(
            bucket="race-test",
            key="shared-log.txt",
            data=payload
        )

    tasks = [_write_version(i) for i in range(num_writers)]
    committed_records = await asyncio.gather(*tasks)

    # All writes succeeded and received unique version IDs
    version_ids = [r.version_id for r in committed_records]
    assert len(set(version_ids)) == num_writers

    # Latest record must match one of the written versions
    latest_rec, latest_data = await engine.get_object(bucket="race-test", key="shared-log.txt")
    assert latest_rec.version_id in version_ids
    assert latest_data.decode("utf-8").startswith("version_")
