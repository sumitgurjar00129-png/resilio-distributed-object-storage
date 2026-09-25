"""
Advanced Chaos Engineering tests for Vault.
Simulates high latency, mid-stream node crashes, and concurrent traffic during rebalance.
"""

import asyncio
import pytest
from vault.engine import QuorumError


@pytest.mark.asyncio
async def test_chaos_latency_injection(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    # Inject 150ms artificial delay into node-1
    storage_nodes[0].simulated_delay_sec = 0.15

    # Write should still succeed within timeout
    res = await engine.put_object(bucket="chaos", key="delayed.txt", data=b"Payload with latency")
    assert res.key == "delayed.txt"

    # Reset
    storage_nodes[0].simulated_delay_sec = 0.0


@pytest.mark.asyncio
async def test_chaos_node_crash_during_multipart(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    mp = engine.initiate_multipart_upload(bucket="chaos", key="surviving_crash.dat")

    # Upload part 1 successfully
    await engine.upload_part(mp.upload_id, 1, b"PART_1_BEFORE_CRASH")

    # Crash node-3
    storage_nodes[2].is_offline = True
    await cluster_mgr.check_all_nodes()

    # Upload part 2: should still succeed because node-1 and node-2 satisfy W=2
    p2 = await engine.upload_part(mp.upload_id, 2, b"PART_2_AFTER_CRASH")
    assert p2.part_number == 2
    assert "node-3" not in p2.replica_nodes

    # Complete upload: should succeed and assemble object correctly
    committed = await engine.complete_multipart_upload(mp.upload_id)
    assert committed.size_bytes == len(b"PART_1_BEFORE_CRASH" + b"PART_2_AFTER_CRASH")

    # Read back assembled object
    rec, data = await engine.get_object(bucket="chaos", key="surviving_crash.dat")
    assert data == b"PART_1_BEFORE_CRASH" + b"PART_2_AFTER_CRASH"
