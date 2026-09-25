"""
Tests for Node Failure, Unreachable Nodes, Network Partitions, and Quorum Degradation.
"""

import pytest
from vault.cluster import NodeStatus
from vault.engine import QuorumError


@pytest.mark.asyncio
async def test_single_node_failure_write_and_read_available(cluster_env):
    """
    With N=3, W=2, R=2:
    When 1 node fails, 2 nodes remain healthy.
    Writes and reads MUST continue succeeding!
    """
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    # 1. Take node-1 offline
    storage_nodes[0].is_offline = True
    await cluster_mgr.check_all_nodes()

    # Verify node-1 is not healthy
    node1_state = cluster_mgr.nodes["node-1"]
    assert node1_state.consecutive_failures > 0

    # 2. Write object: should still succeed with W=2 across node-2 and node-3
    payload = b"Surviving node-1 failure!"
    record = await engine.put_object(
        bucket="resilience",
        key="survivor.txt",
        data=payload
    )
    assert "node-1" not in record.replica_nodes
    assert len(record.replica_nodes) >= 2

    # 3. Read object: should succeed from remaining replicas
    rec, data = await engine.get_object(bucket="resilience", key="survivor.txt")
    assert data == payload


@pytest.mark.asyncio
async def test_quorum_loss_rejects_writes(cluster_env):
    """
    With N=3, W=2:
    When 2 nodes fail, only 1 node remains.
    Write quorum cannot be met, so writes MUST fail and not produce phantom commits.
    """
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    # Take node-1 and node-2 offline
    storage_nodes[0].is_offline = True
    storage_nodes[1].is_offline = True
    await cluster_mgr.check_all_nodes()

    with pytest.raises(QuorumError, match="Insufficient healthy nodes|Write quorum not satisfied"):
        await engine.put_object(
            bucket="resilience",
            key="should-fail.txt",
            data=b"Data that should not commit"
        )

    # Verify nothing committed to metadata
    assert meta_store.get_object("resilience", "should-fail.txt") is None
