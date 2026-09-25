"""
Tests for Cluster Rebalancing when nodes are added or removed.
"""

import pytest
from pathlib import Path
from vault.config import StorageNodeConfig
from vault.node import StorageNodeServer
from vault.rebalance import Rebalancer
from tests.conftest import find_free_port


@pytest.mark.asyncio
async def test_rebalancing_on_node_addition(cluster_env, tmp_path: Path):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    # 1. Write several objects across the initial 3 nodes
    num_objs = 15
    for i in range(num_objs):
        await engine.put_object(
            bucket="rebalance-test",
            key=f"item_{i}.dat",
            data=f"Data payload for item {i}".encode("utf-8")
        )

    # 2. Spin up a 4th storage node and join the cluster
    port4 = find_free_port()
    data_dir4 = tmp_path / "node_4"
    node4_cfg = StorageNodeConfig(
        node_id="node-4",
        host="127.0.0.1",
        port=port4,
        data_dir=data_dir4,
        rack="rack-2"
    )
    node4_server = StorageNodeServer(
        node_id=node4_cfg.node_id,
        host=node4_cfg.host,
        port=node4_cfg.port,
        data_dir=node4_cfg.data_dir,
        rack=node4_cfg.rack
    )
    await node4_server.start()
    storage_nodes.append(node4_server)

    cluster_mgr.register_node(node4_cfg)
    await cluster_mgr.check_all_nodes()

    # 3. Run Rebalancer
    rebalancer = Rebalancer(config, meta_store, cluster_mgr)
    await rebalancer.start()
    progress = await rebalancer.run_rebalance()

    assert progress["state"] == "COMPLETED"
    assert progress["objects_examined"] == num_objs
    assert progress["migrations_planned"] > 0
    assert progress["migrations_completed"] == progress["migrations_planned"]

    await rebalancer.stop()

    # 4. Verify all objects remain 100% accessible and verified
    for i in range(num_objs):
        rec, data = await engine.get_object(bucket="rebalance-test", key=f"item_{i}.dat")
        assert data == f"Data payload for item {i}".encode("utf-8")

    # Verify node-4 actually received some migrated replicas
    node4_blobs = list((node4_server.objects_dir).glob("**/*.blob"))
    assert len(node4_blobs) > 0
