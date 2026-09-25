"""
Shared pytest fixtures for Vault distributed object storage tests.
"""

from __future__ import annotations
import asyncio
import socket
from pathlib import Path
from typing import AsyncGenerator, Tuple, List
import pytest
import aiohttp

from vault.config import VaultConfig, StorageNodeConfig, QuorumConfig
from vault.node import StorageNodeServer
from vault.metadata import MetadataStore
from vault.cluster import ClusterManager
from vault.engine import VaultEngine
from vault.repair import ActiveScrubber
from vault.rebalance import Rebalancer


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture
async def cluster_env(tmp_path: Path) -> AsyncGenerator[Tuple[VaultConfig, VaultEngine, List[StorageNodeServer], MetadataStore, ClusterManager], None]:
    """Spins up a 3-node in-process cluster with isolated storage directories."""
    num_nodes = 3
    node_configs: List[StorageNodeConfig] = []
    storage_nodes: List[StorageNodeServer] = []

    for i in range(1, num_nodes + 1):
        port = find_free_port()
        data_dir = tmp_path / f"node_{i}"
        cfg = StorageNodeConfig(
            node_id=f"node-{i}",
            host="127.0.0.1",
            port=port,
            data_dir=data_dir,
            rack=f"rack-{(i % 2) + 1}",
            zone="zone-test"
        )
        node_configs.append(cfg)
        server = StorageNodeServer(
            node_id=cfg.node_id,
            host=cfg.host,
            port=cfg.port,
            data_dir=cfg.data_dir,
            rack=cfg.rack,
            zone=cfg.zone
        )
        await server.start()
        storage_nodes.append(server)

    config = VaultConfig(
        cluster_name="test-vault",
        base_dir=tmp_path,
        nodes=node_configs,
        quorum=QuorumConfig(replication_factor=3, write_quorum=2, read_quorum=2),
        heartbeat_interval_sec=0.5,
        heartbeat_timeout_sec=0.5
    )

    meta_store = MetadataStore(tmp_path / "metadata.db")
    cluster_mgr = ClusterManager(config)
    engine = VaultEngine(config, meta_store, cluster_mgr)

    await cluster_mgr.start()
    await engine.start()

    # Initial check
    await cluster_mgr.check_all_nodes()

    yield config, engine, storage_nodes, meta_store, cluster_mgr

    await engine.stop()
    await cluster_mgr.stop()
    for s in storage_nodes:
        await s.stop()
