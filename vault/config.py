"""
Vault Configuration Module.
Defines system parameters, quorum policies, node topologies, and runtime settings.
"""

from __future__ import annotations
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class StorageNodeConfig(BaseModel):
    node_id: str
    host: str = "127.0.0.1"
    port: int
    data_dir: Path
    rack: str = "rack-1"
    zone: str = "zone-1"
    weight: int = 1

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


class QuorumConfig(BaseModel):
    replication_factor: int = Field(default=3, ge=1, description="Total number of replicas (N)")
    write_quorum: int = Field(default=2, ge=1, description="Write acknowledgments required (W)")
    read_quorum: int = Field(default=2, ge=1, description="Read acknowledgments queried (R)")

    def validate_durability(self) -> None:
        if self.write_quorum > self.replication_factor:
            raise ValueError(f"Write quorum W ({self.write_quorum}) cannot exceed N ({self.replication_factor})")
        if self.read_quorum > self.replication_factor:
            raise ValueError(f"Read quorum R ({self.read_quorum}) cannot exceed N ({self.replication_factor})")
        # Strong consistency condition
        if (self.read_quorum + self.write_quorum) <= self.replication_factor:
            # We allow it, but flag as weak consistency
            pass


class RebalanceConfig(BaseModel):
    rate_limit_bytes_per_sec: int = 10 * 1024 * 1024  # 10 MB/s
    max_concurrent_tasks: int = 4
    poll_interval_sec: float = 10.0


class ScrubConfig(BaseModel):
    enabled: bool = True
    interval_sec: float = 120.0  # Background scrub interval
    batch_size: int = 50


class VaultConfig(BaseModel):
    cluster_name: str = "vault-cluster"
    base_dir: Path = Path("./vault_data").resolve()
    gateway_host: str = "0.0.0.0"
    gateway_port: int = 8000
    metadata_db_name: str = "metadata.db"
    
    quorum: QuorumConfig = Field(default_factory=QuorumConfig)
    rebalance: RebalanceConfig = Field(default_factory=RebalanceConfig)
    scrub: ScrubConfig = Field(default_factory=ScrubConfig)
    
    nodes: List[StorageNodeConfig] = Field(default_factory=list)
    
    # Network & Heartbeat
    heartbeat_interval_sec: float = 2.0
    heartbeat_timeout_sec: float = 1.5
    suspect_threshold_misses: int = 2
    dead_threshold_misses: int = 5
    
    # Transfer settings
    stream_chunk_size: int = 64 * 1024  # 64 KB
    min_part_size: int = 64 * 1024      # 64 KB minimum for multipart tests

    @classmethod
    def default_cluster(cls, base_dir: Optional[Path] = None, num_nodes: int = 3) -> VaultConfig:
        root = (base_dir or Path("./vault_data")).resolve()
        nodes = []
        for i in range(1, num_nodes + 1):
            nodes.append(
                StorageNodeConfig(
                    node_id=f"node-{i}",
                    host="127.0.0.1",
                    port=9000 + i,
                    data_dir=root / f"node_{i}",
                    rack=f"rack-{(i % 2) + 1}",
                    zone="zone-us-east"
                )
            )
        return cls(
            base_dir=root,
            nodes=nodes,
            quorum=QuorumConfig(replication_factor=num_nodes, write_quorum=(num_nodes // 2) + 1, read_quorum=(num_nodes // 2) + 1)
        )
