"""
Vault Cluster Rebalancing Engine.
Calculates replica placement deltas when nodes join or leave, executes rate-limited
migrations across nodes, updates metadata, and tracks migration progress.
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Set
import aiohttp

from vault.config import VaultConfig, RebalanceConfig
from vault.metadata import MetadataStore, ObjectRecord
from vault.cluster import ClusterManager
from vault.policy import QuorumPolicy
from vault.metrics import metrics

logger = logging.getLogger("vault.rebalance")


class RebalanceProgress:
    def __init__(self) -> None:
        self.state: str = "IDLE"  # IDLE, RUNNING, COMPLETED, FAILED
        self.start_time: float = 0.0
        self.end_time: Optional[float] = None
        self.objects_examined: int = 0
        self.migrations_planned: int = 0
        self.migrations_completed: int = 0
        self.bytes_transferred: int = 0
        self.current_object: Optional[str] = None
        self.error: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        now = self.end_time if self.end_time else time.time()
        elapsed = round(now - self.start_time, 2) if self.start_time > 0 else 0.0
        return {
            "state": self.state,
            "elapsed_seconds": elapsed,
            "objects_examined": self.objects_examined,
            "migrations_planned": self.migrations_planned,
            "migrations_completed": self.migrations_completed,
            "bytes_transferred": self.bytes_transferred,
            "current_object": self.current_object,
            "error": self.error
        }


class Rebalancer:
    def __init__(
        self,
        config: VaultConfig,
        metadata_store: MetadataStore,
        cluster_manager: ClusterManager
    ) -> None:
        self.config = config
        self.rebalance_cfg: RebalanceConfig = config.rebalance
        self.metadata = metadata_store
        self.cluster = cluster_manager
        self.policy = QuorumPolicy(config.quorum)
        self.progress = RebalanceProgress()
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(self.rebalance_cfg.max_concurrent_tasks)

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30.0))

    async def stop(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def run_rebalance(self) -> Dict[str, Any]:
        """Runs the cluster rebalancing process across all objects."""
        if self.progress.state == "RUNNING":
            return self.progress.to_json()

        self.progress = RebalanceProgress()
        self.progress.state = "RUNNING"
        self.progress.start_time = time.time()
        metrics.set_gauge("rebalance_active", 1.0)
        metrics.inc("rebalance_runs_total")

        try:
            objects = self.metadata.get_all_committed_objects()
            # First pass: plan migrations
            plan: List[Dict[str, Any]] = []
            healthy_nodes = self.cluster.get_healthy_node_ids()

            for obj in objects:
                self.progress.objects_examined += 1
                # Target placement according to current ring topology
                target_nodes = self.policy.compute_placement(
                    ring=self.cluster.ring,
                    key=f"{obj.bucket}/{obj.key}",
                    node_metadata={nid: s.config for nid, s in self.cluster.nodes.items()},
                    exclude_nodes=set(self.cluster.nodes.keys()) - healthy_nodes
                )

                current_nodes = set(obj.replica_nodes)
                needed_nodes = set(target_nodes) - current_nodes
                surplus_nodes = current_nodes - set(target_nodes)

                if needed_nodes:
                    plan.append({
                        "object": obj,
                        "needed": list(needed_nodes),
                        "surplus": list(surplus_nodes)
                    })

            self.progress.migrations_planned = len(plan)

            # Second pass: execute planned migrations with concurrency and rate limiting
            for item in plan:
                async with self._semaphore:
                    await self._migrate_object(item["object"], item["needed"], item["surplus"])
                    self.progress.migrations_completed += 1

            self.progress.state = "COMPLETED"
            self.progress.end_time = time.time()
            metrics.log_event("rebalance_completed", self.progress.to_json())
            return self.progress.to_json()

        except Exception as e:
            self.progress.state = "FAILED"
            self.progress.error = str(e)
            self.progress.end_time = time.time()
            logger.exception("Rebalance failed: %s", e)
            return self.progress.to_json()
        finally:
            metrics.set_gauge("rebalance_active", 0.0)

    async def _migrate_object(self, obj: ObjectRecord, needed_nodes: List[str], surplus_nodes: List[str]) -> None:
        self.progress.current_object = f"{obj.bucket}/{obj.key}"
        storage_key = f"{obj.bucket}/{obj.key}/{obj.version_id}.blob"

        # Identify healthy source donor
        donor_node_id = None
        for nid in obj.replica_nodes:
            if nid in self.cluster.get_healthy_node_ids():
                donor_node_id = nid
                break

        if not donor_node_id:
            logger.warning("No healthy donor found for rebalancing %s", storage_key)
            return

        donor_cfg = self.cluster.get_node_config(donor_node_id)
        if not donor_cfg:
            return

        successful_targets: List[str] = []
        for target_node_id in needed_nodes:
            target_cfg = self.cluster.get_node_config(target_node_id)
            if not target_cfg:
                continue

            replicate_url = f"{donor_cfg.url}/node/replicate"
            try:
                async with self._session.post(
                    replicate_url,
                    json={"storage_key": storage_key, "target_node_url": target_cfg.url}
                ) as resp:
                    if resp.status in (200, 201):
                        successful_targets.append(target_node_id)
                        self.progress.bytes_transferred += obj.size_bytes
                        metrics.inc("rebalance_bytes_transferred", obj.size_bytes)
                        
                        # Apply token-bucket rate limiting sleep
                        if self.rebalance_cfg.rate_limit_bytes_per_sec > 0 and obj.size_bytes > 0:
                            delay = obj.size_bytes / self.rebalance_cfg.rate_limit_bytes_per_sec
                            await asyncio.sleep(min(delay, 2.0))
            except Exception as e:
                logger.error("Failed migrating %s to %s: %s", storage_key, target_node_id, e)

        if successful_targets:
            new_replicas = list((set(obj.replica_nodes) | set(successful_targets)) - set(surplus_nodes))
            # Update metadata with new replica set
            self.metadata.update_object_replicas(obj.bucket, obj.key, obj.version_id, new_replicas)
            
            # Safely prune decommissioned/surplus replicas from old nodes
            for surplus_id in surplus_nodes:
                surplus_cfg = self.cluster.get_node_config(surplus_id)
                if surplus_cfg:
                    try:
                        async with self._session.delete(f"{surplus_cfg.url}/node/objects/{storage_key}") as resp:
                            if resp.status in (200, 204):
                                logger.info("Pruned surplus replica %s on node %s", storage_key, surplus_id)
                    except Exception:
                        pass
