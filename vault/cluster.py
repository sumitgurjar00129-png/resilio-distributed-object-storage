"""
Vault Cluster Manager & Node Failure Detector.
Maintains cluster topology, monitors node health via periodic heartbeats,
tracks failure states (HEALTHY, SUSPECT, DEAD), and updates the hash ring.
"""

from __future__ import annotations
import asyncio
import enum
import logging
import time
from typing import Dict, Set, Optional, List
import aiohttp

from vault.config import VaultConfig, StorageNodeConfig
from vault.hashing import ConsistentHashRing
from vault.metrics import metrics

logger = logging.getLogger("vault.cluster")


class NodeStatus(str, enum.Enum):
    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"


class NodeState:
    def __init__(self, config: StorageNodeConfig) -> None:
        self.config = config
        self.status: NodeStatus = NodeStatus.HEALTHY
        self.last_heartbeat_time: float = time.time()
        self.consecutive_failures: int = 0
        self.last_error: Optional[str] = None
        self.disk_info: Dict[str, int] = {}
        self.uptime: float = 0.0


class ClusterManager:
    def __init__(self, config: VaultConfig) -> None:
        self.config = config
        self.nodes: Dict[str, NodeState] = {}
        self.ring = ConsistentHashRing(vnodes=128)
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        
        # Populate initial nodes
        for node_cfg in config.nodes:
            self.register_node(node_cfg)

    def register_node(self, node_cfg: StorageNodeConfig) -> None:
        """Add node to cluster topology and hash ring."""
        if node_cfg.node_id not in self.nodes:
            self.nodes[node_cfg.node_id] = NodeState(node_cfg)
            self.ring.add_node(node_cfg.node_id, weight=node_cfg.weight)
            metrics.log_event("node_registered", {"node_id": node_cfg.node_id, "url": node_cfg.url})
            logger.info("Registered storage node %s at %s", node_cfg.node_id, node_cfg.url)

    def deregister_node(self, node_id: str) -> bool:
        """Remove node from cluster topology and hash ring."""
        if node_id in self.nodes:
            del self.nodes[node_id]
            self.ring.remove_node(node_id)
            metrics.log_event("node_deregistered", {"node_id": node_id})
            logger.info("Deregistered storage node %s", node_id)
            return True
        return False

    def get_node_config(self, node_id: str) -> Optional[StorageNodeConfig]:
        state = self.nodes.get(node_id)
        return state.config if state else None

    def get_healthy_node_ids(self) -> Set[str]:
        return {n for n, s in self.nodes.items() if s.status == NodeStatus.HEALTHY}

    def get_active_node_ids(self) -> Set[str]:
        """Returns HEALTHY and SUSPECT nodes (excluding DEAD)."""
        return {n for n, s in self.nodes.items() if s.status != NodeStatus.DEAD}

    def get_all_node_states(self) -> Dict[str, Dict]:
        result = {}
        for nid, state in self.nodes.items():
            result[nid] = {
                "node_id": nid,
                "url": state.config.url,
                "rack": state.config.rack,
                "zone": state.config.zone,
                "status": state.status.value,
                "consecutive_failures": state.consecutive_failures,
                "last_heartbeat_time": state.last_heartbeat_time,
                "last_error": state.last_error,
                "disk": state.disk_info
            }
        return result

    async def start(self) -> None:
        self._running = True
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.heartbeat_timeout_sec)
        )
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("Cluster failure detector started.")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
        logger.info("Cluster failure detector stopped.")

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                await self.check_all_nodes()
            except Exception as e:
                logger.exception("Error during cluster heartbeat check: %s", e)
            await asyncio.sleep(self.config.heartbeat_interval_sec)

    async def check_all_nodes(self) -> None:
        if not self._session or self._session.closed:
            return

        tasks = [self._ping_node(nid, state) for nid, state in self.nodes.items()]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Update metrics gauges
        healthy_count = sum(1 for s in self.nodes.values() if s.status == NodeStatus.HEALTHY)
        suspect_count = sum(1 for s in self.nodes.values() if s.status == NodeStatus.SUSPECT)
        dead_count = sum(1 for s in self.nodes.values() if s.status == NodeStatus.DEAD)

        metrics.set_gauge("nodes_healthy", healthy_count)
        metrics.set_gauge("nodes_suspect", suspect_count)
        metrics.set_gauge("nodes_dead", dead_count)
        metrics.set_gauge("nodes_total", len(self.nodes))

    async def _ping_node(self, node_id: str, state: NodeState) -> None:
        url = f"{state.config.url}/node/heartbeat"
        prev_status = state.status
        try:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    state.last_heartbeat_time = time.time()
                    state.consecutive_failures = 0
                    state.last_error = None
                    state.disk_info = data.get("disk", {})
                    state.uptime = data.get("uptime_seconds", 0.0)
                    state.status = NodeStatus.HEALTHY
                    if prev_status != NodeStatus.HEALTHY:
                        logger.info("Node %s recovered to HEALTHY", node_id)
                        metrics.log_event("node_recovered", {"node_id": node_id})
                    return
                else:
                    raise RuntimeError(f"HTTP {resp.status}")
        except Exception as e:
            state.consecutive_failures += 1
            state.last_error = str(e)
            
            if state.consecutive_failures >= self.config.dead_threshold_misses:
                state.status = NodeStatus.DEAD
                if prev_status != NodeStatus.DEAD:
                    logger.warning("Node %s marked DEAD after %d misses: %s", node_id, state.consecutive_failures, e)
                    metrics.log_event("node_dead", {"node_id": node_id, "error": str(e)})
            elif state.consecutive_failures >= self.config.suspect_threshold_misses:
                state.status = NodeStatus.SUSPECT
                if prev_status != NodeStatus.SUSPECT:
                    logger.warning("Node %s marked SUSPECT after %d misses: %s", node_id, state.consecutive_failures, e)
                    metrics.log_event("node_suspect", {"node_id": node_id, "error": str(e)})
