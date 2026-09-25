"""
Vault Data Integrity Scrubber & Auto-Repair Worker.
Performs background scanning across all storage nodes, detects bitrot/corruption and missing replicas,
and automatically restores full replication from healthy copies.
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
import aiohttp

from vault.config import VaultConfig
from vault.metadata import MetadataStore, ObjectRecord
from vault.cluster import ClusterManager
from vault.metrics import metrics

logger = logging.getLogger("vault.repair")


class ScrubReport:
    def __init__(self) -> None:
        self.start_time: float = time.time()
        self.end_time: Optional[float] = None
        self.objects_scanned: int = 0
        self.replicas_checked: int = 0
        self.healthy_replicas: int = 0
        self.corrupted_replicas: int = 0
        self.missing_replicas: int = 0
        self.repaired_replicas: int = 0
        self.unrecoverable_objects: List[str] = []

    def finalize(self) -> Dict[str, Any]:
        self.end_time = time.time()
        return {
            "duration_seconds": round(self.end_time - self.start_time, 2),
            "objects_scanned": self.objects_scanned,
            "replicas_checked": self.replicas_checked,
            "healthy_replicas": self.healthy_replicas,
            "corrupted_replicas": self.corrupted_replicas,
            "missing_replicas": self.missing_replicas,
            "repaired_replicas": self.repaired_replicas,
            "unrecoverable_count": len(self.unrecoverable_objects),
            "unrecoverable_objects": self.unrecoverable_objects
        }


class ActiveScrubber:
    def __init__(
        self,
        config: VaultConfig,
        metadata_store: MetadataStore,
        cluster_manager: ClusterManager
    ) -> None:
        self.config = config
        self.metadata = metadata_store
        self.cluster = cluster_manager
        self.is_scrubbing = False
        self.last_report: Optional[Dict[str, Any]] = None
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10.0))
        if self.config.scrub.enabled:
            self._task = asyncio.create_task(self._periodic_scrub_loop())
            logger.info("Background scrubber worker initialized.")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()

    async def _periodic_scrub_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.scrub.interval_sec)
                await self.run_scrub()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error during periodic scrub: %s", e)

    async def run_scrub(self) -> Dict[str, Any]:
        """Runs a complete integrity audit and repairs degraded objects."""
        if self.is_scrubbing:
            return {"status": "in_progress", "message": "Scrub is already running"}

        self.is_scrubbing = True
        report = ScrubReport()
        metrics.inc("scrub_runs_total")

        try:
            objects = self.metadata.get_all_committed_objects()
            for obj in objects:
                await self._audit_and_heal_object(obj, report)
                report.objects_scanned += 1
                await asyncio.sleep(0.01)  # Yield control to prevent event loop starvation

            self.last_report = report.finalize()
            metrics.inc("scrub_corruptions_detected", report.corrupted_replicas)
            metrics.inc("scrub_repairs_completed", report.repaired_replicas)
            metrics.log_event("scrub_completed", self.last_report)
            return self.last_report
        finally:
            self.is_scrubbing = False

    async def _audit_and_heal_object(self, obj: ObjectRecord, report: ScrubReport) -> None:
        storage_key = f"{obj.bucket}/{obj.key}/{obj.version_id}.blob"
        healthy_nodes: List[str] = []
        corrupted_or_missing_nodes: List[str] = []

        # Check all candidate replicas for this object
        for node_id in obj.replica_nodes:
            report.replicas_checked += 1
            node_cfg = self.cluster.get_node_config(node_id)
            if not node_cfg:
                report.missing_replicas += 1
                corrupted_or_missing_nodes.append(node_id)
                continue

            verify_url = f"{node_cfg.url}/node/verify/{storage_key}"
            try:
                async with self._session.post(
                    verify_url,
                    json={"expected_sha256": obj.sha256}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("healthy"):
                            healthy_nodes.append(node_id)
                            report.healthy_replicas += 1
                        else:
                            report.corrupted_replicas += 1
                            corrupted_or_missing_nodes.append(node_id)
                            logger.warning(
                                "Scrubber detected bitrot on %s for %s! Actual: %s, Expected: %s",
                                node_id, storage_key, data.get("actual_sha256"), obj.sha256
                            )
                    elif resp.status == 404:
                        report.missing_replicas += 1
                        corrupted_or_missing_nodes.append(node_id)
                    else:
                        corrupted_or_missing_nodes.append(node_id)
            except Exception as e:
                logger.debug("Failed verifying %s on %s: %s", storage_key, node_id, e)
                corrupted_or_missing_nodes.append(node_id)

        # Evaluate health and heal if needed
        if not healthy_nodes:
            report.unrecoverable_objects.append(f"{obj.bucket}/{obj.key}:{obj.version_id}")
            metrics.inc("objects_unrecoverable")
            logger.critical("CRITICAL: Object %s has 0 healthy replicas!", storage_key)
            return

        # If there are corrupted or missing replicas, heal them from a healthy replica
        if corrupted_or_missing_nodes:
            donor_node_id = healthy_nodes[0]
            donor_cfg = self.cluster.get_node_config(donor_node_id)
            if not donor_cfg:
                return

            for broken_node_id in corrupted_or_missing_nodes:
                target_cfg = self.cluster.get_node_config(broken_node_id)
                if not target_cfg:
                    continue

                replicate_url = f"{donor_cfg.url}/node/replicate"
                try:
                    async with self._session.post(
                        replicate_url,
                        json={"storage_key": storage_key, "target_node_url": target_cfg.url}
                    ) as resp:
                        if resp.status in (200, 201):
                            report.repaired_replicas += 1
                            logger.info("Scrubber restored replica %s on node %s", storage_key, broken_node_id)
                except Exception as e:
                    logger.error("Scrubber failed healing %s to %s: %s", storage_key, broken_node_id, e)
