"""
Vault Storage Engine (Coordinator).
Coordinates client requests, enforces write/read quorums, routes streams to storage nodes,
manages metadata transactions, and executes multipart operations.
"""

from __future__ import annotations
import asyncio
import hashlib
import io
import json
import logging
import time
import uuid
from typing import Optional, Dict, Any, List, Tuple, AsyncIterator, Union
import aiohttp

from vault.config import VaultConfig, StorageNodeConfig
from vault.metadata import MetadataStore, ObjectRecord, MultipartUploadRecord, PartRecord
from vault.policy import QuorumPolicy
from vault.cluster import ClusterManager, NodeStatus
from vault.metrics import metrics

logger = logging.getLogger("vault.engine")


class QuorumError(Exception):
    """Raised when an operation cannot satisfy its configured quorum."""
    pass


class PreconditionFailedError(Exception):
    """Raised when OCC checks (If-Match, If-None-Match) fail."""
    pass


class ObjectNotFoundError(Exception):
    """Raised when an object or version is not found."""
    pass


class ObjectUnrecoverableError(Exception):
    """Raised when an object exists in metadata but has no available healthy replicas."""
    pass


class VaultEngine:
    def __init__(
        self,
        config: VaultConfig,
        metadata_store: MetadataStore,
        cluster_manager: ClusterManager
    ) -> None:
        self.config = config
        self.metadata = metadata_store
        self.cluster = cluster_manager
        self.policy = QuorumPolicy(config.quorum)
        self._session: Optional[aiohttp.ClientSession] = None
        self._repair_queue: asyncio.Queue = asyncio.Queue()

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30.0, connect=3.0)
        self._session = aiohttp.ClientSession(timeout=timeout)

    async def stop(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def _get_node_url(self, node_id: str) -> Optional[str]:
        cfg = self.cluster.get_node_config(node_id)
        return cfg.url if cfg else None

    # --- Bucket Operations ---
    def create_bucket(self, name: str) -> bool:
        created = self.metadata.create_bucket(name)
        if created:
            metrics.inc("buckets_created")
        return created

    def list_buckets(self) -> List[str]:
        return self.metadata.list_buckets()

    # --- Object Operations ---
    async def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        custom_metadata: Optional[Dict[str, str]] = None,
        expected_etag: Optional[str] = None,
        if_none_match: bool = False
    ) -> ObjectRecord:
        """
        Write an object with full quorum durability.
        Payload is streamed concurrently to candidate storage nodes.
        Requires at least W successful node writes to commit.
        """
        start_t = time.time()
        metrics.inc("requests_put_total")
        custom_metadata = custom_metadata or {}

        # Auto-create bucket if missing
        if not self.metadata.bucket_exists(bucket):
            self.metadata.create_bucket(bucket)

        # Compute SHA-256 and ETag
        size_bytes = len(data)
        sha256 = hashlib.sha256(data).hexdigest()
        etag = f'"{sha256[:32]}"'
        version_id = uuid.uuid4().hex[:16]

        # Determine target nodes from ring & rack topology
        healthy_nodes = self.cluster.get_healthy_node_ids()
        candidate_nodes = self.policy.compute_placement(
            ring=self.cluster.ring,
            key=f"{bucket}/{key}",
            node_metadata={nid: state.config for nid, state in self.cluster.nodes.items()},
            exclude_nodes=set(self.cluster.nodes.keys()) - healthy_nodes if len(healthy_nodes) >= self.policy.W else None
        )

        if len(candidate_nodes) < self.policy.W:
            metrics.inc("requests_put_quorum_failed")
            raise QuorumError(
                f"Insufficient healthy nodes to satisfy write quorum W={self.policy.W}. "
                f"Available candidate nodes: {candidate_nodes}"
            )

        storage_key = f"{bucket}/{key}/{version_id}.blob"

        # Prepare pending metadata entry
        self.metadata.prepare_object(
            bucket=bucket,
            key=key,
            size_bytes=size_bytes,
            sha256=sha256,
            etag=etag,
            content_type=content_type,
            custom_metadata=custom_metadata,
            replica_nodes=candidate_nodes,
            version_id=version_id
        )

        # Upload concurrently to candidate nodes
        async def _upload_to_node(node_id: str) -> Optional[str]:
            url = self._get_node_url(node_id)
            if not url:
                return None
            target_url = f"{url}/node/objects/{storage_key}"
            headers = {"X-Expected-SHA256": sha256}
            try:
                async with self._session.put(target_url, data=data, headers=headers) as resp:
                    if resp.status in (200, 201):
                        return node_id
                    else:
                        logger.warning("Node %s rejected write with status %s", node_id, resp.status)
                        return None
            except Exception as e:
                logger.warning("Failed write to node %s: %s", node_id, e)
                return None

        tasks = [_upload_to_node(nid) for nid in candidate_nodes]
        results = await asyncio.gather(*tasks)
        successful_replicas = [nid for nid in results if nid is not None]

        # Evaluate Write Quorum
        if not self.policy.check_write_success(len(successful_replicas)):
            # Rollback staged files asynchronously
            self.metadata.abort_object(bucket, key, version_id)
            metrics.inc("requests_put_quorum_failed")
            asyncio.create_task(self._cleanup_partial_replicas(successful_replicas, storage_key))
            raise QuorumError(
                f"Write quorum not satisfied. Required W={self.policy.W}, achieved={len(successful_replicas)} "
                f"on nodes: {successful_replicas}"
            )

        # Atomically commit metadata
        try:
            committed_record = self.metadata.commit_object(
                bucket=bucket,
                key=key,
                version_id=version_id,
                expected_etag=expected_etag,
                if_none_match=if_none_match
            )
        except ValueError as e:
            self.metadata.abort_object(bucket, key, version_id)
            asyncio.create_task(self._cleanup_partial_replicas(successful_replicas, storage_key))
            metrics.inc("requests_put_precondition_failed")
            raise PreconditionFailedError(str(e))

        if not committed_record:
            raise RuntimeError("Failed to commit object metadata")

        # Update actual replica nodes in metadata
        self.metadata.update_object_replicas(bucket, key, version_id, successful_replicas)
        committed_record.replica_nodes = successful_replicas

        # If we wrote to >= W but < N, queue background repair to reach target N
        if len(successful_replicas) < self.policy.N:
            all_target_nodes = self.policy.compute_placement(
                ring=self.cluster.ring,
                key=f"{bucket}/{key}",
                node_metadata={nid: state.config for nid, state in self.cluster.nodes.items()}
            )
            missing = [n for n in all_target_nodes if n not in successful_replicas]
            if missing:
                self._repair_queue.put_nowait({
                    "action": "replicate_to_target",
                    "bucket": bucket,
                    "key": key,
                    "version_id": version_id,
                    "storage_key": storage_key,
                    "source_node": successful_replicas[0],
                    "target_nodes": missing
                })

        metrics.inc("requests_put_success")
        metrics.inc("bytes_written", size_bytes)
        metrics.observe_latency("put", time.time() - start_t)
        return committed_record

    async def get_object(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None
    ) -> Tuple[ObjectRecord, bytes]:
        """
        Fetch object data with checksum verification and inline read-repair.
        If a replica is corrupted or unreachable, falls back to remaining replicas.
        """
        start_t = time.time()
        metrics.inc("requests_get_total")

        record = self.metadata.get_object(bucket, key, version_id=version_id)
        if not record:
            metrics.inc("requests_get_not_found")
            raise ObjectNotFoundError(f"Object {bucket}/{key} not found")

        storage_key = f"{bucket}/{key}/{record.version_id}.blob"
        healthy_nodes = self.cluster.get_healthy_node_ids()

        # Sort candidate nodes: healthy first
        nodes_to_try = sorted(
            record.replica_nodes,
            key=lambda nid: 0 if nid in healthy_nodes else 1
        )

        headers = {}
        if range_start is not None or range_end is not None:
            r_start = range_start if range_start is not None else ""
            r_end = range_end if range_end is not None else ""
            headers["Range"] = f"bytes={r_start}-{r_end}"

        corrupted_nodes: List[str] = []
        payload_data: Optional[bytes] = None
        successful_source_node: Optional[str] = None

        for node_id in nodes_to_try:
            url = self._get_node_url(node_id)
            if not url:
                continue

            target_url = f"{url}/node/objects/{storage_key}"
            try:
                async with self._session.get(target_url, headers=headers) as resp:
                    if resp.status in (200, 206):
                        data = await resp.read()
                        
                        # Verify checksum if full content was requested
                        if range_start is None and range_end is None:
                            calc_sha = hashlib.sha256(data).hexdigest()
                            if calc_sha.lower() != record.sha256.lower():
                                logger.error(
                                    "Bitrot/corruption detected on node %s for %s! Expected: %s, Actual: %s",
                                    node_id, storage_key, record.sha256, calc_sha
                                )
                                metrics.inc("corruptions_detected")
                                metrics.log_event("bitrot_detected", {
                                    "node_id": node_id,
                                    "bucket": bucket,
                                    "key": key,
                                    "version_id": record.version_id
                                })
                                corrupted_nodes.append(node_id)
                                continue

                        if payload_data is None:
                            payload_data = data
                            successful_source_node = node_id
                    elif resp.status == 404:
                        logger.warning("Replica missing on node %s for %s", node_id, storage_key)
                        corrupted_nodes.append(node_id)
            except Exception as e:
                logger.warning("Failed read from node %s: %s", node_id, e)
                continue

        if payload_data is None:
            metrics.inc("requests_get_unrecoverable")
            raise ObjectUnrecoverableError(
                f"All replicas for object {bucket}/{key} failed read or were corrupted. "
                f"Replica nodes tried: {nodes_to_try}"
            )

        # Trigger inline Read Repair if any corrupted/missing replica was detected
        if corrupted_nodes and successful_source_node:
            logger.info("Executing inline Read Repair for corrupted nodes: %s", corrupted_nodes)
            metrics.inc("read_repairs_triggered")
            await self._repair_replicas(
                bucket=bucket,
                key=key,
                version_id=record.version_id,
                source_node=successful_source_node,
                target_nodes=corrupted_nodes,
                expected_sha256=record.sha256
            )

        metrics.inc("requests_get_success")
        metrics.inc("bytes_read", len(payload_data))
        metrics.observe_latency("get", time.time() - start_t)
        return record, payload_data

    async def head_object(self, bucket: str, key: str, version_id: Optional[str] = None) -> ObjectRecord:
        metrics.inc("requests_head_total")
        rec = self.metadata.get_object(bucket, key, version_id=version_id)
        if not rec:
            raise ObjectNotFoundError(f"Object {bucket}/{key} not found")
        metrics.inc("requests_head_success")
        return rec

    async def delete_object(self, bucket: str, key: str) -> None:
        """Tombstones metadata and deletes replicas across storage nodes."""
        metrics.inc("requests_delete_total")
        rec = self.metadata.get_object(bucket, key)
        if not rec:
            raise ObjectNotFoundError(f"Object {bucket}/{key} not found")

        version_id = self.metadata.delete_object(bucket, key)
        storage_key = f"{bucket}/{key}/{rec.version_id}.blob"

        # Fan out delete calls to replica nodes
        async def _delete_from_node(node_id: str) -> bool:
            url = self._get_node_url(node_id)
            if not url:
                return False
            try:
                async with self._session.delete(f"{url}/node/objects/{storage_key}") as resp:
                    return resp.status in (200, 204, 404)
            except Exception:
                return False

        tasks = [_delete_from_node(nid) for nid in rec.replica_nodes]
        await asyncio.gather(*tasks)
        metrics.inc("requests_delete_success")

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: Optional[str] = None,
        marker: Optional[str] = None,
        limit: int = 100
    ) -> Tuple[List[ObjectRecord], List[str], Optional[str]]:
        metrics.inc("requests_list_total")
        return self.metadata.list_objects(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            marker=marker,
            limit=limit
        )

    # --- Multipart Upload Operations ---
    def initiate_multipart_upload(
        self,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        custom_metadata: Optional[Dict[str, str]] = None
    ) -> MultipartUploadRecord:
        metrics.inc("multipart_initiated")
        if not self.metadata.bucket_exists(bucket):
            self.metadata.create_bucket(bucket)
        return self.metadata.create_multipart_upload(
            bucket=bucket,
            key=key,
            content_type=content_type,
            custom_metadata=custom_metadata
        )

    async def upload_part(
        self,
        upload_id: str,
        part_number: int,
        data: bytes
    ) -> PartRecord:
        """Uploads a single multipart chunk to quorum storage nodes."""
        start_t = time.time()
        metrics.inc("multipart_parts_uploaded")

        upload_rec = self.metadata.get_multipart_upload(upload_id)
        if not upload_rec or upload_rec.state != "INITIATED":
            raise ValueError(f"Multipart upload {upload_id} not active")

        size_bytes = len(data)
        sha256 = hashlib.sha256(data).hexdigest()
        etag = f'"{sha256[:32]}"'

        # Part storage key
        storage_key = f"multipart/{upload_id}/part_{part_number}.blob"

        candidate_nodes = self.policy.compute_placement(
            ring=self.cluster.ring,
            key=f"{upload_rec.bucket}/{upload_rec.key}/part_{part_number}",
            node_metadata={nid: state.config for nid, state in self.cluster.nodes.items()}
        )

        async def _upload_part_node(node_id: str) -> Optional[str]:
            url = self._get_node_url(node_id)
            if not url:
                return None
            headers = {"X-Expected-SHA256": sha256}
            try:
                async with self._session.put(f"{url}/node/objects/{storage_key}", data=data, headers=headers) as resp:
                    if resp.status in (200, 201):
                        return node_id
            except Exception as e:
                logger.warning("Error uploading part to node %s: %s", node_id, e)
            return None

        tasks = [_upload_part_node(nid) for nid in candidate_nodes]
        results = await asyncio.gather(*tasks)
        successful_replicas = [nid for nid in results if nid is not None]

        if not self.policy.check_write_success(len(successful_replicas)):
            raise QuorumError(f"Failed to achieve quorum write for part {part_number}")

        part_rec = self.metadata.record_part(
            upload_id=upload_id,
            part_number=part_number,
            size_bytes=size_bytes,
            sha256=sha256,
            etag=etag,
            replica_nodes=successful_replicas
        )
        return part_rec

    async def complete_multipart_upload(self, upload_id: str) -> ObjectRecord:
        """
        Validates all contiguous parts, stitches them into a final object blob,
        and atomically commits object metadata.
        """
        start_t = time.time()
        upload_rec = self.metadata.get_multipart_upload(upload_id)
        if not upload_rec or upload_rec.state != "INITIATED":
            raise ValueError(f"Multipart upload {upload_id} not active")

        parts = self.metadata.list_parts(upload_id)
        if not parts:
            raise ValueError("Cannot complete multipart upload with zero parts")

        # Verify contiguous parts 1..N
        for idx, part in enumerate(parts, start=1):
            if part.part_number != idx:
                raise ValueError(f"Missing part number {idx} in multipart sequence")

        # Stream all parts from replicas and assemble the complete payload
        assembled_chunks = []
        for part in parts:
            # Read part from first available replica
            part_storage_key = f"multipart/{upload_id}/part_{part.part_number}.blob"
            part_data = None
            for nid in part.replica_nodes:
                url = self._get_node_url(nid)
                if not url:
                    continue
                try:
                    async with self._session.get(f"{url}/node/objects/{part_storage_key}") as resp:
                        if resp.status == 200:
                            part_data = await resp.read()
                            break
                except Exception:
                    continue
            if part_data is None:
                raise ObjectUnrecoverableError(f"Failed to read part {part.part_number} for assembly")
            assembled_chunks.append(part_data)

        full_data = b"".join(assembled_chunks)

        # Write assembled object via standard put_object
        committed_record = await self.put_object(
            bucket=upload_rec.bucket,
            key=upload_rec.key,
            data=full_data,
            content_type=upload_rec.content_type,
            custom_metadata=upload_rec.custom_metadata
        )

        # Mark multipart completed and clean up part blobs
        self.metadata.complete_multipart_upload(upload_id)
        asyncio.create_task(self._cleanup_multipart_parts(upload_id, parts))
        metrics.inc("multipart_completed")
        return committed_record

    async def abort_multipart_upload(self, upload_id: str) -> None:
        upload_rec = self.metadata.get_multipart_upload(upload_id)
        if not upload_rec:
            return
        parts = self.metadata.list_parts(upload_id)
        self.metadata.abort_multipart_upload(upload_id)
        asyncio.create_task(self._cleanup_multipart_parts(upload_id, parts))
        metrics.inc("multipart_aborted")

    # --- Background Repair Helpers ---
    async def _repair_replicas(
        self,
        bucket: str,
        key: str,
        version_id: str,
        source_node: str,
        target_nodes: List[str],
        expected_sha256: str
    ) -> None:
        """Copies verified object payload from source_node to target_nodes."""
        source_url = self._get_node_url(source_node)
        if not source_url:
            return

        storage_key = f"{bucket}/{key}/{version_id}.blob"
        for target_node in target_nodes:
            target_url = self._get_node_url(target_node)
            if not target_url:
                continue

            try:
                # Trigger node-to-node replication
                async with self._session.post(
                    f"{source_url}/node/replicate",
                    json={"storage_key": storage_key, "target_node_url": target_url}
                ) as resp:
                    if resp.status in (200, 201):
                        logger.info("Successfully repaired %s to node %s", storage_key, target_node)
                        metrics.inc("repairs_completed")
                        metrics.log_event("repair_success", {
                            "bucket": bucket,
                            "key": key,
                            "target_node": target_node
                        })
                    else:
                        logger.warning("Node replication failed to %s: HTTP %s", target_node, resp.status)
            except Exception as e:
                logger.error("Failed to repair replica to %s: %s", target_node, e)

        # Update metadata with current healthy replica list
        rec = self.metadata.get_object(bucket, key, version_id=version_id)
        if rec:
            all_replicas = set(rec.replica_nodes).union(set(target_nodes))
            self.metadata.update_object_replicas(bucket, key, version_id, list(all_replicas))

    async def _cleanup_partial_replicas(self, node_ids: List[str], storage_key: str) -> None:
        for nid in node_ids:
            url = self._get_node_url(nid)
            if url:
                try:
                    await self._session.delete(f"{url}/node/objects/{storage_key}")
                except Exception:
                    pass

    async def _cleanup_multipart_parts(self, upload_id: str, parts: List[PartRecord]) -> None:
        for part in parts:
            part_key = f"multipart/{upload_id}/part_{part.part_number}.blob"
            for nid in part.replica_nodes:
                url = self._get_node_url(nid)
                if url:
                    try:
                        await self._session.delete(f"{url}/node/objects/{part_key}")
                    except Exception:
                        pass
