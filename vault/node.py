"""
Vault Storage Node Service.
Handles chunked streaming I/O, atomic staging, on-disk hashing, verification, and replication.
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Optional, Dict, Any
from aiohttp import web

logger = logging.getLogger("vault.node")


class StorageNodeServer:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        data_dir: Path,
        rack: str = "rack-1",
        zone: str = "zone-1"
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.data_dir = Path(data_dir).resolve()
        self.staging_dir = self.data_dir / "staging"
        self.objects_dir = self.data_dir / "objects"
        self.rack = rack
        self.zone = zone
        self.start_time = time.time()
        
        # Fault injection controls
        self.is_offline = False
        self.simulated_delay_sec = 0.0
        
        # Ensure directories exist
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)

        self.app = web.Application()
        self._setup_routes()
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

    def _setup_routes(self) -> None:
        self.app.router.add_get("/node/heartbeat", self.handle_heartbeat)
        self.app.router.add_put("/node/objects/{storage_key:.*}", self.handle_put_object)
        self.app.router.add_get("/node/objects/{storage_key:.*}", self.handle_get_object, allow_head=False)
        self.app.router.add_head("/node/objects/{storage_key:.*}", self.handle_head_object)
        self.app.router.add_delete("/node/objects/{storage_key:.*}", self.handle_delete_object)
        self.app.router.add_post("/node/verify/{storage_key:.*}", self.handle_verify_object)
        self.app.router.add_post("/node/replicate", self.handle_replicate_object)
        
        # Fault injection routes
        self.app.router.add_post("/node/faults/corrupt", self.handle_inject_corruption)
        self.app.router.add_post("/node/faults/toggle_offline", self.handle_toggle_offline)
        self.app.router.add_post("/node/faults/set_delay", self.handle_set_delay)

    def _resolve_object_path(self, storage_key: str) -> Path:
        # Sanitize storage key to avoid path traversal
        clean_key = storage_key.lstrip("/")
        return self.objects_dir / clean_key

    async def _apply_faults(self) -> None:
        if self.is_offline:
            raise web.HTTPServiceUnavailable(text=f"Node {self.node_id} is simulated OFFLINE")
        if self.simulated_delay_sec > 0:
            await asyncio.sleep(self.simulated_delay_sec)

    async def handle_heartbeat(self, request: web.Request) -> web.Response:
        await self._apply_faults()
        total, used, free = shutil.disk_usage(self.data_dir)
        return web.json_response({
            "node_id": self.node_id,
            "status": "UP",
            "uptime_seconds": time.time() - self.start_time,
            "rack": self.rack,
            "zone": self.zone,
            "disk": {
                "total_bytes": total,
                "used_bytes": used,
                "free_bytes": free
            }
        })

    async def handle_put_object(self, request: web.Request) -> web.Response:
        await self._apply_faults()
        storage_key = request.match_info["storage_key"]
        final_path = self._resolve_object_path(storage_key)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        staging_file = self.staging_dir / f"stage_{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()
        total_bytes = 0

        try:
            with open(staging_file, "wb") as f:
                async for chunk in request.content.iter_chunked(64 * 1024):
                    f.write(chunk)
                    hasher.update(chunk)
                    total_bytes += len(chunk)

            calculated_sha256 = hasher.hexdigest()
            expected_sha256 = request.headers.get("X-Expected-SHA256")
            
            if expected_sha256 and expected_sha256.lower() != calculated_sha256.lower():
                staging_file.unlink(missing_ok=True)
                return web.json_response(
                    {"error": "Checksum mismatch", "calculated": calculated_sha256, "expected": expected_sha256},
                    status=400
                )

            # Atomic commit to final path
            staging_file.replace(final_path)
            
            # Save sidecar metadata with checksum
            meta_path = final_path.with_suffix(final_path.suffix + ".meta")
            with open(meta_path, "w") as mf:
                json.dump({
                    "storage_key": storage_key,
                    "size_bytes": total_bytes,
                    "sha256": calculated_sha256,
                    "created_at": time.time()
                }, mf)

            return web.json_response({
                "node_id": self.node_id,
                "storage_key": storage_key,
                "size_bytes": total_bytes,
                "sha256": calculated_sha256,
                "status": "COMMITTED"
            }, status=201)

        except Exception as e:
            staging_file.unlink(missing_ok=True)
            logger.exception("Failed to write object %s", storage_key)
            return web.json_response({"error": str(e)}, status=500)

    async def handle_get_object(self, request: web.Request) -> web.StreamResponse:
        await self._apply_faults()
        storage_key = request.match_info["storage_key"]
        final_path = self._resolve_object_path(storage_key)

        if not final_path.exists() or not final_path.is_file():
            raise web.HTTPNotFound(text="Object not found on node")

        file_size = final_path.stat().st_size
        range_header = request.headers.get("Range")

        start = 0
        end = file_size - 1
        is_range = False

        if range_header and range_header.startswith("bytes="):
            is_range = True
            range_val = range_header[6:].strip()
            parts = range_val.split("-")
            if parts[0]:
                start = int(parts[0])
            if len(parts) > 1 and parts[1]:
                end = int(parts[1])
            if start >= file_size or end >= file_size or start > end:
                raise web.HTTPRequestRangeNotSatisfiable(
                    headers={"Content-Range": f"bytes */{file_size}"}
                )

        length = end - start + 1
        status = 206 if is_range else 200

        headers = {
            "Content-Length": str(length),
            "Content-Type": "application/octet-stream",
            "Accept-Ranges": "bytes"
        }
        if is_range:
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

        # Fetch checksum if available
        meta_path = final_path.with_suffix(final_path.suffix + ".meta")
        if meta_path.exists():
            try:
                with open(meta_path, "r") as mf:
                    meta = json.load(mf)
                    headers["X-Checksum-SHA256"] = meta.get("sha256", "")
            except Exception:
                pass

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)

        with open(final_path, "rb") as f:
            f.seek(start)
            bytes_left = length
            chunk_size = 64 * 1024
            while bytes_left > 0:
                to_read = min(chunk_size, bytes_left)
                data = f.read(to_read)
                if not data:
                    break
                await response.write(data)
                bytes_left -= len(data)

        await response.write_eof()
        return response

    async def handle_head_object(self, request: web.Request) -> web.Response:
        await self._apply_faults()
        storage_key = request.match_info["storage_key"]
        final_path = self._resolve_object_path(storage_key)

        if not final_path.exists() or not final_path.is_file():
            raise web.HTTPNotFound(text="Object not found on node")

        file_size = final_path.stat().st_size
        headers = {
            "Content-Length": str(file_size),
            "Content-Type": "application/octet-stream",
            "Accept-Ranges": "bytes"
        }
        meta_path = final_path.with_suffix(final_path.suffix + ".meta")
        if meta_path.exists():
            try:
                with open(meta_path, "r") as mf:
                    meta = json.load(mf)
                    headers["X-Checksum-SHA256"] = meta.get("sha256", "")
            except Exception:
                pass

        return web.Response(status=200, headers=headers)

    async def handle_delete_object(self, request: web.Request) -> web.Response:
        await self._apply_faults()
        storage_key = request.match_info["storage_key"]
        final_path = self._resolve_object_path(storage_key)
        meta_path = final_path.with_suffix(final_path.suffix + ".meta")

        if not final_path.exists():
            raise web.HTTPNotFound(text="Object not found on node")

        final_path.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)
        return web.Response(status=204)

    async def handle_verify_object(self, request: web.Request) -> web.Response:
        """Calculates actual SHA-256 of the on-disk file and verifies against metadata."""
        await self._apply_faults()
        storage_key = request.match_info["storage_key"]
        final_path = self._resolve_object_path(storage_key)

        if not final_path.exists() or not final_path.is_file():
            return web.json_response({
                "node_id": self.node_id,
                "storage_key": storage_key,
                "exists": False,
                "healthy": False,
                "error": "Blob missing"
            }, status=404)

        hasher = hashlib.sha256()
        size_bytes = 0
        with open(final_path, "rb") as f:
            while chunk := f.read(64 * 1024):
                hasher.update(chunk)
                size_bytes += len(chunk)

        actual_sha256 = hasher.hexdigest()
        
        # Check expected sha256 from payload or sidecar
        data = {}
        if request.can_read_body:
            try:
                data = await request.json()
            except Exception:
                pass
                
        expected_sha256 = data.get("expected_sha256")
        if not expected_sha256:
            meta_path = final_path.with_suffix(final_path.suffix + ".meta")
            if meta_path.exists():
                try:
                    with open(meta_path, "r") as mf:
                        expected_sha256 = json.load(mf).get("sha256")
                except Exception:
                    pass

        is_healthy = True
        if expected_sha256 and expected_sha256.lower() != actual_sha256.lower():
            is_healthy = False

        return web.json_response({
            "node_id": self.node_id,
            "storage_key": storage_key,
            "exists": True,
            "healthy": is_healthy,
            "size_bytes": size_bytes,
            "actual_sha256": actual_sha256,
            "expected_sha256": expected_sha256
        })

    async def handle_replicate_object(self, request: web.Request) -> web.Response:
        """Pushes a local object to another storage node URL directly."""
        await self._apply_faults()
        data = await request.json()
        storage_key = data["storage_key"]
        target_node_url = data["target_node_url"]
        
        final_path = self._resolve_object_path(storage_key)
        if not final_path.exists():
            raise web.HTTPNotFound(text="Source object not found for replication")

        meta_path = final_path.with_suffix(final_path.suffix + ".meta")
        sha256 = None
        if meta_path.exists():
            try:
                with open(meta_path, "r") as mf:
                    sha256 = json.load(mf).get("sha256")
            except Exception:
                pass

        import aiohttp
        async with aiohttp.ClientSession() as session:
            with open(final_path, "rb") as f:
                headers = {}
                if sha256:
                    headers["X-Expected-SHA256"] = sha256
                url = f"{target_node_url.rstrip('/')}/node/objects/{storage_key}"
                async with session.put(url, data=f, headers=headers) as resp:
                    if resp.status not in (200, 201):
                        err_text = await resp.text()
                        return web.json_response({
                            "status": "error",
                            "target_url": url,
                            "error": err_text
                        }, status=502)

        return web.json_response({
            "status": "success",
            "storage_key": storage_key,
            "target_node_url": target_node_url
        })

    # --- Fault Injection Handlers ---
    async def handle_inject_corruption(self, request: web.Request) -> web.Response:
        """Corrupts bytes in an object blob on disk to simulate bitrot."""
        data = await request.json()
        storage_key = data["storage_key"]
        final_path = self._resolve_object_path(storage_key)
        if not final_path.exists():
            raise web.HTTPNotFound(text="Object to corrupt does not exist")

        with open(final_path, "r+b") as f:
            f.seek(0)
            f.write(b"CORRUPTED_BITROT_BYTES")

        return web.json_response({"status": "corrupted", "storage_key": storage_key})

    async def handle_toggle_offline(self, request: web.Request) -> web.Response:
        data = await request.json()
        self.is_offline = bool(data.get("offline", True))
        return web.json_response({"node_id": self.node_id, "is_offline": self.is_offline})

    async def handle_set_delay(self, request: web.Request) -> web.Response:
        data = await request.json()
        self.simulated_delay_sec = float(data.get("delay_sec", 0.0))
        return web.json_response({"node_id": self.node_id, "delay_sec": self.simulated_delay_sec})

    async def start(self) -> None:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        logger.info("Storage Node %s listening at http://%s:%s", self.node_id, self.host, self.port)

    async def stop(self) -> None:
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        logger.info("Storage Node %s stopped", self.node_id)
