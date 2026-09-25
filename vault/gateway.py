"""
Vault REST API Gateway & Control Plane Service.
Provides S3-compatible REST API for clients, cluster orchestration, metrics, and Web Dashboard.
"""

from __future__ import annotations
import io
from pathlib import Path
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, Request, Response, HTTPException, Query, Header, status
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from vault.config import VaultConfig, StorageNodeConfig
from vault.metadata import MetadataStore
from vault.cluster import ClusterManager
from vault.engine import (
    VaultEngine,
    QuorumError,
    PreconditionFailedError,
    ObjectNotFoundError,
    ObjectUnrecoverableError
)
from vault.repair import ActiveScrubber
from vault.rebalance import Rebalancer
from vault.metrics import metrics


from contextlib import asynccontextmanager

def create_gateway_app(
    config: VaultConfig,
    metadata_store: Optional[MetadataStore] = None,
    cluster_manager: Optional[ClusterManager] = None,
    engine: Optional[VaultEngine] = None,
    scrubber: Optional[ActiveScrubber] = None,
    rebalancer: Optional[Rebalancer] = None
) -> FastAPI:
    meta = metadata_store or MetadataStore(config.base_dir / config.metadata_db_name)
    cluster = cluster_manager or ClusterManager(config)
    eng = engine or VaultEngine(config, meta, cluster)
    scrub = scrubber or ActiveScrubber(config, meta, cluster)
    rebal = rebalancer or Rebalancer(config, meta, cluster)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await cluster.start()
        await eng.start()
        await scrub.start()
        await rebal.start()
        yield
        await rebal.stop()
        await scrub.stop()
        await eng.stop()
        await cluster.stop()

    app = FastAPI(
        title="Vault Object Storage",
        version="0.1.0",
        description="Fault-tolerant distributed object storage system",
        lifespan=lifespan
    )

    # Store references in app.state
    app.state.config = config
    app.state.metadata = meta
    app.state.cluster = cluster
    app.state.engine = eng
    app.state.scrubber = scrub
    app.state.rebalancer = rebal

    # --- Web Dashboard & Dedicated Page Routes ---
    dashboard_path = Path(__file__).parent / "web" / "index.html"

    @app.get("/", response_class=HTMLResponse)
    @app.get("/dashboard", response_class=HTMLResponse)
    @app.get("/overview", response_class=HTMLResponse)
    @app.get("/nodes", response_class=HTMLResponse)
    @app.get("/policy", response_class=HTMLResponse)
    @app.get("/quorum", response_class=HTMLResponse)
    @app.get("/objects", response_class=HTMLResponse)
    @app.get("/explorer", response_class=HTMLResponse)
    @app.get("/chaos", response_class=HTMLResponse)
    @app.head("/", response_class=HTMLResponse)
    @app.head("/dashboard", response_class=HTMLResponse)
    @app.head("/overview", response_class=HTMLResponse)
    @app.head("/nodes", response_class=HTMLResponse)
    @app.head("/policy", response_class=HTMLResponse)
    @app.head("/quorum", response_class=HTMLResponse)
    @app.head("/objects", response_class=HTMLResponse)
    @app.head("/explorer", response_class=HTMLResponse)
    @app.head("/chaos", response_class=HTMLResponse)
    async def get_dashboard() -> str:
        if dashboard_path.exists():
            return dashboard_path.read_text(encoding="utf-8")
        return "<h1>Vault Object Storage Dashboard</h1>"

    # --- Object Operations ---
    @app.put("/api/v1/objects/{bucket}/{key:path}", status_code=status.HTTP_201_CREATED)
    async def put_object(
        bucket: str,
        key: str,
        request: Request,
        content_type: str = Header(default="application/octet-stream"),
        if_match: Optional[str] = Header(default=None),
        if_none_match: Optional[str] = Header(default=None)
    ) -> Dict[str, Any]:
        data = await request.body()
        
        # Extract custom metadata from X-Meta-* headers
        custom_metadata = {}
        for h_key, h_val in request.headers.items():
            if h_key.lower().startswith("x-meta-"):
                clean_k = h_key[7:]
                custom_metadata[clean_k] = h_val

        try:
            record = await eng.put_object(
                bucket=bucket,
                key=key,
                data=data,
                content_type=content_type,
                custom_metadata=custom_metadata,
                expected_etag=if_match,
                if_none_match=(if_none_match == "*")
            )
            return JSONResponse(
                status_code=status.HTTP_201_CREATED,
                content=record.model_dump(),
                headers={
                    "ETag": record.etag,
                    "X-Version-Id": record.version_id,
                    "X-Checksum-SHA256": record.sha256
                }
            )
        except QuorumError as qe:
            raise HTTPException(status_code=503, detail=str(qe))
        except PreconditionFailedError as pe:
            raise HTTPException(status_code=412, detail=str(pe))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/v1/objects/{bucket}/{key:path}")
    async def get_object(
        bucket: str,
        key: str,
        version_id: Optional[str] = Query(default=None),
        range_header: Optional[str] = Header(default=None, alias="Range")
    ) -> Response:
        range_start, range_end = None, None
        if range_header and range_header.startswith("bytes="):
            parts = range_header[6:].split("-")
            if parts[0]:
                range_start = int(parts[0])
            if len(parts) > 1 and parts[1]:
                range_end = int(parts[1])

        try:
            record, data = await eng.get_object(
                bucket=bucket,
                key=key,
                version_id=version_id,
                range_start=range_start,
                range_end=range_end
            )
            
            headers = {
                "ETag": record.etag,
                "X-Version-Id": record.version_id,
                "X-Checksum-SHA256": record.sha256,
                "Content-Type": record.content_type,
                "Content-Length": str(len(data)),
                "Accept-Ranges": "bytes"
            }
            # Custom metadata headers
            for mk, mv in record.custom_metadata.items():
                headers[f"X-Meta-{mk}"] = mv

            status_code = 206 if (range_start is not None or range_end is not None) else 200
            if status_code == 206:
                headers["Content-Range"] = f"bytes {range_start or 0}-{(range_start or 0) + len(data) - 1}/{record.size_bytes}"

            return Response(content=data, status_code=status_code, headers=headers)
        except ObjectNotFoundError:
            raise HTTPException(status_code=404, detail="Object not found")
        except ObjectUnrecoverableError as ue:
            raise HTTPException(status_code=500, detail=str(ue))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.head("/api/v1/objects/{bucket}/{key:path}")
    async def head_object(
        bucket: str,
        key: str,
        version_id: Optional[str] = Query(default=None)
    ) -> Response:
        try:
            record = await eng.head_object(bucket, key, version_id=version_id)
            headers = {
                "ETag": record.etag,
                "X-Version-Id": record.version_id,
                "X-Checksum-SHA256": record.sha256,
                "Content-Type": record.content_type,
                "Content-Length": str(record.size_bytes),
                "Accept-Ranges": "bytes"
            }
            for mk, mv in record.custom_metadata.items():
                headers[f"X-Meta-{mk}"] = mv
            return Response(status_code=200, headers=headers)
        except ObjectNotFoundError:
            raise HTTPException(status_code=404, detail="Object not found")

    @app.delete("/api/v1/objects/{bucket}/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_object(bucket: str, key: str) -> Response:
        try:
            await eng.delete_object(bucket, key)
            return Response(status_code=204)
        except ObjectNotFoundError:
            raise HTTPException(status_code=404, detail="Object not found")

    @app.get("/api/v1/objects/{bucket}")
    async def list_objects(
        bucket: str,
        prefix: str = Query(default=""),
        delimiter: Optional[str] = Query(default=None),
        marker: Optional[str] = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000)
    ) -> Dict[str, Any]:
        objects, prefixes, next_marker = eng.list_objects(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            marker=marker,
            limit=limit
        )
        return {
            "bucket": bucket,
            "prefix": prefix,
            "delimiter": delimiter,
            "marker": marker,
            "next_marker": next_marker,
            "common_prefixes": prefixes,
            "objects": [o.model_dump() for o in objects]
        }

    # --- Multipart Transfers ---
    @app.post("/api/v1/multipart/init")
    async def initiate_multipart(request: Request) -> Dict[str, Any]:
        data = await request.json()
        bucket = data.get("bucket", "default")
        key = data.get("key")
        if not key:
            raise HTTPException(status_code=400, detail="Key is required")
        rec = eng.initiate_multipart_upload(
            bucket=bucket,
            key=key,
            content_type=data.get("content_type", "application/octet-stream"),
            custom_metadata=data.get("custom_metadata", {})
        )
        return rec.model_dump()

    @app.put("/api/v1/multipart/{upload_id}/part")
    async def upload_multipart_part(
        upload_id: str,
        request: Request,
        part_number: int = Query(ge=1, le=10000)
    ) -> Dict[str, Any]:
        data = await request.body()
        try:
            part_rec = await eng.upload_part(upload_id=upload_id, part_number=part_number, data=data)
            return part_rec.model_dump()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/v1/multipart/{upload_id}/complete")
    async def complete_multipart(upload_id: str) -> Dict[str, Any]:
        try:
            committed = await eng.complete_multipart_upload(upload_id)
            return committed.model_dump()
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.delete("/api/v1/multipart/{upload_id}", status_code=204)
    async def abort_multipart(upload_id: str) -> Response:
        await eng.abort_multipart_upload(upload_id)
        return Response(status_code=204)

    # --- Cluster Management & Health ---
    @app.get("/api/v1/cluster/status")
    async def cluster_status() -> Dict[str, Any]:
        nodes_state = cluster.get_all_node_states()
        healthy_nodes = cluster.get_healthy_node_ids()
        return {
            "cluster_name": config.cluster_name,
            "quorum": {
                "N": config.quorum.replication_factor,
                "W": config.quorum.write_quorum,
                "R": config.quorum.read_quorum,
                "strongly_consistent": eng.policy.is_strongly_consistent
            },
            "nodes_total": len(nodes_state),
            "nodes_healthy": len(healthy_nodes),
            "is_available": len(healthy_nodes) >= config.quorum.write_quorum,
            "nodes": nodes_state
        }

    @app.post("/api/v1/cluster/check_nodes")
    async def trigger_check_nodes() -> Dict[str, Any]:
        await cluster.check_all_nodes()
        return {"status": "checked"}

    @app.post("/api/v1/cluster/nodes/join")
    async def node_join(node_cfg: StorageNodeConfig) -> Dict[str, Any]:
        cluster.register_node(node_cfg)
        return {"status": "joined", "node_id": node_cfg.node_id}

    @app.post("/api/v1/cluster/nodes/leave")
    async def node_leave(node_id: str = Query(...)) -> Dict[str, Any]:
        success = cluster.deregister_node(node_id)
        return {"status": "left" if success else "not_found", "node_id": node_id}

    @app.post("/api/v1/cluster/scrub")
    async def trigger_scrub() -> Dict[str, Any]:
        return await scrub.run_scrub()

    @app.post("/api/v1/cluster/rebalance")
    async def trigger_rebalance() -> Dict[str, Any]:
        return await rebal.run_rebalance()

    @app.get("/api/v1/cluster/rebalance/status")
    async def rebalance_status() -> Dict[str, Any]:
        return rebal.progress.to_json()

    # --- Metrics & Telemetry ---
    @app.get("/api/v1/metrics")
    async def get_metrics_json() -> Dict[str, Any]:
        return metrics.to_json()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def get_metrics_prometheus() -> str:
        return metrics.to_prometheus()

    # --- Chaos & Fault Injection Helpers ---
    @app.post("/api/v1/cluster/faults/corrupt")
    async def inject_corruption(request: Request) -> Dict[str, Any]:
        data = await request.json()
        node_id = data["node_id"]
        bucket = data["bucket"]
        key = data["key"]
        
        rec = meta.get_object(bucket, key)
        if not rec:
            raise HTTPException(status_code=404, detail="Object not found")
            
        node_cfg = cluster.get_node_config(node_id)
        if not node_cfg:
            raise HTTPException(status_code=404, detail="Node not found")

        import aiohttp
        storage_key = f"{bucket}/{key}/{rec.version_id}.blob"
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{node_cfg.url}/node/faults/corrupt",
                json={"storage_key": storage_key}
            ) as resp:
                return await resp.json()

    @app.post("/api/v1/cluster/faults/toggle_offline")
    async def toggle_node_offline(node_id: str = Query(...), offline: bool = True) -> Dict[str, Any]:
        node_cfg = cluster.get_node_config(node_id)
        if not node_cfg:
            raise HTTPException(status_code=404, detail="Node not found")

        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{node_cfg.url}/node/faults/toggle_offline",
                json={"offline": offline}
            ) as resp:
                await cluster.check_all_nodes()
                return await resp.json()

    @app.post("/api/v1/cluster/faults/set_delay")
    async def set_node_delay(node_id: str = Query(...), delay_sec: float = Query(...)) -> Dict[str, Any]:
        node_cfg = cluster.get_node_config(node_id)
        if not node_cfg:
            raise HTTPException(status_code=404, detail="Node not found")

        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{node_cfg.url}/node/faults/set_delay",
                json={"delay_sec": delay_sec}
            ) as resp:
                return await resp.json()

    return app
