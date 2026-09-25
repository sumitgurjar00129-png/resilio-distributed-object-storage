"""
Integration tests for Gateway HTTP REST API.
"""

import pytest
import httpx
from vault.gateway import create_gateway_app


@pytest.mark.asyncio
async def test_gateway_rest_api_lifecycle(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env
    app = create_gateway_app(
        config=config,
        metadata_store=meta_store,
        cluster_manager=cluster_mgr,
        engine=engine
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        # 1. Cluster Status
        resp = await client.get("/api/v1/cluster/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["nodes_healthy"] == 3
        assert data["is_available"] is True

        # 2. PUT object
        put_resp = await client.put(
            "/api/v1/objects/documents/contract.pdf",
            content=b"Sample PDF binary stream content",
            headers={"Content-Type": "application/pdf", "X-Meta-Department": "Legal"}
        )
        assert put_resp.status_code == 201
        put_data = put_resp.json()
        assert put_data["bucket"] == "documents"
        assert put_data["key"] == "contract.pdf"
        assert "ETag" in put_resp.headers

        # 3. GET object
        get_resp = await client.get("/api/v1/objects/documents/contract.pdf")
        assert get_resp.status_code == 200
        assert get_resp.content == b"Sample PDF binary stream content"
        assert get_resp.headers["X-Meta-Department"] == "Legal"

        # 4. HEAD object
        head_resp = await client.head("/api/v1/objects/documents/contract.pdf")
        assert head_resp.status_code == 200
        assert head_resp.headers["Content-Length"] == str(len(b"Sample PDF binary stream content"))

        # 5. LIST objects
        list_resp = await client.get("/api/v1/objects/documents")
        assert list_resp.status_code == 200
        assert len(list_resp.json()["objects"]) == 1

        # 6. Multipart flow via HTTP
        init_resp = await client.post(
            "/api/v1/multipart/init",
            json={"bucket": "documents", "key": "large_archive.zip"}
        )
        assert init_resp.status_code == 200
        upload_id = init_resp.json()["upload_id"]

        # Upload part 1
        part_resp = await client.put(
            f"/api/v1/multipart/{upload_id}/part?part_number=1",
            content=b"PART_1_CONTENT"
        )
        assert part_resp.status_code == 200

        # Complete multipart
        comp_resp = await client.post(f"/api/v1/multipart/{upload_id}/complete")
        assert comp_resp.status_code == 200

        # Read back assembled object
        down_resp = await client.get("/api/v1/objects/documents/large_archive.zip")
        assert down_resp.status_code == 200
        assert down_resp.content == b"PART_1_CONTENT"

        # 7. Metrics endpoint
        metrics_resp = await client.get("/api/v1/metrics")
        assert metrics_resp.status_code == 200
        assert "requests_put_success" in metrics_resp.json()["counters"]

        prom_resp = await client.get("/metrics")
        assert prom_resp.status_code == 200
        assert "vault_requests_put_success" in prom_resp.text

        # 8. Web dashboard HTML
        dash_resp = await client.get("/dashboard")
        assert dash_resp.status_code == 200
        assert "Vault Object Storage" in dash_resp.text
