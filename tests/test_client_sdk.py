"""
Tests for VaultClient and AsyncVaultClient Python SDKs.
"""

import pytest
from pathlib import Path
from vault.client import AsyncVaultClient, VaultClientError
from vault.gateway import create_gateway_app
import httpx


@pytest.mark.asyncio
async def test_async_client_sdk_crud(cluster_env, tmp_path: Path):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env
    app = create_gateway_app(config, meta_store, cluster_mgr, engine)

    transport = httpx.ASGITransport(app=app)
    async with AsyncVaultClient(endpoint_url="http://testserver", transport=transport) as client:
        # 1. Put Object
        put_res = await client.put_object(
            bucket="sdk-bucket",
            key="greeting.txt",
            data="Hello from Async Python SDK!",
            content_type="text/plain",
            metadata={"source": "pytest"}
        )
        assert put_res["key"] == "greeting.txt"

        # 2. Get Object
        obj = await client.get_object(bucket="sdk-bucket", key="greeting.txt")
        assert obj.text() == "Hello from Async Python SDK!"
        assert obj.custom_metadata["source"] == "pytest"

        # 3. Head Object
        head = await client.head_object(bucket="sdk-bucket", key="greeting.txt")
        assert head["size_bytes"] == len("Hello from Async Python SDK!")

        # 4. List Objects
        listed = await client.list_objects(bucket="sdk-bucket")
        assert len(listed["objects"]) == 1

        # 5. File upload and download helpers
        local_src = tmp_path / "sample.bin"
        local_src.write_bytes(b"BINARY_FILE_PAYLOAD_12345")
        await client.upload_file(bucket="sdk-bucket", key="files/sample.bin", file_path=local_src)

        local_dst = tmp_path / "downloaded.bin"
        await client.download_file(bucket="sdk-bucket", key="files/sample.bin", dest_path=local_dst)
        assert local_dst.read_bytes() == b"BINARY_FILE_PAYLOAD_12345"

        # 6. Delete
        assert await client.delete_object(bucket="sdk-bucket", key="greeting.txt") is True
        with pytest.raises(VaultClientError):
            await client.get_object(bucket="sdk-bucket", key="greeting.txt")
