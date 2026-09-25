"""
Tests for StorageNodeServer HTTP endpoints and atomic staging engine.
"""

import hashlib
import pytest
from pathlib import Path
import aiohttp
from vault.node import StorageNodeServer
from tests.conftest import find_free_port


@pytest.fixture
async def single_node(tmp_path: Path):
    port = find_free_port()
    data_dir = tmp_path / "single_node_data"
    node = StorageNodeServer(
        node_id="test-node-1",
        host="127.0.0.1",
        port=port,
        data_dir=data_dir
    )
    await node.start()
    yield node, f"http://127.0.0.1:{port}"
    await node.stop()


@pytest.mark.asyncio
async def test_node_put_get_verify(single_node):
    node, base_url = single_node
    payload = b"Hello, Vault Distributed Storage World!"
    sha256 = hashlib.sha256(payload).hexdigest()

    async with aiohttp.ClientSession() as session:
        # PUT object
        put_url = f"{base_url}/node/objects/bucket1/key1.blob"
        async with session.put(put_url, data=payload, headers={"X-Expected-SHA256": sha256}) as resp:
            assert resp.status == 201
            data = await resp.json()
            assert data["sha256"] == sha256
            assert data["size_bytes"] == len(payload)

        # GET full object
        async with session.get(put_url) as resp:
            assert resp.status == 200
            content = await resp.read()
            assert content == payload
            assert resp.headers["X-Checksum-SHA256"] == sha256

        # GET Range
        async with session.get(put_url, headers={"Range": "bytes=0-4"}) as resp:
            assert resp.status == 206
            part = await resp.read()
            assert part == b"Hello"
            assert resp.headers["Content-Length"] == "5"

        # Verify endpoint
        verify_url = f"{base_url}/node/verify/bucket1/key1.blob"
        async with session.post(verify_url, json={"expected_sha256": sha256}) as resp:
            assert resp.status == 200
            vdata = await resp.json()
            assert vdata["healthy"] is True
            assert vdata["actual_sha256"] == sha256


@pytest.mark.asyncio
async def test_node_checksum_mismatch_rejection(single_node):
    node, base_url = single_node
    payload = b"Sample data"

    async with aiohttp.ClientSession() as session:
        put_url = f"{base_url}/node/objects/bucket1/key2.blob"
        # Provide incorrect expected checksum
        async with session.put(put_url, data=payload, headers={"X-Expected-SHA256": "invalid_hash_1234"}) as resp:
            assert resp.status == 400
            data = await resp.json()
            assert "Checksum mismatch" in data["error"]

        # Verify staging file was cleaned up and object doesn't exist
        async with session.get(put_url) as resp:
            assert resp.status == 404
