"""
Integration tests for VaultEngine Quorum Operations (N=3, W=2, R=2).
"""

import hashlib
import pytest
from vault.engine import QuorumError, ObjectNotFoundError, PreconditionFailedError


@pytest.mark.asyncio
async def test_engine_put_get_lifecycle(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    payload = b"Vault distributed object storage integration test payload."
    sha256 = hashlib.sha256(payload).hexdigest()

    # 1. Quorum Write (W=2)
    record = await engine.put_object(
        bucket="media",
        key="banner.png",
        data=payload,
        content_type="image/png",
        custom_metadata={"uploader": "antigravity"}
    )
    assert record.size_bytes == len(payload)
    assert record.sha256 == sha256
    assert len(record.replica_nodes) >= 2  # At least W=2 replicas

    # 2. Quorum Read (R=2)
    fetched_rec, fetched_data = await engine.get_object(bucket="media", key="banner.png")
    assert fetched_data == payload
    assert fetched_rec.version_id == record.version_id
    assert fetched_rec.custom_metadata["uploader"] == "antigravity"

    # 3. Head Object
    head_rec = await engine.head_object(bucket="media", key="banner.png")
    assert head_rec.size_bytes == len(payload)
    assert head_rec.etag == record.etag

    # 4. Partial Range Read
    range_rec, range_data = await engine.get_object(
        bucket="media",
        key="banner.png",
        range_start=0,
        range_end=4
    )
    assert range_data == b"Vault"

    # 5. Delete Object
    await engine.delete_object(bucket="media", key="banner.png")
    with pytest.raises(ObjectNotFoundError):
        await engine.get_object(bucket="media", key="banner.png")


@pytest.mark.asyncio
async def test_engine_versioning_and_occ(cluster_env):
    config, engine, storage_nodes, meta_store, cluster_mgr = cluster_env

    # Write initial version
    v1_rec = await engine.put_object(
        bucket="config",
        key="app.json",
        data=b'{"version": 1}'
    )

    # OCC: update with matching ETag
    v2_rec = await engine.put_object(
        bucket="config",
        key="app.json",
        data=b'{"version": 2}',
        expected_etag=v1_rec.etag
    )
    assert v2_rec.version_id != v1_rec.version_id

    # Read latest returns v2
    rec, data = await engine.get_object(bucket="config", key="app.json")
    assert data == b'{"version": 2}'

    # Read specific version v1
    rec_v1, data_v1 = await engine.get_object(bucket="config", key="app.json", version_id=v1_rec.version_id)
    assert data_v1 == b'{"version": 1}'

    # OCC failure: attempt update with outdated ETag
    with pytest.raises(PreconditionFailedError):
        await engine.put_object(
            bucket="config",
            key="app.json",
            data=b'{"version": 3}',
            expected_etag=v1_rec.etag  # Stale ETag
        )

    # If-None-Match: * failure when object already exists
    with pytest.raises(PreconditionFailedError):
        await engine.put_object(
            bucket="config",
            key="app.json",
            data=b'{"version": 4}',
            if_none_match=True
        )
