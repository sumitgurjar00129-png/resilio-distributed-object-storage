"""
Unit tests for Transactional SQLite MetadataStore.
"""

import pytest
from pathlib import Path
from vault.metadata import MetadataStore


@pytest.fixture
def store(tmp_path: Path) -> MetadataStore:
    db_file = tmp_path / "metadata_test.db"
    return MetadataStore(db_file)


def test_bucket_lifecycle(store: MetadataStore):
    assert store.create_bucket("test-bucket") is True
    assert store.create_bucket("test-bucket") is False  # Duplicate
    assert store.bucket_exists("test-bucket") is True
    assert store.bucket_exists("non-existent") is False
    assert "test-bucket" in store.list_buckets()


def test_object_versioning_and_occ(store: MetadataStore):
    store.create_bucket("b1")

    # Write v1
    v1_rec = store.prepare_object(
        bucket="b1",
        key="file.txt",
        size_bytes=100,
        sha256="hash1",
        etag='"etag1"',
        content_type="text/plain",
        custom_metadata={"author": "alice"},
        replica_nodes=["node-1", "node-2"]
    )
    assert v1_rec.state == "PENDING"
    
    # Commit v1
    comm1 = store.commit_object("b1", "file.txt", v1_rec.version_id)
    assert comm1 is not None
    assert comm1.is_latest is True
    assert comm1.state == "COMMITTED"

    # Fetch latest
    obj = store.get_object("b1", "file.txt")
    assert obj.version_id == v1_rec.version_id
    assert obj.custom_metadata["author"] == "alice"

    # Write v2 with expected_etag matching
    v2_rec = store.prepare_object(
        bucket="b1",
        key="file.txt",
        size_bytes=200,
        sha256="hash2",
        etag='"etag2"',
        content_type="text/plain",
        custom_metadata={"author": "bob"},
        replica_nodes=["node-2", "node-3"]
    )
    comm2 = store.commit_object("b1", "file.txt", v2_rec.version_id, expected_etag='"etag1"')
    assert comm2.version_id == v2_rec.version_id

    # Verify v1 is no longer latest, but still retrievable by version_id
    old_v1 = store.get_object("b1", "file.txt", version_id=v1_rec.version_id)
    assert old_v1.version_id == v1_rec.version_id
    assert old_v1.is_latest is False

    latest = store.get_object("b1", "file.txt")
    assert latest.version_id == v2_rec.version_id

    # Test OCC failure: wrong expected ETag
    v3_rec = store.prepare_object(
        bucket="b1",
        key="file.txt",
        size_bytes=300,
        sha256="hash3",
        etag='"etag3"',
        content_type="text/plain",
        custom_metadata={},
        replica_nodes=["node-1"]
    )
    with pytest.raises(ValueError, match="Precondition failed: ETag mismatch"):
        store.commit_object("b1", "file.txt", v3_rec.version_id, expected_etag='"wrong-etag"')


def test_object_deletion_and_listing(store: MetadataStore):
    store.create_bucket("docs")

    for name in ["doc1.pdf", "doc2.pdf", "images/photo1.png", "images/photo2.png"]:
        prep = store.prepare_object(
            bucket="docs",
            key=name,
            size_bytes=50,
            sha256=f"hash_{name}",
            etag=f'"etag_{name}"',
            content_type="text/plain",
            custom_metadata={},
            replica_nodes=["node-1", "node-2"]
        )
        store.commit_object("docs", name, prep.version_id)

    # Prefix list
    objs, prefixes, marker = store.list_objects("docs", prefix="images/")
    assert len(objs) == 2
    assert objs[0].key == "images/photo1.png"

    # Delimiter list (directory emulation)
    objs_root, dirs, marker = store.list_objects("docs", prefix="", delimiter="/")
    assert len(objs_root) == 2  # doc1.pdf, doc2.pdf
    assert dirs == ["images/"]

    # Delete doc1.pdf
    del_ver = store.delete_object("docs", "doc1.pdf")
    assert del_ver is not None
    assert store.get_object("docs", "doc1.pdf") is None  # Tombstoned
