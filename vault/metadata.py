"""
Transactional SQLite-based Metadata Store for Vault.
Handles object catalog, version tracking, replica assignments, and multipart manifests.
Uses Write-Ahead Logging (WAL) and optimistic concurrency control.
"""

from __future__ import annotations
import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from pydantic import BaseModel


class ObjectRecord(BaseModel):
    bucket: str
    key: str
    version_id: str
    size_bytes: int
    sha256: str
    etag: str
    content_type: str = "application/octet-stream"
    custom_metadata: Dict[str, str] = {}
    replica_nodes: List[str]
    state: str = "COMMITTED"  # PENDING, COMMITTED, DELETED
    is_latest: bool = True
    created_at: float
    deleted_at: Optional[float] = None


class MultipartUploadRecord(BaseModel):
    upload_id: str
    bucket: str
    key: str
    content_type: str
    custom_metadata: Dict[str, str]
    state: str  # INITIATED, COMPLETED, ABORTED
    created_at: float


class PartRecord(BaseModel):
    upload_id: str
    part_number: int
    size_bytes: int
    sha256: str
    etag: str
    replica_nodes: List[str]
    created_at: float


class MetadataStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS buckets (
                    name TEXT PRIMARY KEY,
                    created_at REAL NOT NULL,
                    settings_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS objects (
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    etag TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    custom_metadata_json TEXT DEFAULT '{}',
                    replica_nodes_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    is_latest INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    deleted_at REAL,
                    PRIMARY KEY (bucket, key, version_id)
                );

                CREATE INDEX IF NOT EXISTS idx_objects_lookup 
                ON objects (bucket, key, is_latest, state);

                CREATE INDEX IF NOT EXISTS idx_objects_prefix 
                ON objects (bucket, is_latest, state, key);

                CREATE TABLE IF NOT EXISTS multipart_uploads (
                    upload_id TEXT PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    custom_metadata_json TEXT DEFAULT '{}',
                    state TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS multipart_parts (
                    upload_id TEXT NOT NULL,
                    part_number INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    etag TEXT NOT NULL,
                    replica_nodes_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (upload_id, part_number)
                );
            """)
            conn.commit()

    # --- Bucket Operations ---
    def create_bucket(self, name: str) -> bool:
        with self._get_connection() as conn:
            try:
                conn.execute(
                    "INSERT INTO buckets (name, created_at) VALUES (?, ?)",
                    (name, time.time())
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def bucket_exists(self, name: str) -> bool:
        with self._get_connection() as conn:
            cur = conn.execute("SELECT 1 FROM buckets WHERE name = ?", (name,))
            return cur.fetchone() is not None

    def list_buckets(self) -> List[str]:
        with self._get_connection() as conn:
            cur = conn.execute("SELECT name FROM buckets ORDER BY name ASC")
            return [row["name"] for row in cur.fetchall()]

    # --- Object Operations ---
    def prepare_object(
        self,
        bucket: str,
        key: str,
        size_bytes: int,
        sha256: str,
        etag: str,
        content_type: str,
        custom_metadata: Dict[str, str],
        replica_nodes: List[str],
        version_id: Optional[str] = None
    ) -> ObjectRecord:
        """Create a pending object version record."""
        ver = version_id or str(uuid.uuid4())
        now = time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO objects (
                    bucket, key, version_id, size_bytes, sha256, etag,
                    content_type, custom_metadata_json, replica_nodes_json,
                    state, is_latest, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?)
                """,
                (
                    bucket, key, ver, size_bytes, sha256, etag,
                    content_type, json.dumps(custom_metadata),
                    json.dumps(replica_nodes), now
                )
            )
            conn.commit()

        return ObjectRecord(
            bucket=bucket,
            key=key,
            version_id=ver,
            size_bytes=size_bytes,
            sha256=sha256,
            etag=etag,
            content_type=content_type,
            custom_metadata=custom_metadata,
            replica_nodes=replica_nodes,
            state="PENDING",
            is_latest=False,
            created_at=now
        )

    def commit_object(
        self,
        bucket: str,
        key: str,
        version_id: str,
        expected_etag: Optional[str] = None,
        if_none_match: bool = False
    ) -> Optional[ObjectRecord]:
        """
        Atomically commit object version:
        - Checks OCC preconditions (if_none_match, expected_etag).
        - Demotes any previous 'is_latest' versions.
        - Sets this version to COMMITTED and is_latest = 1.
        """
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            
            # Check preconditions
            cur = conn.execute(
                "SELECT etag FROM objects WHERE bucket = ? AND key = ? AND is_latest = 1 AND state = 'COMMITTED'",
                (bucket, key)
            )
            latest_row = cur.fetchone()
            
            if if_none_match and latest_row is not None:
                conn.rollback()
                raise ValueError("Precondition failed: Object already exists (If-None-Match: *)")
                
            if expected_etag and (latest_row is None or latest_row["etag"] != expected_etag):
                conn.rollback()
                raise ValueError(f"Precondition failed: ETag mismatch (expected {expected_etag})")

            # Demote previous latest
            conn.execute(
                "UPDATE objects SET is_latest = 0 WHERE bucket = ? AND key = ?",
                (bucket, key)
            )

            # Promote this version
            cur = conn.execute(
                """
                UPDATE objects 
                SET state = 'COMMITTED', is_latest = 1
                WHERE bucket = ? AND key = ? AND version_id = ?
                """,
                (bucket, key, version_id)
            )
            if cur.rowcount == 0:
                conn.rollback()
                return None

            conn.commit()

        return self.get_object(bucket, key, version_id=version_id)

    def abort_object(self, bucket: str, key: str, version_id: str) -> None:
        """Mark an uncommitted object as ABORTED."""
        with self._get_connection() as conn:
            conn.execute(
                "DELETE FROM objects WHERE bucket = ? AND key = ? AND version_id = ? AND state = 'PENDING'",
                (bucket, key, version_id)
            )
            conn.commit()

    def get_object(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None
    ) -> Optional[ObjectRecord]:
        """Fetch object metadata for latest or specific version."""
        with self._get_connection() as conn:
            if version_id:
                cur = conn.execute(
                    """
                    SELECT * FROM objects 
                    WHERE bucket = ? AND key = ? AND version_id = ? AND state = 'COMMITTED'
                    """,
                    (bucket, key, version_id)
                )
            else:
                cur = conn.execute(
                    """
                    SELECT * FROM objects 
                    WHERE bucket = ? AND key = ? AND is_latest = 1 AND state = 'COMMITTED'
                    """,
                    (bucket, key)
                )
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_record(row)

    def delete_object(self, bucket: str, key: str) -> Optional[str]:
        """
        Soft delete: records a tombstone version or marks current latest as DELETED.
        Returns the deleted version_id, or None if not found.
        """
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "SELECT version_id FROM objects WHERE bucket = ? AND key = ? AND is_latest = 1 AND state = 'COMMITTED'",
                (bucket, key)
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            
            ver = row["version_id"]
            now = time.time()
            conn.execute(
                "UPDATE objects SET state = 'DELETED', is_latest = 0, deleted_at = ? WHERE bucket = ? AND key = ? AND version_id = ?",
                (now, bucket, key, ver)
            )
            conn.commit()
            return ver

    def update_object_replicas(
        self,
        bucket: str,
        key: str,
        version_id: str,
        replica_nodes: List[str]
    ) -> bool:
        """Update replica locations after read-repair or rebalancing."""
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE objects 
                SET replica_nodes_json = ? 
                WHERE bucket = ? AND key = ? AND version_id = ?
                """,
                (json.dumps(replica_nodes), bucket, key, version_id)
            )
            conn.commit()
            return cur.rowcount > 0

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: Optional[str] = None,
        marker: Optional[str] = None,
        limit: int = 100
    ) -> Tuple[List[ObjectRecord], List[str], Optional[str]]:
        """
        Lists committed latest objects matching prefix.
        Supports delimiter (for S3-style directory simulation) and marker pagination.
        Returns: (objects, common_prefixes, next_marker)
        """
        with self._get_connection() as conn:
            query = """
                SELECT * FROM objects 
                WHERE bucket = ? AND is_latest = 1 AND state = 'COMMITTED'
            """
            params: List[Any] = [bucket]
            
            if prefix:
                query += " AND key LIKE ?"
                params.append(f"{prefix}%")
                
            if marker:
                query += " AND key > ?"
                params.append(marker)
                
            query += " ORDER BY key ASC LIMIT ?"
            params.append(limit + 1)
            
            cur = conn.execute(query, params)
            rows = cur.fetchall()

        objects: List[ObjectRecord] = []
        common_prefixes: Set[str] = set()
        next_marker: Optional[str] = None

        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]
            next_marker = rows[-1]["key"]

        for row in rows:
            rec = self._row_to_record(row)
            if delimiter and delimiter in rec.key[len(prefix):]:
                # Extract common prefix
                rel = rec.key[len(prefix):]
                part = rel.split(delimiter)[0] + delimiter
                common_prefixes.add(prefix + part)
            else:
                objects.append(rec)

        return objects, sorted(list(common_prefixes)), next_marker

    def get_all_committed_objects(self) -> List[ObjectRecord]:
        """Fetch all committed latest objects for active cluster scrub and rebalance."""
        with self._get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM objects WHERE is_latest = 1 AND state = 'COMMITTED' ORDER BY bucket, key"
            )
            return [self._row_to_record(r) for r in cur.fetchall()]

    # --- Multipart Upload Operations ---
    def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        custom_metadata: Optional[Dict[str, str]] = None
    ) -> MultipartUploadRecord:
        upload_id = str(uuid.uuid4())
        now = time.time()
        custom_metadata = custom_metadata or {}
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO multipart_uploads (
                    upload_id, bucket, key, content_type, custom_metadata_json, state, created_at
                ) VALUES (?, ?, ?, ?, ?, 'INITIATED', ?)
                """,
                (upload_id, bucket, key, content_type, json.dumps(custom_metadata), now)
            )
            conn.commit()

        return MultipartUploadRecord(
            upload_id=upload_id,
            bucket=bucket,
            key=key,
            content_type=content_type,
            custom_metadata=custom_metadata,
            state="INITIATED",
            created_at=now
        )

    def record_part(
        self,
        upload_id: str,
        part_number: int,
        size_bytes: int,
        sha256: str,
        etag: str,
        replica_nodes: List[str]
    ) -> PartRecord:
        now = time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO multipart_parts (
                    upload_id, part_number, size_bytes, sha256, etag, replica_nodes_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (upload_id, part_number, size_bytes, sha256, etag, json.dumps(replica_nodes), now)
            )
            conn.commit()

        return PartRecord(
            upload_id=upload_id,
            part_number=part_number,
            size_bytes=size_bytes,
            sha256=sha256,
            etag=etag,
            replica_nodes=replica_nodes,
            created_at=now
        )

    def get_multipart_upload(self, upload_id: str) -> Optional[MultipartUploadRecord]:
        with self._get_connection() as conn:
            cur = conn.execute("SELECT * FROM multipart_uploads WHERE upload_id = ?", (upload_id,))
            row = cur.fetchone()
            if not row:
                return None
            return MultipartUploadRecord(
                upload_id=row["upload_id"],
                bucket=row["bucket"],
                key=row["key"],
                content_type=row["content_type"],
                custom_metadata=json.loads(row["custom_metadata_json"]),
                state=row["state"],
                created_at=row["created_at"]
            )

    def list_parts(self, upload_id: str) -> List[PartRecord]:
        with self._get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM multipart_parts WHERE upload_id = ? ORDER BY part_number ASC",
                (upload_id,)
            )
            return [
                PartRecord(
                    upload_id=r["upload_id"],
                    part_number=r["part_number"],
                    size_bytes=r["size_bytes"],
                    sha256=r["sha256"],
                    etag=r["etag"],
                    replica_nodes=json.loads(r["replica_nodes_json"]),
                    created_at=r["created_at"]
                )
                for r in cur.fetchall()
            ]

    def complete_multipart_upload(self, upload_id: str) -> None:
        with self._get_connection() as conn:
            conn.execute("UPDATE multipart_uploads SET state = 'COMPLETED' WHERE upload_id = ?", (upload_id,))
            conn.commit()

    def abort_multipart_upload(self, upload_id: str) -> None:
        with self._get_connection() as conn:
            conn.execute("UPDATE multipart_uploads SET state = 'ABORTED' WHERE upload_id = ?", (upload_id,))
            conn.execute("DELETE FROM multipart_parts WHERE upload_id = ?", (upload_id,))
            conn.commit()

    def _row_to_record(self, row: sqlite3.Row) -> ObjectRecord:
        return ObjectRecord(
            bucket=row["bucket"],
            key=row["key"],
            version_id=row["version_id"],
            size_bytes=row["size_bytes"],
            sha256=row["sha256"],
            etag=row["etag"],
            content_type=row["content_type"],
            custom_metadata=json.loads(row["custom_metadata_json"]),
            replica_nodes=json.loads(row["replica_nodes_json"]),
            state=row["state"],
            is_latest=bool(row["is_latest"]),
            created_at=row["created_at"],
            deleted_at=row["deleted_at"]
        )
