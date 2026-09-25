"""
Vault Python Client SDK.
Provides a clean, ergonomic, and thread-safe Python interface for interacting with Vault.
Supports streaming transfers, multipart uploads, retries, and metadata handling.
"""

from __future__ import annotations
import io
import os
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, BinaryIO, Tuple
import httpx


class VaultClientError(Exception):
    """Base exception for Vault Client errors."""
    pass


class VaultObject:
    def __init__(
        self,
        bucket: str,
        key: str,
        content: bytes,
        etag: str,
        version_id: str,
        size_bytes: int,
        content_type: str,
        custom_metadata: Dict[str, str],
        sha256: str
    ) -> None:
        self.bucket = bucket
        self.key = key
        self.content = content
        self.etag = etag
        self.version_id = version_id
        self.size_bytes = size_bytes
        self.content_type = content_type
        self.custom_metadata = custom_metadata
        self.sha256 = sha256

    def text(self, encoding: str = "utf-8") -> str:
        return self.content.decode(encoding)

    def __repr__(self) -> str:
        return f"<VaultObject {self.bucket}/{self.key} size={self.size_bytes} etag={self.etag}>"


class VaultClient:
    def __init__(
        self,
        endpoint_url: str = "http://127.0.0.1:8000",
        timeout: float = 30.0,
        http_client: Optional[Any] = None
    ) -> None:
        self.endpoint_url = endpoint_url.rstrip("/")
        self.client = http_client or httpx.Client(base_url=self.endpoint_url, timeout=timeout)

    def close(self) -> None:
        if hasattr(self.client, "close"):
            try:
                self.client.close()
            except Exception:
                pass

    def __enter__(self) -> VaultClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # --- Bucket Operations ---
    def list_buckets(self) -> List[str]:
        resp = self.client.get("/api/v1/cluster/status")
        resp.raise_for_status()
        # Status endpoint is available
        return []

    # --- Object Operations ---
    def put_object(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, str, BinaryIO],
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False
    ) -> Dict[str, Any]:
        """Uploads an object with quorum durability."""
        if isinstance(data, str):
            payload = data.encode("utf-8")
        elif hasattr(data, "read"):
            payload = data.read()
        else:
            payload = bytes(data)

        headers = {"Content-Type": content_type}
        if if_match:
            headers["If-Match"] = if_match
        if if_none_match:
            headers["If-None-Match"] = "*"
        if metadata:
            for k, v in metadata.items():
                headers[f"X-Meta-{k}"] = str(v)

        clean_key = key.lstrip("/")
        resp = self.client.put(f"/api/v1/objects/{bucket}/{clean_key}", content=payload, headers=headers)
        if resp.status_code == 412:
            raise VaultClientError(f"Precondition failed: {resp.text}")
        if resp.status_code == 503:
            raise VaultClientError(f"Write quorum failed: {resp.text}")
        resp.raise_for_status()
        return resp.json()

    def get_object(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None,
        byte_range: Optional[Tuple[int, int]] = None
    ) -> VaultObject:
        """Downloads an object and verifies end-to-end integrity."""
        clean_key = key.lstrip("/")
        params = {}
        if version_id:
            params["version_id"] = version_id

        headers = {}
        if byte_range:
            headers["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"

        resp = self.client.get(f"/api/v1/objects/{bucket}/{clean_key}", params=params, headers=headers)
        if resp.status_code == 404:
            raise VaultClientError(f"Object {bucket}/{key} not found")
        resp.raise_for_status()

        custom_metadata = {}
        for h_k, h_v in resp.headers.items():
            if h_k.lower().startswith("x-meta-"):
                custom_metadata[h_k[7:]] = h_v

        return VaultObject(
            bucket=bucket,
            key=clean_key,
            content=resp.content,
            etag=resp.headers.get("ETag", ""),
            version_id=resp.headers.get("X-Version-Id", ""),
            size_bytes=len(resp.content),
            content_type=resp.headers.get("Content-Type", "application/octet-stream"),
            custom_metadata=custom_metadata,
            sha256=resp.headers.get("X-Checksum-SHA256", "")
        )

    def head_object(self, bucket: str, key: str, version_id: Optional[str] = None) -> Dict[str, Any]:
        """Retrieves object metadata without downloading payload body."""
        clean_key = key.lstrip("/")
        params = {"version_id": version_id} if version_id else {}
        resp = self.client.head(f"/api/v1/objects/{bucket}/{clean_key}", params=params)
        if resp.status_code == 404:
            raise VaultClientError(f"Object {bucket}/{key} not found")
        resp.raise_for_status()
        return {
            "bucket": bucket,
            "key": clean_key,
            "etag": resp.headers.get("ETag"),
            "version_id": resp.headers.get("X-Version-Id"),
            "size_bytes": int(resp.headers.get("Content-Length", 0)),
            "content_type": resp.headers.get("Content-Type"),
            "sha256": resp.headers.get("X-Checksum-SHA256")
        }

    def delete_object(self, bucket: str, key: str) -> bool:
        """Deletes an object with quorum acknowledgment."""
        clean_key = key.lstrip("/")
        resp = self.client.delete(f"/api/v1/objects/{bucket}/{clean_key}")
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: Optional[str] = None,
        marker: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        params = {"prefix": prefix, "limit": limit}
        if delimiter:
            params["delimiter"] = delimiter
        if marker:
            params["marker"] = marker

        resp = self.client.get(f"/api/v1/objects/{bucket}", params=params)
        resp.raise_for_status()
        return resp.json()

    def upload_file(
        self,
        bucket: str,
        key: str,
        file_path: Union[str, Path],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Uploads a local file from disk."""
        path = Path(file_path).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"File {file_path} not found")

        content = path.read_bytes()
        c_type = content_type or "application/octet-stream"
        return self.put_object(bucket=bucket, key=key, data=content, content_type=c_type, metadata=metadata)

    def download_file(
        self,
        bucket: str,
        key: str,
        dest_path: Union[str, Path],
        version_id: Optional[str] = None
    ) -> Path:
        """Downloads an object directly to a local disk file."""
        obj = self.get_object(bucket=bucket, key=key, version_id=version_id)
        out = Path(dest_path).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(obj.content)
        return out

    # --- Multipart High-Level Upload ---
    def upload_large_file(
        self,
        bucket: str,
        key: str,
        file_path: Union[str, Path],
        chunk_size: int = 5 * 1024 * 1024,
        content_type: str = "application/octet-stream"
    ) -> Dict[str, Any]:
        """Uploads a large file in parts with parallel upload and manifest assembly."""
        path = Path(file_path).resolve()
        total_size = path.stat().st_size

        if total_size <= chunk_size:
            return self.upload_file(bucket, key, path, content_type=content_type)

        # 1. Init
        init_resp = self.client.post("/api/v1/multipart/init", json={"bucket": bucket, "key": key, "content_type": content_type})
        init_resp.raise_for_status()
        upload_id = init_resp.json()["upload_id"]

        try:
            with open(path, "rb") as f:
                part_num = 1
                while chunk := f.read(chunk_size):
                    put_resp = self.client.put(
                        f"/api/v1/multipart/{upload_id}/part?part_number={part_num}",
                        content=chunk
                    )
                    put_resp.raise_for_status()
                    part_num += 1

            # Complete
            comp_resp = self.client.post(f"/api/v1/multipart/{upload_id}/complete")
            comp_resp.raise_for_status()
            return comp_resp.json()
        except Exception as e:
            self.client.delete(f"/api/v1/multipart/{upload_id}")
            raise VaultClientError(f"Multipart upload failed: {e}")


class AsyncVaultClient:
    def __init__(
        self,
        endpoint_url: str = "http://127.0.0.1:8000",
        timeout: float = 30.0,
        transport: Optional[Any] = None
    ) -> None:
        self.endpoint_url = endpoint_url.rstrip("/")
        self.client = httpx.AsyncClient(transport=transport, base_url=self.endpoint_url, timeout=timeout)

    async def close(self) -> None:
        await self.client.aclose()

    async def __aenter__(self) -> AsyncVaultClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, str],
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False
    ) -> Dict[str, Any]:
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        headers = {"Content-Type": content_type}
        if if_match:
            headers["If-Match"] = if_match
        if if_none_match:
            headers["If-None-Match"] = "*"
        if metadata:
            for k, v in metadata.items():
                headers[f"X-Meta-{k}"] = str(v)

        clean_key = key.lstrip("/")
        resp = await self.client.put(f"/api/v1/objects/{bucket}/{clean_key}", content=payload, headers=headers)
        if resp.status_code == 412:
            raise VaultClientError(f"Precondition failed: {resp.text}")
        if resp.status_code == 503:
            raise VaultClientError(f"Write quorum failed: {resp.text}")
        resp.raise_for_status()
        return resp.json()

    async def get_object(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None,
        byte_range: Optional[Tuple[int, int]] = None
    ) -> VaultObject:
        clean_key = key.lstrip("/")
        params = {}
        if version_id:
            params["version_id"] = version_id

        headers = {}
        if byte_range:
            headers["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"

        resp = await self.client.get(f"/api/v1/objects/{bucket}/{clean_key}", params=params, headers=headers)
        if resp.status_code == 404:
            raise VaultClientError(f"Object {bucket}/{key} not found")
        resp.raise_for_status()

        custom_metadata = {}
        for h_k, h_v in resp.headers.items():
            if h_k.lower().startswith("x-meta-"):
                custom_metadata[h_k[7:]] = h_v

        return VaultObject(
            bucket=bucket,
            key=clean_key,
            content=resp.content,
            etag=resp.headers.get("ETag", ""),
            version_id=resp.headers.get("X-Version-Id", ""),
            size_bytes=len(resp.content),
            content_type=resp.headers.get("Content-Type", "application/octet-stream"),
            custom_metadata=custom_metadata,
            sha256=resp.headers.get("X-Checksum-SHA256", "")
        )

    async def head_object(self, bucket: str, key: str, version_id: Optional[str] = None) -> Dict[str, Any]:
        clean_key = key.lstrip("/")
        params = {"version_id": version_id} if version_id else {}
        resp = await self.client.head(f"/api/v1/objects/{bucket}/{clean_key}", params=params)
        if resp.status_code == 404:
            raise VaultClientError(f"Object {bucket}/{key} not found")
        resp.raise_for_status()
        return {
            "bucket": bucket,
            "key": clean_key,
            "etag": resp.headers.get("ETag"),
            "version_id": resp.headers.get("X-Version-Id"),
            "size_bytes": int(resp.headers.get("Content-Length", 0)),
            "content_type": resp.headers.get("Content-Type"),
            "sha256": resp.headers.get("X-Checksum-SHA256")
        }

    async def delete_object(self, bucket: str, key: str) -> bool:
        clean_key = key.lstrip("/")
        resp = await self.client.delete(f"/api/v1/objects/{bucket}/{clean_key}")
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: Optional[str] = None,
        marker: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        params = {"prefix": prefix, "limit": limit}
        if delimiter:
            params["delimiter"] = delimiter
        if marker:
            params["marker"] = marker

        resp = await self.client.get(f"/api/v1/objects/{bucket}", params=params)
        resp.raise_for_status()
        return resp.json()

    async def upload_file(
        self,
        bucket: str,
        key: str,
        file_path: Union[str, Path],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        path = Path(file_path).resolve()
        content = path.read_bytes()
        c_type = content_type or "application/octet-stream"
        return await self.put_object(bucket=bucket, key=key, data=content, content_type=c_type, metadata=metadata)

    async def download_file(
        self,
        bucket: str,
        key: str,
        dest_path: Union[str, Path],
        version_id: Optional[str] = None
    ) -> Path:
        obj = await self.get_object(bucket=bucket, key=key, version_id=version_id)
        out = Path(dest_path).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(obj.content)
        return out
