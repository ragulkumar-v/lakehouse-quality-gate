"""
Storage backend abstraction for the lake's object storage layer.

Production deployments point this at a MinIO (or any S3-compatible) bucket.
Local development and the automated test suite use a plain filesystem
directory instead, so the pipeline can be built and verified with zero
external services. Both backends implement the exact same tiny interface,
so nothing above this layer (ingestion, pyiceberg's FileIO, dbt) needs to
know or care which one is active.

Select the backend with the LAKE_BACKEND env var:
    LAKE_BACKEND=local   -> LocalStorageBackend   (default, offline-safe)
    LAKE_BACKEND=minio   -> MinioStorageBackend   (requires a real MinIO/S3 endpoint)
"""

from __future__ import annotations

import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path


class StorageBackend(ABC):
    """Minimal object-storage interface the pipeline depends on."""

    @abstractmethod
    def warehouse_uri(self) -> str:
        """URI pyiceberg/pyarrow should use as the table warehouse root."""

    @abstractmethod
    def write_bytes(self, key: str, data: bytes) -> str:
        """Write raw bytes at `key`, returning the fully qualified path/URI."""

    @abstractmethod
    def read_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...


class LocalStorageBackend(StorageBackend):
    """Filesystem-backed stand-in for object storage. Used offline / in CI."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def warehouse_uri(self) -> str:
        return str(self.root.resolve())

    def write_bytes(self, key: str, data: bytes) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path.resolve())

    def read_bytes(self, key: str) -> bytes:
        return (self.root / key).read_bytes()

    def exists(self, key: str) -> bool:
        return (self.root / key).exists()

    def reset(self) -> None:
        """Test/chaos helper: wipe and recreate the storage root."""
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)


class MinioStorageBackend(StorageBackend):
    """
    Real S3-compatible backend for MinIO, used in the docker-compose full
    stack (see docker-compose.yml) and production. Not exercised by the
    default offline test suite -- see tests/data/test_minio_integration.py,
    which is skipped unless MINIO_ENDPOINT is reachable.
    """

    def __init__(
        self,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
    ):
        import boto3  # local import: keep boto3 optional for local-only usage

        self.endpoint_url = endpoint_url or os.environ["MINIO_ENDPOINT"]
        self.bucket = bucket or os.environ.get("MINIO_BUCKET", "lakehouse")
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=access_key or os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=secret_key or os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        )
        existing = {b["Name"] for b in self.client.list_buckets().get("Buckets", [])}
        if self.bucket not in existing:
            self.client.create_bucket(Bucket=self.bucket)

    def warehouse_uri(self) -> str:
        return f"s3://{self.bucket}"

    def write_bytes(self, key: str, data: bytes) -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"

    def read_bytes(self, key: str) -> bytes:
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False


def get_storage_backend(lake_root: str | Path) -> StorageBackend:
    """Factory chosen by LAKE_BACKEND env var. Defaults to local (offline-safe)."""
    backend = os.environ.get("LAKE_BACKEND", "local").lower()
    if backend == "minio":
        return MinioStorageBackend()
    return LocalStorageBackend(lake_root)
