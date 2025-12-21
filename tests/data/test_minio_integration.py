"""
Opt-in integration test for ingestion/storage_backend.py's MinioStorageBackend.

SKIPPED BY DEFAULT: this is the one piece of this project that talks to a
real network service, so it's gated behind both a pytest marker
(`@pytest.mark.integration`, excluded from the default run via the `-m`
deselect below) and an explicit `MINIO_ENDPOINT` environment variable. CI's
default `pytest -q` run never touches this file's actual body.

To run it for real (e.g. against `docker run -p 9000:9000 minio/minio server /data`):

    docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \\
        -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
    MINIO_ENDPOINT=http://localhost:9000 pytest -m integration tests/data/test_minio_integration.py
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.integration

requires_minio = pytest.mark.skipif(
    "MINIO_ENDPOINT" not in os.environ,
    reason="set MINIO_ENDPOINT (and run a real MinIO/S3-compatible server) to exercise this test",
)


@requires_minio
def test_minio_backend_write_read_roundtrip():
    from ingestion.storage_backend import MinioStorageBackend

    backend = MinioStorageBackend(bucket=f"lakehouse-test-{uuid.uuid4().hex[:8]}")
    key = "raw/trips/integration-test.parquet"

    backend.write_bytes(key, b"integration test payload")

    assert backend.exists(key)
    assert backend.read_bytes(key) == b"integration test payload"
