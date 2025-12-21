"""Unit tests for ingestion/storage_backend.py."""

from __future__ import annotations

import pytest

from ingestion.storage_backend import LocalStorageBackend, MinioStorageBackend, get_storage_backend


def test_local_backend_write_read_roundtrip(tmp_path):
    backend = LocalStorageBackend(tmp_path / "lake")
    path = backend.write_bytes("raw/trips/part-0.parquet", b"hello world")

    assert backend.exists("raw/trips/part-0.parquet")
    assert backend.read_bytes("raw/trips/part-0.parquet") == b"hello world"
    assert path.endswith("part-0.parquet")


def test_local_backend_exists_is_false_for_missing_key(tmp_path):
    backend = LocalStorageBackend(tmp_path / "lake")
    assert backend.exists("does/not/exist.parquet") is False


def test_local_backend_reset_wipes_and_recreates_root(tmp_path):
    backend = LocalStorageBackend(tmp_path / "lake")
    backend.write_bytes("a.txt", b"data")
    assert backend.exists("a.txt")

    backend.reset()

    assert not backend.exists("a.txt")
    assert backend.root.is_dir()


def test_warehouse_uri_is_an_absolute_path(tmp_path):
    backend = LocalStorageBackend(tmp_path / "lake")
    uri = backend.warehouse_uri()
    assert uri == str((tmp_path / "lake").resolve())


def test_factory_defaults_to_local_backend(tmp_path, monkeypatch):
    monkeypatch.delenv("LAKE_BACKEND", raising=False)
    backend = get_storage_backend(tmp_path / "lake")
    assert isinstance(backend, LocalStorageBackend)


def test_factory_is_case_insensitive(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_BACKEND", "LOCAL")
    backend = get_storage_backend(tmp_path / "lake")
    assert isinstance(backend, LocalStorageBackend)


def test_minio_backend_requires_endpoint_configuration(monkeypatch):
    """
    Fully offline check: constructing MinioStorageBackend without an
    endpoint configured must fail fast and clearly, rather than silently
    trying (and hanging on) a network call.
    """
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    with pytest.raises(KeyError):
        MinioStorageBackend()
