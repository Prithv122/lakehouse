"""S3 smoke checks against the MinIO service from docker-compose.yml.

Deselected by default. Run with `docker compose up -d` first, then `uv run pytest -m minio`.
Skips cleanly when the endpoint is not reachable.
"""

from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from lakehouse.catalog import S3_ENV, ensure_namespace, get_catalog

pytestmark = pytest.mark.minio

BUCKET = "lake"


def _setting(key: str) -> str:
    env, default = S3_ENV[key]
    return os.environ.get(env, default)


@pytest.fixture(scope="module")
def s3_options() -> dict[str, str]:
    endpoint = _setting("s3.endpoint")
    parsed = urlparse(endpoint)
    host, port = parsed.hostname or "localhost", parsed.port or 9000
    try:
        with socket.create_connection((host, port), timeout=2):
            pass
    except OSError:
        pytest.skip(f"MinIO not reachable at {endpoint} (run `docker compose up -d`)")
    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": _setting("s3.access-key-id"),
        "AWS_SECRET_ACCESS_KEY": _setting("s3.secret-access-key"),
        "AWS_REGION": _setting("s3.region"),
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _sample() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["a", "b", "c"], pa.string()),
        }
    )


def test_iceberg_roundtrip_on_s3(s3_options: dict[str, str], tmp_path: Path) -> None:
    run = uuid.uuid4().hex[:8]
    catalog = get_catalog(f"s3://{BUCKET}/warehouse/{run}", catalog_dir=tmp_path)
    ensure_namespace(catalog, "smoke")
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
    )
    table = catalog.create_table(f"smoke.t_{run}", schema=schema)
    table.append(_sample())
    got = table.scan().to_arrow().sort_by("id")
    assert got.num_rows == 3
    assert got["id"].to_pylist() == [1, 2, 3]
    assert got["name"].to_pylist() == ["a", "b", "c"]


def test_delta_roundtrip_on_s3(s3_options: dict[str, str]) -> None:
    uri = f"s3://{BUCKET}/delta/{uuid.uuid4().hex[:8]}"
    write_deltalake(uri, _sample(), mode="append", storage_options=s3_options)
    write_deltalake(uri, _sample(), mode="append", storage_options=s3_options)
    dtab = DeltaTable(uri, storage_options=s3_options)
    assert dtab.version() == 1
    got = dtab.to_pyarrow_table()
    assert got.num_rows == 6
    first = DeltaTable(uri, version=0, storage_options=s3_options).to_pyarrow_table()
    assert first.num_rows == 3
