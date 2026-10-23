"""PyIceberg SQL catalog (SQLite file) over a local or S3 (MinIO) warehouse."""

from __future__ import annotations

import os
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog

CATALOG_DB = "catalog.db"

# Read only when the warehouse is s3://... (the MinIO run). Defaults match docker-compose.yml.
S3_ENV = {
    "s3.endpoint": ("LAKEHOUSE_S3_ENDPOINT", "http://localhost:9000"),
    "s3.access-key-id": ("LAKEHOUSE_S3_ACCESS_KEY", "minioadmin"),
    "s3.secret-access-key": ("LAKEHOUSE_S3_SECRET_KEY", "minioadmin"),
    "s3.region": ("LAKEHOUSE_S3_REGION", "us-east-1"),
}


def is_s3(warehouse: str | Path) -> bool:
    return str(warehouse).startswith("s3://")


def warehouse_location(warehouse: str | Path) -> str:
    """The warehouse string PyIceberg gets.

    Local directories are passed as absolute POSIX-style paths, not file:// URIs: on Windows
    PyIceberg's pyarrow FileIO turns file:///C:/... into /C:/... and every write fails.
    """
    if is_s3(warehouse):
        return str(warehouse).rstrip("/")
    return Path(warehouse).resolve().as_posix()


def get_catalog(
    warehouse: str | Path,
    name: str = "lake",
    catalog_dir: str | Path | None = None,
) -> SqlCatalog:
    """Open (creating if needed) the SQLite-backed catalog for `warehouse`.

    The catalog database lives inside a local warehouse; for an S3 warehouse it lives in
    `catalog_dir` (default: ./.lakehouse), because SQLite needs a local file.
    """
    location = warehouse_location(warehouse)
    props: dict[str, str] = {"warehouse": location}
    if is_s3(warehouse):
        db_dir = Path(catalog_dir or ".lakehouse").resolve()
        props.update({key: os.environ.get(env, default) for key, (env, default) in S3_ENV.items()})
    else:
        db_dir = Path(catalog_dir).resolve() if catalog_dir else Path(location)
    db_dir.mkdir(parents=True, exist_ok=True)
    props["uri"] = f"sqlite:///{(db_dir / CATALOG_DB).as_posix()}"
    return SqlCatalog(name, **props)


def ensure_namespace(catalog: SqlCatalog, namespace: str) -> None:
    if (namespace,) not in catalog.list_namespaces():
        catalog.create_namespace(namespace)
