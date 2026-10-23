from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from lakehouse.catalog import ensure_namespace, get_catalog


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    path = tmp_path / "warehouse"
    path.mkdir()
    return path


@pytest.fixture
def catalog(warehouse: Path) -> SqlCatalog:
    cat = get_catalog(warehouse)
    for namespace in ("lake", "bench"):
        ensure_namespace(cat, namespace)
    return cat
