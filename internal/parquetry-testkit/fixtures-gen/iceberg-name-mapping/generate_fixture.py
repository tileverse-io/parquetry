#!/usr/bin/env python3
"""Deterministically generate the vendored iceberg-name-mapping corpus.

One v2 table (`migrated`) in the add_files migration shape: the data files
embed NO Parquet field ids, and the table metadata holds a
`schema.name-mapping.default` property whose names match the physical
columns. Reuses the static-catalog writer from the iceberg-geo-testbed
submodule.

    git submodule update --init internal/parquetry-testkit/fixtures-gen/iceberg-geo-testbed
    python3 -m venv /tmp/iceberg-fixtures-venv
    source /tmp/iceberg-fixtures-venv/bin/activate
    pip install -r internal/parquetry-testkit/fixtures-gen/requirements.txt
    python internal/parquetry-testkit/fixtures-gen/iceberg-name-mapping/generate_fixture.py
"""

from __future__ import annotations

import hashlib
import random
import shutil
import sys
import uuid
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
SUBMODULE = HERE.parent / "iceberg-geo-testbed"
sys.path.insert(0, str(SUBMODULE))

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from pyiceberg.schema import Schema  # noqa: E402
from pyiceberg.types import IntegerType, NestedField, StringType  # noqa: E402

from testbed._static_catalog import write_static_catalog  # noqa: E402
from testbed.common import stable_seed  # noqa: E402

TABLE = "migrated"
DEST = HERE.parent.parent / "src" / "main" / "resources" / "iceberg-name-mapping" / TABLE
PARTS = ["alpha", "bravo", "charlie"]
ROWS_PER_FILE = 1000

# The writer's wall-clock / entropy inputs, pinned the same way
# ../regenerate.py pins them (epoch offset keeps the snapshot id distinct
# from the geo-testbed catalogs; the table uuid derives from the corpus name).
EPOCH_SECONDS = 1_700_000_100.0
AVRO_SYNC_MARKER = b"\x00" * 16
TABLE_UUID = uuid.UUID(bytes=hashlib.sha256(b"iceberg-name-mapping/migrated").digest()[:16])

PY_SCHEMA = Schema(
    NestedField(1, "id", StringType(), required=False),
    NestedField(2, "n", IntegerType(), required=False),
)

# No PARQUET:field_id metadata on any field: id-less data is the point.
ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=True),
        pa.field("n", pa.int32(), nullable=True),
    ]
)


def _write_parquet(part: str) -> Path:
    rng = random.Random(stable_seed(part))
    ids = [f"{part}-{i}" for i in range(ROWS_PER_FILE)]
    ns = [rng.randint(0, 1_000_000) for _ in range(ROWS_PER_FILE)]
    table = pa.table(
        {"id": pa.array(ids, type=pa.string()), "n": pa.array(ns, type=pa.int32())},
        schema=ARROW_SCHEMA,
    )
    out_dir = DEST / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{part}.parquet"
    pq.write_table(table, out, compression="zstd", store_schema=True, write_statistics=True)
    return out


def build() -> Path:
    if DEST.exists():
        shutil.rmtree(DEST)
    data_files = []
    for part in PARTS:
        p = _write_parquet(part)
        first_id, last_id = f"{part}-0".encode(), f"{part}-{ROWS_PER_FILE - 1}".encode()
        data_files.append(
            {
                "path": f"data/{part}.parquet",
                "size": p.stat().st_size,
                "rows": ROWS_PER_FILE,
                "lower": {1: first_id},
                "upper": {1: last_id},
                "value_counts": {1: ROWS_PER_FILE, 2: ROWS_PER_FILE},
                "null_value_counts": {1: 0, 2: 0},
            }
        )
    with mock.patch("time.time", return_value=EPOCH_SECONDS), mock.patch(
        "uuid.uuid4", return_value=TABLE_UUID
    ), mock.patch("os.urandom", return_value=AVRO_SYNC_MARKER):
        return write_static_catalog(
            table_root=DEST,
            iceberg_schema=PY_SCHEMA,
            schema_json_fields=[
                {"id": 1, "name": "id", "required": False, "type": "string"},
                {"id": 2, "name": "n", "required": False, "type": "int"},
            ],
            name_mapping=[
                {"field-id": 1, "names": ["id"]},
                {"field-id": 2, "names": ["n"]},
            ],
            data_files=data_files,
            format_version_in_metadata=2,
            location_uri=f"file:///iceberg-name-mapping/{TABLE}",
        )


if __name__ == "__main__":
    print(f"metadata.json: {build()}")
