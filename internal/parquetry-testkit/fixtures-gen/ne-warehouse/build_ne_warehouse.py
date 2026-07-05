#!/usr/bin/env python3
"""Build the demo NE Iceberg warehouse from the demo/data/ne GeoParquet files.

Turns the five Natural Earth GeoParquet layers under
`integrations/parquetry-geoserver/demo/data/ne/` into a static Iceberg
warehouse (format-version 3, native `geometry` columns with per-file bounds)
committed under `.../demo/data/iceberg-warehouse/`. The warehouse publishes one
dotted dataset name per layer (`ne.countries`, `ne.coastlines`, ...); the
`countries` layer is split into one data file per continent with per-file
geometry bounds in the manifest, which lets a reader prune whole files for a
bbox query.

The generator reuses the pinned `iceberg-geo-testbed` submodule's static-catalog
writer (imported via `sys.path`, like `fixtures-gen/regenerate.py`).

Determinism: two runs produce byte-identical output. The only wall-clock /
entropy inputs are the snapshot-id (`time.time`), the table-uuid
(`uuid.uuid4`), and the Avro OCF sync marker (`os.urandom`); all three are
pinned per table. The recorded table `location` is a synthetic root, not this
checkout's absolute path. The output does not depend on where the repo lives:
the reader treats `location` as a prefix to strip and re-roots each data file
onto wherever the warehouse is physically opened.

Reproduce the committed warehouse:

    cd internal/parquetry-testkit/fixtures-gen
    python3 -m venv /tmp/ne-warehouse-venv
    source /tmp/ne-warehouse-venv/bin/activate
    pip install -r requirements.txt
    python ne-warehouse/build_ne_warehouse.py

Requires the submodule to be checked out:
    git submodule update --init \
        internal/parquetry-testkit/fixtures-gen/iceberg-geo-testbed
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import geoarrow.pyarrow as ga
import shapely
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

HERE = Path(__file__).resolve().parent
SUBMODULE = HERE.parent / "iceberg-geo-testbed"
sys.path.insert(0, str(SUBMODULE))
from testbed.common import packed_xy_le  # noqa: E402
from testbed._static_catalog import write_static_catalog  # noqa: E402

# ne-warehouse -> fixtures-gen -> parquetry-testkit -> internal -> repo root.
REPO_ROOT = HERE.parents[3]
NE_DIR = REPO_ROOT / "integrations" / "parquetry-geoserver" / "demo" / "data" / "ne"
WAREHOUSE = REPO_ROOT / "integrations" / "parquetry-geoserver" / "demo" / "data" / "iceberg-warehouse"

# The GDAL-written NE layers share these column conventions (confirmed by
# reading each layer's `geo` footer metadata and arrow schema).
GEOM_COLUMN = "geom"  # GeoParquet primary_column on every layer
BBOX_COVERING_COLUMN = "geom_bbox"  # GDAL 1.1 covering struct; dropped, not an Iceberg field
CONTINENT_COLUMN = "CONTINENT"  # countries split key

# The geoarrow WKB extension serializes to Parquet's native Geometry logical
# type (BYTE_ARRAY physical, WKB encoded) with CRS84.
GEOM_EXT = ga.wkb().with_crs(ga.OGC_CRS84)

# The Iceberg schema type token for the geometry column. The parquetry reader
# maps the bare `geometry` token to a WKB geometry column and reads the CRS from
# the Parquet file's native Geometry logical type; a `geometry(OGC:CRS84)` token
# is not a type the reader recognizes.
GEOMETRY_TYPE_TOKEN = "geometry"

# The four single-file layers plus the split layer, in a stable order. The index
# of a table in this list offsets its pinned snapshot-id, keeping each table's id
# distinct and reproducible.
SIMPLE_TABLES = ["boundary_lines_land", "coastlines", "disputed_areas", "populated_places"]
SPLIT_TABLE = "countries"
TABLE_ORDER = sorted([*SIMPLE_TABLES, SPLIT_TABLE])
TABLE_INDEX = {name: index for index, name in enumerate(TABLE_ORDER)}

# A fixed point in time, in seconds. The writer derives the snapshot-id from
# int(time.time() * 1000); the per-table offset keeps ids distinct.
# 1_750_000_000 s = 2025-06-15T14:26:40Z.
BASE_EPOCH_SECONDS = 1_750_000_000

# Fixed Avro OCF sync marker (normally os.urandom(16) per file).
AVRO_SYNC_MARKER = b"\x00" * 16

# A synthetic, checkout-independent root recorded as each table's `location`.
WAREHOUSE_URI = "file:///warehouse"


@dataclass(frozen=True)
class TableModel:
    """The Iceberg-facing schema of one NE layer: the reordered arrow schema
    (attributes first, geometry last, each field id-tagged), the pyiceberg schema
    the manifest writer validates against, the metadata.json `fields` array, the
    name mapping, and the field id assigned to the geometry column."""

    arrow_schema: pa.Schema
    iceberg_schema: Schema
    schema_json_fields: list[dict]
    name_mapping: list[dict]
    geom_field_id: int


def main() -> int:
    if not (SUBMODULE / "testbed" / "__init__.py").exists():
        print(
            "submodule not checked out; run:\n"
            "  git submodule update --init "
            "internal/parquetry-testkit/fixtures-gen/iceberg-geo-testbed",
            file=sys.stderr,
        )
        return 1

    written: dict[str, tuple[Path, int]] = {}
    for name in SIMPLE_TABLES:
        source = load_ne(name)
        model = build_model(source)
        table = reorder_and_wrap(source, model)
        metadata_path = write_table(name, [(name, table)], model)
        written[name] = (metadata_path, source.num_rows)

    countries = load_ne(SPLIT_TABLE)
    model = build_model(countries)
    reordered = reorder_and_wrap(countries, model)
    parts = split_by_continent(reordered)
    metadata_path = write_table(SPLIT_TABLE, parts, model)
    written[SPLIT_TABLE] = (metadata_path, countries.num_rows)

    verify(written)
    return 0


def load_ne(layer: str) -> pa.Table:
    """Read a GDAL GeoParquet layer, dropping the bbox covering struct."""
    table = pq.read_table(NE_DIR / f"{layer}.parquet")
    kept = [name for name in table.column_names if name != BBOX_COVERING_COLUMN]
    return table.select(kept)


def build_model(source: pa.Table) -> TableModel:
    """Assign Iceberg field ids 1..N over (attributes..., geometry) and build the
    matching arrow schema, pyiceberg schema, metadata.json fields, and name
    mapping. The geometry column is placed last and typed as a native geometry.
    Because pyiceberg has no geometry type, its Python schema uses a binary
    placeholder that the manifest writer validates against."""
    attribute_names = [name for name in source.column_names if name != GEOM_COLUMN]
    ordered_names = [*attribute_names, GEOM_COLUMN]

    arrow_fields: list[pa.Field] = []
    iceberg_fields: list[NestedField] = []
    schema_json_fields: list[dict] = []
    name_mapping: list[dict] = []

    for field_id, name in enumerate(ordered_names, start=1):
        source_field = source.schema.field(name)
        required = not source_field.nullable
        arrow_meta = {"PARQUET:field_id": str(field_id)}
        if name == GEOM_COLUMN:
            arrow_fields.append(pa.field(name, GEOM_EXT, nullable=source_field.nullable, metadata=arrow_meta))
            iceberg_fields.append(NestedField(field_id, name, BinaryType(), required=required))
            type_token = GEOMETRY_TYPE_TOKEN
        else:
            type_token, iceberg_type = iceberg_type_of(source_field.type)
            arrow_fields.append(pa.field(name, source_field.type, nullable=source_field.nullable, metadata=arrow_meta))
            iceberg_fields.append(NestedField(field_id, name, iceberg_type, required=required))
        schema_json_fields.append({"id": field_id, "name": name, "required": required, "type": type_token})
        name_mapping.append({"field-id": field_id, "names": [name]})

    return TableModel(
        arrow_schema=pa.schema(arrow_fields),
        iceberg_schema=Schema(*iceberg_fields),
        schema_json_fields=schema_json_fields,
        name_mapping=name_mapping,
        geom_field_id=len(ordered_names),
    )


def reorder_and_wrap(source: pa.Table, model: TableModel) -> pa.Table:
    """Project the source into the model's column order, wrapping the geometry as
    the geoarrow WKB extension array so it writes as a native Geometry column."""
    columns: dict[str, object] = {}
    for name in model.arrow_schema.names:
        if name == GEOM_COLUMN:
            columns[name] = GEOM_EXT.wrap_array(source.column(GEOM_COLUMN).combine_chunks())
        else:
            columns[name] = source.column(name)
    return pa.table(columns, schema=model.arrow_schema)


def split_by_continent(table: pa.Table) -> list[tuple[str, pa.Table]]:
    """One (slug, rows) part per distinct CONTINENT value, continents sorted so the
    part order is stable across runs."""
    values = sorted(set(table.column(CONTINENT_COLUMN).to_pylist()))
    parts: list[tuple[str, pa.Table]] = []
    for value in values:
        mask = pc.equal(table.column(CONTINENT_COLUMN), value)
        parts.append((slugify(value), table.filter(mask)))
    return parts


def write_table(name: str, parts: list[tuple[str, pa.Table]], model: TableModel) -> Path:
    """Write each part's Parquet file, then the static catalog (metadata.json,
    manifest, manifest-list) recording per-file geometry bounds keyed by the
    geometry field id."""
    table_root = WAREHOUSE / "ne" / name
    shutil.rmtree(table_root, ignore_errors=True)

    data_files: list[dict] = []
    for part_name, part in parts:
        relative_path = f"data/{part_name}.parquet"
        out = table_root / relative_path
        out.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(part, out, compression="zstd")
        lower, upper = geometry_bounds(part)
        data_files.append(
            {
                "path": relative_path,
                "size": out.stat().st_size,
                "rows": part.num_rows,
                "lower": {model.geom_field_id: lower},
                "upper": {model.geom_field_id: upper},
            }
        )
    return write_catalog(name, table_root, model, data_files)


def write_catalog(name: str, table_root: Path, model: TableModel, data_files: list[dict]) -> Path:
    """Write the static Iceberg catalog for one table, pinning the three
    wall-clock / entropy inputs so the manifests and metadata are reproducible."""
    epoch_seconds = float(BASE_EPOCH_SECONDS + TABLE_INDEX[name])
    with mock.patch("time.time", return_value=epoch_seconds), mock.patch(
        "uuid.uuid4", return_value=stable_uuid(name)
    ), mock.patch("os.urandom", return_value=AVRO_SYNC_MARKER):
        return write_static_catalog(
            table_root=table_root,
            iceberg_schema=model.iceberg_schema,
            schema_json_fields=model.schema_json_fields,
            name_mapping=model.name_mapping,
            data_files=data_files,
            format_version_in_metadata=3,
            location_uri=f"{WAREHOUSE_URI}/ne/{name}",
        )


def geometry_bounds(part: pa.Table) -> tuple[bytes, bytes]:
    """Per-file lower/upper geometry bounds as packed little-endian XY doubles."""
    wkb_values = part.column(GEOM_COLUMN).combine_chunks().storage.to_pylist()
    xmin, ymin, xmax, ymax = shapely.total_bounds(shapely.from_wkb(wkb_values))
    return packed_xy_le(xmin, ymin), packed_xy_le(xmax, ymax)


def iceberg_type_of(arrow_type: pa.DataType) -> tuple[str, object]:
    """Map an NE attribute's arrow type to its (metadata.json token, pyiceberg
    type). NE attributes are only int32, int64, double, and string."""
    if pa.types.is_int32(arrow_type):
        return "int", IntegerType()
    if pa.types.is_int64(arrow_type):
        return "long", LongType()
    if pa.types.is_float64(arrow_type):
        return "double", DoubleType()
    if pa.types.is_string(arrow_type):
        return "string", StringType()
    raise ValueError(f"no Iceberg type mapping for arrow type {arrow_type}")


def slugify(value: str) -> str:
    """A lowercase, filesystem- and URI-safe token for a partition value, e.g.
    'Seven seas (open ocean)' -> 'seven_seas_open_ocean'."""
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def stable_uuid(name: str) -> uuid.UUID:
    """A deterministic table-uuid derived from the table name."""
    digest = hashlib.sha256(name.encode("utf-8")).digest()[:16]
    return uuid.UUID(bytes=digest)


def verify(written: dict[str, tuple[Path, int]]) -> None:
    """Self-checks: every table's metadata.json parses as a format-version 3
    table, and the snapshot's total-records equals the source layer's row count."""
    for name, (metadata_path, source_rows) in sorted(written.items()):
        metadata = json.loads(metadata_path.read_text())
        if metadata["format-version"] != 3:
            raise SystemExit(f"{name}: expected format-version 3, got {metadata['format-version']}")
        snapshot = metadata["snapshots"][0]
        recorded_rows = int(snapshot["summary"]["total-records"])
        if recorded_rows != source_rows:
            raise SystemExit(f"{name}: recorded {recorded_rows} rows, source has {source_rows}")
        print(f"ok  ne.{name:<20} rows={source_rows:>5}  {metadata_path}")
    total = sum(rows for _, rows in written.values())
    print(f"\ndone: {len(written)} tables, {total} rows total, under {WAREHOUSE}")


if __name__ == "__main__":
    raise SystemExit(main())
