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

Two layers showcase Iceberg features a demo reader exercises:

  - `populated_places` shows schema evolution. The Parquet files stay as
    written; the table schema renames one attribute and keeps its field id.
    Id-based resolution maps the file column onto the new name. The schema
    also appends a field no file has (it reads as null).
  - `disputed_areas` shows merge-on-read position deletes: a delete file drops
    the first five rows of its single data file, and the snapshot references a
    delete manifest alongside the data manifest.

A `bonus/` tree copies three vendored Iceberg fixtures (deletion vectors,
equality deletes, native-geometry row lineage). The warehouse then also
publishes `bonus.deletion_vectors`, `bonus.equality`, and
`bonus.geometry_lineage`.

The generator reuses the pinned `iceberg-geo-testbed` submodule's static-catalog
writer (imported via `sys.path`, like `fixtures-gen/regenerate.py`). The
position-delete manifest is written here, not in the submodule, by extending the
submodule's V3 manifest writers.

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
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import geoarrow.pyarrow as ga
import shapely
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    read_manifest_list,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
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
from testbed._static_catalog import (  # noqa: E402
    write_static_catalog,
    _ManifestListWriterV3,
    _ManifestWriterV3,
)

# ne-warehouse -> fixtures-gen -> parquetry-testkit -> internal -> repo root.
REPO_ROOT = HERE.parents[3]
NE_DIR = REPO_ROOT / "integrations" / "parquetry-geoserver" / "demo" / "data" / "ne"
WAREHOUSE = REPO_ROOT / "integrations" / "parquetry-geoserver" / "demo" / "data" / "iceberg-warehouse"

# Vendored Iceberg fixtures copied verbatim into the warehouse under bonus/.
# They are path-portable (locations recorded relative to a synthetic root) and
# publish one bonus.<name> dataset each.
RESOURCES = HERE.parent.parent / "src" / "main" / "resources"
BONUS_TABLES = {
    "deletion_vectors": "iceberg-deletes/deletion-vectors",
    "equality": "iceberg-deletes/equality",
    "geometry_lineage": "iceberg-geo-testbed/v3_geometry_lineage",
}

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

# Schema evolution on populated_places (reader-visible only; the Parquet files
# are untouched). The renamed attribute keeps its field id; the appended field
# has no column in any file and reads as null.
EVOLVED_TABLE = "populated_places"
RENAMED_FIELD_FROM = "name"
RENAMED_FIELD_TO = "place_name"
APPENDED_FIELD_NAME = "local_note"

# Merge-on-read position deletes on disputed_areas: drop the first five row
# positions of its single data file. Iceberg reserves these field ids for a
# position-delete file's two columns.
DELETE_TABLE = "disputed_areas"
DELETED_POSITIONS = [0, 1, 2, 3, 4]
DELETE_FILE_PATH_FIELD_ID = 2147483546
DELETE_POS_FIELD_ID = 2147483545

# countries is split one data file per continent. Antarctica has a single row.
# A one-row data file crashes the engine's multi-file read path today, which is
# why its row is folded into South America (its geographic neighbor): the fold
# keeps the table multi-file with per-file geometry bounds for pruning. Restore
# the natural per-continent split once multi-file reads handle single-row files.
FOLD_SOURCE_CONTINENT = "antarctica"
FOLD_TARGET_CONTINENT = "south_america"

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
    name mapping, the field id assigned to the geometry column, and the
    `last-column-id` to record (None derives it from the highest field id)."""

    arrow_schema: pa.Schema
    iceberg_schema: Schema
    schema_json_fields: list[dict]
    name_mapping: list[dict]
    geom_field_id: int
    last_column_id: int | None = None


def main() -> int:
    if not (SUBMODULE / "testbed" / "__init__.py").exists():
        print(
            "submodule not checked out; run:\n"
            "  git submodule update --init "
            "internal/parquetry-testkit/fixtures-gen/iceberg-geo-testbed",
            file=sys.stderr,
        )
        return 1
    if not NE_DIR.is_dir():
        print(f"NE source layers not found under {NE_DIR}", file=sys.stderr)
        return 1

    written: dict[str, tuple[Path, int]] = {}
    for name in SIMPLE_TABLES:
        source = load_ne(name)
        model = build_model(source)
        if name == EVOLVED_TABLE:
            model = evolve_schema(model)
        table = reorder_and_wrap(source, model)
        metadata_path = write_table(name, [(name, table)], model)
        written[name] = (metadata_path, source.num_rows)

    countries = load_ne(SPLIT_TABLE)
    model = build_model(countries)
    reordered = reorder_and_wrap(countries, model)
    parts = fold_sparse_continents(split_by_continent(reordered))
    metadata_path = write_table(SPLIT_TABLE, parts, model)
    written[SPLIT_TABLE] = (metadata_path, countries.num_rows)

    copy_bonus_tables()
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


def evolve_schema(model: TableModel) -> TableModel:
    """Rename one attribute and append a never-written field to the
    reader-visible schema, leaving the Parquet files untouched. The rename keeps
    the field id, which lets a reader resolve the file column by id onto the new
    name. The appended field has no column in any file and reads as null. The
    name mapping is unchanged: it still maps the physical file column name to the
    renamed field's id."""
    fields = [dict(field) for field in model.schema_json_fields]
    for field in fields:
        if field["name"] == RENAMED_FIELD_FROM:
            field["name"] = RENAMED_FIELD_TO
    appended_id = max(field["id"] for field in fields) + 1
    fields.append({"id": appended_id, "name": APPENDED_FIELD_NAME, "required": False, "type": "string"})
    return replace(model, schema_json_fields=fields, last_column_id=appended_id)


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


def fold_sparse_continents(parts: list[tuple[str, pa.Table]]) -> list[tuple[str, pa.Table]]:
    """Merge the single-row Antarctica part into South America, dropping the
    Antarctica part. A one-row data file crashes the engine's multi-file read
    path today; folding Antarctica into its geographic neighbor keeps the table
    multi-file with per-file geometry bounds while avoiding the one-row file.
    Restore the natural per-continent split once multi-file reads handle
    single-row files."""
    by_slug = dict(parts)
    if FOLD_SOURCE_CONTINENT not in by_slug or FOLD_TARGET_CONTINENT not in by_slug:
        return parts
    merged = pa.concat_tables([by_slug[FOLD_TARGET_CONTINENT], by_slug[FOLD_SOURCE_CONTINENT]])
    folded: list[tuple[str, pa.Table]] = []
    for slug, part in parts:
        if slug == FOLD_SOURCE_CONTINENT:
            continue
        folded.append((slug, merged if slug == FOLD_TARGET_CONTINENT else part))
    return folded


def write_table(name: str, parts: list[tuple[str, pa.Table]], model: TableModel) -> Path:
    """Write each part's Parquet file, then the static catalog (metadata.json,
    manifest, manifest-list) recording per-file geometry bounds keyed by the
    geometry field id. The delete-showcase table also writes a position-delete
    file and a delete manifest."""
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
    if name == DELETE_TABLE:
        return write_catalog_with_deletes(name, table_root, model, data_files)
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
            last_column_id_override=model.last_column_id,
        )


def write_catalog_with_deletes(name: str, table_root: Path, model: TableModel, data_files: list[dict]) -> Path:
    """Write the catalog for the merge-on-read delete showcase: the same data
    catalog plus a position-delete file that drops the first rows of the single
    data file, referenced by a delete manifest in the same snapshot. Pins the
    same wall-clock / entropy inputs as the plain path."""
    location_uri = f"{WAREHOUSE_URI}/ne/{name}"
    data_file_path = f"{location_uri}/{data_files[0]['path']}"
    delete_relative = f"data/{name}-deletes.parquet"
    delete_out = table_root / delete_relative
    delete_size = write_position_delete_file(delete_out, data_file_path, DELETED_POSITIONS)
    delete_files = [{"path": delete_relative, "size": delete_size, "rows": len(DELETED_POSITIONS)}]

    epoch_seconds = float(BASE_EPOCH_SECONDS + TABLE_INDEX[name])
    with mock.patch("time.time", return_value=epoch_seconds), mock.patch(
        "uuid.uuid4", return_value=stable_uuid(name)
    ), mock.patch("os.urandom", return_value=AVRO_SYNC_MARKER):
        return write_catalog_with_position_deletes(
            table_root=table_root,
            model=model,
            data_files=data_files,
            delete_files=delete_files,
            location_uri=location_uri,
        )


class _DeleteManifestWriterV3(_ManifestWriterV3):
    """A V3 manifest writer whose content is DELETES. The submodule's V3 writer
    is content DATA; a delete manifest is otherwise identical, its entries just
    reference delete-content data files."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES


def write_position_delete_file(path: Path, data_file_path: str, positions: list[int]) -> int:
    """Write an Iceberg position-delete Parquet file dropping `positions` of the
    data file at `data_file_path`, and return its size. The `file_path` column
    repeats the referenced data file's location exactly as the manifest records
    it, which is how the reader matches a delete row to its data file."""
    schema = pa.schema(
        [
            pa.field(
                "file_path",
                pa.string(),
                nullable=False,
                metadata={"PARQUET:field_id": str(DELETE_FILE_PATH_FIELD_ID)},
            ),
            pa.field(
                "pos",
                pa.int64(),
                nullable=False,
                metadata={"PARQUET:field_id": str(DELETE_POS_FIELD_ID)},
            ),
        ]
    )
    table = pa.table(
        {"file_path": [data_file_path] * len(positions), "pos": [int(p) for p in positions]},
        schema=schema,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")
    return path.stat().st_size


def write_catalog_with_position_deletes(
    *,
    table_root: Path,
    model: TableModel,
    data_files: list[dict],
    delete_files: list[dict],
    location_uri: str,
) -> Path:
    """Write metadata.json + a data manifest + a delete manifest + a manifest-list
    referencing both, for a format-version 3 table with merge-on-read position
    deletes. Mirrors the submodule's V3 data-only writer and adds the delete
    manifest at the same sequence number, which is what makes the delete apply
    to the data file."""
    location_uri = location_uri.rstrip("/")
    meta_dir = table_root / "metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)

    snapshot_id = int(time.time() * 1000)
    sequence_number = 1
    io = PyArrowFileIO()
    schema_override = json.dumps({"type": "struct", "schema-id": 0, "fields": model.schema_json_fields})

    data_manifest = write_data_manifest(
        io, meta_dir, snapshot_id, sequence_number, model, data_files, schema_override, location_uri
    )
    delete_manifest = write_delete_manifest(
        io, meta_dir, snapshot_id, sequence_number, model, delete_files, schema_override, location_uri
    )

    manifest_list_path = meta_dir / f"snap-{snapshot_id}-manifest-list.avro"
    manifest_list_writer = _ManifestListWriterV3(
        output_file=io.new_output(str(manifest_list_path)),
        snapshot_id=snapshot_id,
        parent_snapshot_id=None,
        sequence_number=sequence_number,
        compression="null",
    )
    with manifest_list_writer as writer:
        writer.add_manifests([data_manifest, delete_manifest])

    metadata = build_metadata_with_deletes(
        model=model,
        location_uri=location_uri,
        snapshot_id=snapshot_id,
        sequence_number=sequence_number,
        manifest_list_uri=f"{location_uri}/metadata/{manifest_list_path.name}",
        data_files=data_files,
        delete_files=delete_files,
    )
    metadata_json_path = meta_dir / "v1.metadata.json"
    metadata_json_path.write_text(json.dumps(metadata, indent=2))
    return metadata_json_path


def write_data_manifest(
    io: PyArrowFileIO,
    meta_dir: Path,
    snapshot_id: int,
    sequence_number: int,
    model: TableModel,
    data_files: list[dict],
    schema_override: str,
    location_uri: str,
):
    """Write the V3 data manifest, one ADDED entry per data file with per-file
    geometry bounds and the file's first-row-id, and return its ManifestFile
    bound to the location the manifest-list records."""
    manifest_path = meta_dir / f"snap-{snapshot_id}-manifest.avro"
    writer = _ManifestWriterV3(
        spec=PartitionSpec(),
        schema=model.iceberg_schema,
        output_file=io.new_output(str(manifest_path)),
        snapshot_id=snapshot_id,
        avro_compression="null",
        schema_override_json=schema_override,
    )
    with writer as manifest:
        for index, data_file in enumerate(data_files):
            entry = ManifestEntry.from_args(
                _table_format_version=3,
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                sequence_number=sequence_number,
                file_sequence_number=sequence_number,
                data_file=DataFile.from_args(
                    _table_format_version=3,
                    content=DataFileContent.DATA,
                    file_path=f"{location_uri}/{data_file['path']}",
                    file_format=FileFormat.PARQUET,
                    partition=Record(),
                    record_count=data_file["rows"],
                    file_size_in_bytes=data_file["size"],
                    lower_bounds=data_file["lower"],
                    upper_bounds=data_file["upper"],
                    first_row_id=sum(preceding["rows"] for preceding in data_files[:index]),
                ),
            )
            manifest.add_entry(entry)
    return bind_manifest_location(writer, location_uri, manifest_path)


def write_delete_manifest(
    io: PyArrowFileIO,
    meta_dir: Path,
    snapshot_id: int,
    sequence_number: int,
    model: TableModel,
    delete_files: list[dict],
    schema_override: str,
    location_uri: str,
):
    """Write the V3 delete manifest, one ADDED entry per position-delete file at
    the data manifest's sequence number, and return its ManifestFile bound to the
    location the manifest-list records."""
    manifest_path = meta_dir / f"snap-{snapshot_id}-delete-manifest.avro"
    writer = _DeleteManifestWriterV3(
        spec=PartitionSpec(),
        schema=model.iceberg_schema,
        output_file=io.new_output(str(manifest_path)),
        snapshot_id=snapshot_id,
        avro_compression="null",
        schema_override_json=schema_override,
    )
    with writer as manifest:
        for delete_file in delete_files:
            entry = ManifestEntry.from_args(
                _table_format_version=3,
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                sequence_number=sequence_number,
                file_sequence_number=sequence_number,
                data_file=DataFile.from_args(
                    _table_format_version=3,
                    content=DataFileContent.POSITION_DELETES,
                    file_path=f"{location_uri}/{delete_file['path']}",
                    file_format=FileFormat.PARQUET,
                    partition=Record(),
                    record_count=delete_file["rows"],
                    file_size_in_bytes=delete_file["size"],
                    first_row_id=None,
                ),
            )
            manifest.add_entry(entry)
    return bind_manifest_location(writer, location_uri, manifest_path)


def bind_manifest_location(writer, location_uri: str, manifest_path: Path):
    """Finish a manifest and record its location as the manifest-list URI, not
    the local write path. The URI form lets an engine reading the catalog from
    another location resolve it. ManifestFile.manifest_path has no setter; it is
    a Record backed by a mutable list whose index 0 is the path."""
    manifest = writer.to_manifest_file()
    manifest._data[0] = f"{location_uri}/metadata/{manifest_path.name}"
    return manifest


def build_metadata_with_deletes(
    *,
    model: TableModel,
    location_uri: str,
    snapshot_id: int,
    sequence_number: int,
    manifest_list_uri: str,
    data_files: list[dict],
    delete_files: list[dict],
) -> dict:
    """The metadata.json for a V3 table with position deletes, matching the
    submodule's V3 data-only shape with the snapshot summary extended to account
    for the delete file. Position deletes do not change the data record count;
    the reader subtracts them when it scans."""
    total_records = sum(data_file["rows"] for data_file in data_files)
    total_files = len(data_files)
    total_size = sum(data_file["size"] for data_file in data_files)
    delete_records = sum(delete_file["rows"] for delete_file in delete_files)
    delete_count = len(delete_files)
    delete_size = sum(delete_file["size"] for delete_file in delete_files)
    snapshot = {
        "snapshot-id": snapshot_id,
        "sequence-number": sequence_number,
        "timestamp-ms": snapshot_id,
        "manifest-list": manifest_list_uri,
        "schema-id": 0,
        "summary": {
            "operation": "overwrite",
            "added-data-files": str(total_files),
            "added-records": str(total_records),
            "added-files-size": str(total_size),
            "added-delete-files": str(delete_count),
            "added-position-delete-files": str(delete_count),
            "added-position-deletes": str(delete_records),
            "added-delete-files-size": str(delete_size),
            "total-data-files": str(total_files),
            "total-records": str(total_records),
            "total-files-size": str(total_size),
            "total-delete-files": str(delete_count),
            "total-equality-deletes": "0",
            "total-position-deletes": str(delete_records),
            "changed-partition-count": "1",
        },
        "first-row-id": 0,
        "added-rows": total_records,
    }
    return {
        "format-version": 3,
        "table-uuid": str(uuid.uuid4()),
        "location": location_uri,
        "last-sequence-number": sequence_number,
        "last-updated-ms": snapshot_id,
        "last-column-id": resolve_last_column_id(model),
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": model.schema_json_fields}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {"schema.name-mapping.default": json.dumps(model.name_mapping)},
        "current-snapshot-id": snapshot_id,
        "refs": {"main": {"snapshot-id": snapshot_id, "type": "branch"}},
        "snapshots": [snapshot],
        "snapshot-log": [{"snapshot-id": snapshot_id, "timestamp-ms": snapshot_id}],
        "metadata-log": [],
        "next-row-id": total_records,
        "statistics": [],
        "partition-statistics": [],
        "row-lineage": False,
    }


def resolve_last_column_id(model: TableModel) -> int:
    """The table's last-column-id: the model's explicit value, or the highest
    field id when it has none."""
    if model.last_column_id is not None:
        return model.last_column_id
    return max(field["id"] for field in model.schema_json_fields)


def copy_bonus_tables() -> None:
    """Copy the vendored bonus fixtures verbatim into the warehouse under bonus/."""
    for name, source in BONUS_TABLES.items():
        target = WAREHOUSE / "bonus" / name
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(RESOURCES / source, target)


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
    table with the source's row count, plus the showcase-specific properties -
    countries per-file geometry bounds, the disputed_areas position deletes, and
    the populated_places evolved schema."""
    for name, (metadata_path, source_rows) in sorted(written.items()):
        metadata = json.loads(metadata_path.read_text())
        if metadata["format-version"] != 3:
            raise SystemExit(f"{name}: expected format-version 3, got {metadata['format-version']}")
        snapshot = metadata["snapshots"][0]
        recorded_rows = int(snapshot["summary"]["total-records"])
        if recorded_rows != source_rows:
            raise SystemExit(f"{name}: recorded {recorded_rows} rows, source has {source_rows}")
        print(f"ok  ne.{name:<20} rows={source_rows:>5}  {metadata_path}")

    verify_countries_bounds(written[SPLIT_TABLE][0])
    verify_disputed_areas_deletes(written[DELETE_TABLE])
    verify_populated_places_schema(written[EVOLVED_TABLE][0])

    total = sum(rows for _, rows in written.values())
    print(f"\ndone: {len(written)} tables, {total} rows total, plus {len(BONUS_TABLES)} bonus tables, under {WAREHOUSE}")


def verify_countries_bounds(metadata_path: Path) -> None:
    """Assert every countries data file records lower and upper geometry bounds -
    the property the per-continent split exists for."""
    metadata = json.loads(metadata_path.read_text())
    geom_field_id = geometry_field_id(metadata)
    for data_file in read_data_files(metadata_path.parent):
        lower = data_file.lower_bounds or {}
        upper = data_file.upper_bounds or {}
        if geom_field_id not in lower or geom_field_id not in upper:
            name = Path(data_file.file_path).name
            raise SystemExit(f"countries: {name} has no geometry bounds in the manifest")
    print(f"ok  ne.countries every data file has geometry bounds")


def verify_disputed_areas_deletes(entry: tuple[Path, int]) -> None:
    """Assert the disputed_areas snapshot records the position deletes, and print
    the live row count a reader sees after applying them."""
    metadata_path, source_rows = entry
    metadata = json.loads(metadata_path.read_text())
    summary = metadata["snapshots"][0]["summary"]
    recorded_deletes = int(summary["total-position-deletes"])
    if recorded_deletes != len(DELETED_POSITIONS):
        raise SystemExit(
            f"disputed_areas: recorded {recorded_deletes} position deletes, expected {len(DELETED_POSITIONS)}"
        )
    live_rows = source_rows - len(DELETED_POSITIONS)
    print(f"ok  ne.disputed_areas position deletes={recorded_deletes}  live rows={live_rows}")


def verify_populated_places_schema(metadata_path: Path) -> None:
    """Assert the populated_places schema shows the evolved attribute names and
    no longer the pre-rename column name."""
    metadata = json.loads(metadata_path.read_text())
    field_names = [field["name"] for field in metadata["schemas"][0]["fields"]]
    for expected in (RENAMED_FIELD_TO, APPENDED_FIELD_NAME):
        if expected not in field_names:
            raise SystemExit(f"populated_places: schema has no field {expected!r}")
    if RENAMED_FIELD_FROM in field_names:
        raise SystemExit(f"populated_places: schema still has pre-rename field {RENAMED_FIELD_FROM!r}")
    print(f"ok  ne.populated_places schema has {RENAMED_FIELD_TO!r} and {APPENDED_FIELD_NAME!r}")


def read_data_files(meta_dir: Path) -> list:
    """The DataFile entries of every data manifest under `meta_dir`, read from the
    physical files (the manifest-list records synthetic warehouse URIs)."""
    io = PyArrowFileIO()
    manifest_list_path = sorted(meta_dir.glob("*manifest-list.avro"))[0]
    data_files: list = []
    for manifest in read_manifest_list(io.new_input(str(manifest_list_path))):
        if manifest.content != ManifestContent.DATA:
            continue
        manifest._data[0] = str(meta_dir / Path(manifest.manifest_path).name)
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=False):
            data_files.append(entry.data_file)
    return data_files


def geometry_field_id(metadata: dict) -> int:
    """The field id of the schema's geometry column."""
    for field in metadata["schemas"][0]["fields"]:
        if field["type"] == GEOMETRY_TYPE_TOKEN:
            return field["id"]
    raise SystemExit("no geometry field in schema")


if __name__ == "__main__":
    raise SystemExit(main())
