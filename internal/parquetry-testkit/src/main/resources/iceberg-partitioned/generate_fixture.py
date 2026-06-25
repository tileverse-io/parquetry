# Generates the parquetry-iceberg partitioned-read test fixture.
#
# Requires: pyiceberg[pyarrow,sql-sqlite] >= 0.7, pyarrow, fastavro. Run from this directory:
#   python3 generate_fixture.py
#
# Produces two vendored Iceberg tables (metadata/ + data/), both partitioned by identity(category):
#   ./by_category          - the category column is retained physically in each data file.
#   ./by_category_omitted  - the category column is dropped from the data files (id/amount keep
#                            their field ids) so the reader must reconstruct category from the
#                            manifest partition tuple.
#
# Post-processing makes the fixtures portable and matches the reader's conventions:
#   - the absolute generation path is rewritten to a clean logical root (file:///iceberg-partitioned/...)
#     in the metadata json and in the manifest-list / manifest Avro files. No personal path is then
#     committed, and the table resolves from any extraction directory by prefix-strip;
#   - the latest metadata document is renamed to v1.metadata.json (the reader picks the highest
#     vN.metadata.json), and the empty initial metadata document is removed.
import json
import os
import shutil

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField, LongType, StringType

HERE = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE = os.path.join(HERE, "_warehouse")
TARGET = os.path.join(HERE, "by_category")
OMITTED = os.path.join(HERE, "by_category_omitted")
CLEAN_ROOT = "file:///iceberg-partitioned"


def generate_table():
    if os.path.exists(WAREHOUSE):
        shutil.rmtree(WAREHOUSE)
    os.makedirs(WAREHOUSE)

    catalog = SqlCatalog(
        "fixture",
        uri=f"sqlite:///{WAREHOUSE}/catalog.db",
        warehouse=f"file://{WAREHOUSE}",
    )

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="category", field_type=StringType(), required=True),
        NestedField(field_id=3, name="amount", field_type=LongType(), required=True),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")
    )

    catalog.create_namespace("fixture")
    table = catalog.create_table("fixture.by_category", schema=schema, partition_spec=spec)

    rows = []
    for category, base in (("a", 0), ("b", 1000), ("c", 2000)):
        for i in range(100):
            rows.append({"id": base + i, "category": category, "amount": (base + i) * 2})
    arrow = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("category", pa.string(), nullable=False),
                pa.field("amount", pa.int64(), nullable=False),
            ]
        ),
    )
    table.append(arrow)

    src = os.path.join(WAREHOUSE, "fixture", "by_category")
    if os.path.exists(TARGET):
        shutil.rmtree(TARGET)
    shutil.copytree(src, TARGET)
    shutil.rmtree(WAREHOUSE)
    return f"file://{src}"


def rewrite_avro_paths(path, old_prefix, new_prefix):
    with open(path, "rb") as handle:
        reader = fastavro.reader(handle)
        schema = reader.writer_schema
        metadata = dict(reader.metadata)
        codec = metadata.pop("avro.codec", "null")
        metadata.pop("avro.schema", None)
        records = [_replace_in_value(record, old_prefix, new_prefix) for record in reader]
    with open(path, "wb") as handle:
        fastavro.writer(handle, schema, records, codec=codec, metadata=metadata)


def _replace_in_value(value, old_prefix, new_prefix):
    if isinstance(value, str):
        return value.replace(old_prefix, new_prefix)
    if isinstance(value, dict):
        return {k: _replace_in_value(v, old_prefix, new_prefix) for k, v in value.items()}
    if isinstance(value, list):
        return [_replace_in_value(v, old_prefix, new_prefix) for v in value]
    return value


def rewrite_table_paths(table_dir, old_prefix, new_prefix):
    metadata_dir = os.path.join(table_dir, "metadata")
    for name in os.listdir(metadata_dir):
        path = os.path.join(metadata_dir, name)
        if name.endswith(".metadata.json"):
            text = open(path).read().replace(old_prefix, new_prefix)
            # Pretty-print: these are human-read test fixtures, and the reader parses the JSON regardless of whitespace.
            with open(path, "w") as handle:
                handle.write(json.dumps(json.loads(text), indent=2))
                handle.write("\n")
        elif name.endswith(".avro"):
            rewrite_avro_paths(path, old_prefix, new_prefix)


def normalize_metadata_name(table_dir):
    metadata_dir = os.path.join(table_dir, "metadata")
    latest = None
    for name in sorted(os.listdir(metadata_dir)):
        if not name.endswith(".metadata.json"):
            continue
        document = json.load(open(os.path.join(metadata_dir, name)))
        if document.get("snapshots"):
            latest = name
    if latest is None:
        raise SystemExit("no metadata document with a snapshot")
    for name in list(os.listdir(metadata_dir)):
        if name.endswith(".metadata.json") and name != latest:
            os.remove(os.path.join(metadata_dir, name))
    os.rename(os.path.join(metadata_dir, latest), os.path.join(metadata_dir, "v1.metadata.json"))


def drop_partition_column(path, column, keep_field_ids):
    parquet_file = pq.ParquetFile(path)
    table = parquet_file.read().drop_columns([column])
    fields = []
    for field in table.schema:
        meta = {b"PARQUET:field_id": str(keep_field_ids[field.name]).encode()}
        fields.append(field.with_metadata(meta))
    table = table.cast(pa.schema(fields))
    pq.write_table(table, path)


def generate_omitted_variant(base_root):
    if os.path.exists(OMITTED):
        shutil.rmtree(OMITTED)
    shutil.copytree(TARGET, OMITTED)
    rewrite_table_paths(OMITTED, base_root, f"{CLEAN_ROOT}/by_category_omitted")
    data_root = os.path.join(OMITTED, "data")
    for dirpath, _dirnames, filenames in os.walk(data_root):
        for name in filenames:
            if name.endswith(".parquet"):
                drop_partition_column(
                    os.path.join(dirpath, name), "category", {"id": 1, "amount": 3}
                )


def main():
    generated_root = generate_table()
    rewrite_table_paths(TARGET, generated_root, f"{CLEAN_ROOT}/by_category")
    generate_omitted_variant(f"{CLEAN_ROOT}/by_category")
    normalize_metadata_name(TARGET)
    normalize_metadata_name(OMITTED)
    print(f"wrote {TARGET}")
    print(f"wrote {OMITTED}")


if __name__ == "__main__":
    main()
