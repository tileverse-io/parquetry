"""Normalize the raw Spark/Iceberg warehouse and vendor it as a test fixture.

Run on the host (needs pyarrow + fastavro; see run.sh's venv note) AFTER
build_positional_deletes.py has produced ./work/warehouse/db/events.

Makes the table portable, mirroring iceberg-partitioned/generate_fixture.py:
  - rewrites the absolute generation path to a clean logical root
    (file:///iceberg-deletes/positional) in the metadata json, the manifest
    list / manifest Avro, and the position-delete Parquet's file_path column;
  - keeps only the current metadata document, renamed to v1.metadata.json (the
    reader picks the highest vN.metadata.json), and drops version-hint.text and
    the Hadoop .crc checksums.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
SOURCE = HERE / "work" / "warehouse" / "db" / "events"
DEST = HERE.parents[1] / "src" / "main" / "resources" / "iceberg-deletes" / "positional"
CLEAN_ROOT = "file:///iceberg-deletes/positional"


def current_metadata_document(metadata_dir: Path) -> Path:
    latest = None
    for path in sorted(metadata_dir.glob("*.metadata.json")):
        document = json.loads(path.read_text())
        if document.get("current-snapshot-id") is not None:
            latest = path
    if latest is None:
        raise SystemExit("no metadata document with a current snapshot")
    return latest


def generated_root(metadata_document: Path) -> str:
    return json.loads(metadata_document.read_text())["location"]


def copy_without_checksums(source: Path, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(source, dest, ignore=shutil.ignore_patterns(".*.crc"))


def replace_in_value(value, old_prefix: str, new_prefix: str):
    if isinstance(value, str):
        return value.replace(old_prefix, new_prefix)
    if isinstance(value, dict):
        return {k: replace_in_value(v, old_prefix, new_prefix) for k, v in value.items()}
    if isinstance(value, list):
        return [replace_in_value(v, old_prefix, new_prefix) for v in value]
    return value


def rewrite_avro_paths(path: Path, old_prefix: str, new_prefix: str) -> None:
    with open(path, "rb") as handle:
        reader = fastavro.reader(handle)
        schema = reader.writer_schema
        metadata = dict(reader.metadata)
        codec = metadata.pop("avro.codec", "null")
        metadata.pop("avro.schema", None)
        records = [replace_in_value(record, old_prefix, new_prefix) for record in reader]
    with open(path, "wb") as handle:
        fastavro.writer(handle, schema, records, codec=codec, metadata=metadata)


def write_current_metadata(metadata_dir: Path, current: Path, old_prefix: str) -> None:
    text = current.read_text().replace(old_prefix, CLEAN_ROOT)
    document = json.loads(text)
    for path in list(metadata_dir.glob("*.metadata.json")):
        path.unlink()
    target = metadata_dir / "v1.metadata.json"
    target.write_text(json.dumps(document, indent=2) + "\n")


def rewrite_manifest_avro(metadata_dir: Path, old_prefix: str) -> None:
    for path in sorted(metadata_dir.glob("*.avro")):
        rewrite_avro_paths(path, old_prefix, CLEAN_ROOT)


def rewrite_delete_file_paths(data_dir: Path, old_prefix: str) -> None:
    for path in sorted(data_dir.glob("*-deletes.parquet")):
        table = pq.read_table(path)
        rewritten = [
            value.replace(old_prefix, CLEAN_ROOT) if value is not None else None
            for value in table.column("file_path").to_pylist()
        ]
        index = table.schema.get_field_index("file_path")
        field = table.schema.field(index)
        table = table.set_column(index, field, pa.array(rewritten, type=field.type))
        pq.write_table(table, path)


def drop_version_hint(metadata_dir: Path) -> None:
    hint = metadata_dir / "version-hint.text"
    if hint.exists():
        hint.unlink()


def main() -> int:
    copy_without_checksums(SOURCE, DEST)
    metadata_dir = DEST / "metadata"
    data_dir = DEST / "data"

    current = current_metadata_document(metadata_dir)
    old_prefix = generated_root(current)

    write_current_metadata(metadata_dir, current, old_prefix)
    rewrite_manifest_avro(metadata_dir, old_prefix)
    rewrite_delete_file_paths(data_dir, old_prefix)
    drop_version_hint(metadata_dir)

    print(f"wrote {DEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
