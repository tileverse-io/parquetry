"""Normalize the raw evolved equality-delete warehouse and vendor it as a fixture.

Run on the host (needs pyarrow + fastavro; see README.md's venv note) AFTER
equality-evolved-gen has produced ./equality-evolved-gen/work/warehouse/events.

This fixture is an EVOLVED partitioned table: identity(category) is the
partition column, and that column is dropped from the data Parquet files (the
omitted-partition-column layout). A reader reconstructs category from the
manifest partition tuple. An equality delete keyed on category="a" removes every
category-a row; the reader must fold that delete against the reconstructed
constant.

Makes the table portable, mirroring normalize_and_vendor_equality.py and
iceberg-partitioned/generate_fixture.py:
  - rewrites the absolute generation path to a clean logical root
    (file:///iceberg-deletes/equality-evolved) in the metadata json and the
    manifest list / manifest Avro. The Iceberg Java writer emits the path with a
    file: scheme in the metadata (file:///abs and file:/abs) and as a bare
    filesystem path in the manifest data_file.file_path; all spellings are
    rewritten to the schemed clean root;
  - drops the category column from EACH data Parquet file while preserving the
    id and value field ids (the same technique as the omitted-partition fixture).
    The equality-delete Parquet keeps its category column (category is the
    equality field) and has no file_path column; no Parquet path rewrite applies
    to it;
  - keeps only the current metadata document, renamed to v1.metadata.json (the
    reader picks the highest vN.metadata.json), and drops version-hint.text and
    the Hadoop .crc checksums.
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
SOURCE = HERE / "equality-evolved-gen" / "work" / "warehouse" / "events"
DEST = HERE.parents[1] / "src" / "main" / "resources" / "iceberg-deletes" / "equality-evolved"
CLEAN_ROOT = "file:///iceberg-deletes/equality-evolved"

DROPPED_COLUMN = "category"
KEPT_FIELD_IDS = {"id": 1, "value": 3}
DELETE_FILE_NAME = "equality-deletes.parquet"


def current_metadata_document(metadata_dir: Path) -> Path:
    latest = None
    for path in sorted(metadata_dir.glob("*.metadata.json")):
        document = json.loads(path.read_text())
        if document.get("current-snapshot-id") is not None:
            latest = path
    if latest is None:
        raise SystemExit("no metadata document with a current snapshot")
    return latest


def generated_fs_path(metadata_document: Path) -> str:
    location = json.loads(metadata_document.read_text())["location"]
    return re.sub(r"^file:/+", "/", location)


def copy_without_checksums(source: Path, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(source, dest, ignore=shutil.ignore_patterns(".*.crc"))


def replace_path(text: str, fs_path: str) -> str:
    schemed = re.compile(r"file:/+" + re.escape(fs_path.lstrip("/")))
    text = schemed.sub(CLEAN_ROOT, text)
    bare = re.compile(re.escape(fs_path))
    return bare.sub(CLEAN_ROOT, text)


def replace_in_value(value, fs_path: str):
    if isinstance(value, str):
        return replace_path(value, fs_path)
    if isinstance(value, dict):
        return {k: replace_in_value(v, fs_path) for k, v in value.items()}
    if isinstance(value, list):
        return [replace_in_value(v, fs_path) for v in value]
    return value


def rewrite_avro_paths(path: Path, fs_path: str) -> None:
    with open(path, "rb") as handle:
        reader = fastavro.reader(handle)
        schema = reader.writer_schema
        metadata = dict(reader.metadata)
        codec = metadata.pop("avro.codec", "null")
        metadata.pop("avro.schema", None)
        records = [replace_in_value(record, fs_path) for record in reader]
    with open(path, "wb") as handle:
        fastavro.writer(handle, schema, records, codec=codec, metadata=metadata)


def write_current_metadata(metadata_dir: Path, current: Path, fs_path: str) -> None:
    text = replace_path(current.read_text(), fs_path)
    document = json.loads(text)
    for path in list(metadata_dir.glob("*.metadata.json")):
        path.unlink()
    target = metadata_dir / "v1.metadata.json"
    target.write_text(json.dumps(document, indent=2) + "\n")


def rewrite_manifest_avro(metadata_dir: Path, fs_path: str) -> None:
    for path in sorted(metadata_dir.glob("*.avro")):
        rewrite_avro_paths(path, fs_path)


def drop_version_hint(metadata_dir: Path) -> None:
    hint = metadata_dir / "version-hint.text"
    if hint.exists():
        hint.unlink()


def drop_category_from_data_file(path: Path) -> None:
    parquet_file = pq.ParquetFile(path)
    table = parquet_file.read().drop_columns([DROPPED_COLUMN])
    fields = []
    for field in table.schema:
        meta = {b"PARQUET:field_id": str(KEPT_FIELD_IDS[field.name]).encode()}
        fields.append(field.with_metadata(meta))
    table = table.cast(pa.schema(fields))
    pq.write_table(table, path)


def drop_category_from_data_files(data_root: Path) -> None:
    for path in sorted(data_root.rglob("*.parquet")):
        if path.name == DELETE_FILE_NAME:
            continue
        drop_category_from_data_file(path)


def main() -> int:
    copy_without_checksums(SOURCE, DEST)
    metadata_dir = DEST / "metadata"

    current = current_metadata_document(metadata_dir)
    fs_path = generated_fs_path(current)

    write_current_metadata(metadata_dir, current, fs_path)
    rewrite_manifest_avro(metadata_dir, fs_path)
    drop_version_hint(metadata_dir)
    drop_category_from_data_files(DEST / "data")

    print(f"wrote {DEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
