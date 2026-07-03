"""Normalize the raw Iceberg materialized row-lineage warehouse and vendor it as a fixture.

Run on the host (needs pyarrow + fastavro; reuse the iceberg-deletes venv) AFTER
materialized-gen has produced ./materialized-gen/work/warehouse/events.

Makes the table portable, mirroring the iceberg-deletes normalize scripts:
  - rewrites the absolute generation path to a clean logical root
    (file:///iceberg-row-lineage/materialized) in the metadata json and the manifest
    list / manifest Avro. The Iceberg Java writer emits the same path with a
    file: scheme in the metadata (file:///abs and file:/abs) and as a bare
    filesystem path in the manifest data_file.file_path; every spelling is
    rewritten to the schemed clean root;
  - keeps only the current metadata document, renamed to v1.metadata.json (the
    reader picks the highest vN.metadata.json), and drops version-hint.text and
    the Hadoop .crc checksums.

The data Parquet files have no file_path column; no Parquet path rewrite applies
to them. The second data file physically materializes the reserved _row_id
column (Parquet field id 2147483540) as a nullable column with a mix of stored
and null cells; that column and its cells are left untouched. The row-lineage
bases (metadata next-row-id, per-snapshot first-row-id, and the manifest-list
ManifestFile.first_row_id) are numeric and path-free; the rewrite leaves them
untouched.
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import fastavro

HERE = Path(__file__).resolve().parent
SOURCE = HERE / "materialized-gen" / "work" / "warehouse" / "events"
DEST = HERE.parents[1] / "src" / "main" / "resources" / "iceberg-row-lineage" / "materialized"
CLEAN_ROOT = "file:///iceberg-row-lineage/materialized"


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


def main() -> int:
    copy_without_checksums(SOURCE, DEST)
    metadata_dir = DEST / "metadata"

    current = current_metadata_document(metadata_dir)
    fs_path = generated_fs_path(current)

    write_current_metadata(metadata_dir, current, fs_path)
    rewrite_manifest_avro(metadata_dir, fs_path)
    drop_version_hint(metadata_dir)

    print(f"wrote {DEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
