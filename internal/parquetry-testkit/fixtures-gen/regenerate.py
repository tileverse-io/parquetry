#!/usr/bin/env python3
"""Deterministically regenerate the vendored iceberg-geo-testbed goldens.

Runs the pinned upstream writers (the `iceberg-geo-testbed` git submodule) with
a fixed location root and fixed snapshot-id / table-uuid, then copies the output
into the testkit resources. The upstream generator is deterministic for the
Parquet data and per-file bounds (region RNGs are seeded with a hashlib
`stable_seed`); the only wall-clock inputs are the snapshot-id and table-uuid in
`_static_catalog.py`, which this script pins so regeneration is byte-identical.

Reproduce the committed corpus:

    python3 -m venv /tmp/iceberg-fixtures-venv
    source /tmp/iceberg-fixtures-venv/bin/activate
    pip install -r requirements.txt
    python regenerate.py

Requires the submodule to be checked out:
    git submodule update --init internal/parquetry-testkit/fixtures-gen/iceberg-geo-testbed
"""

from __future__ import annotations

import hashlib
import importlib
import shutil
import sys
import uuid
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
SUBMODULE = HERE / "iceberg-geo-testbed"
DEST = HERE.parent / "src" / "main" / "resources" / "iceberg-geo-testbed"

# The six local-only fixtures (the cloud IRC catalog writer is intentionally
# excluded). Each writes ten single-region Parquet files plus a static catalog.
FIXTURES = [
    "v2_flat_columns",
    "v2_bbox_struct",
    "v2_geo_convention",
    "v3_geometry",
    "v3_geometry_lineage",
    "v3_minimal",
]

# A fixed point in time, in seconds. The writer derives snapshot-id as
# int(time.time() * 1000); offsetting per fixture keeps each catalog's id
# stable and distinct. 1_700_000_000 s = 2023-11-14T22:13:20Z.
BASE_EPOCH_SECONDS = 1_700_000_000

# Fixed Avro OCF sync marker (normally os.urandom(16) per file).
AVRO_SYNC_MARKER = b"\x00" * 16


def fixed_uuid(name: str) -> uuid.UUID:
    """A deterministic table-uuid derived from the fixture name."""
    digest = hashlib.sha256(name.encode("utf-8")).digest()[:16]
    return uuid.UUID(bytes=digest)


def regenerate_fixture(index: int, name: str) -> None:
    module = importlib.import_module(f"testbed.{name}")
    epoch_seconds = float(BASE_EPOCH_SECONDS + index)
    # Three wall-clock / entropy inputs make the catalog non-reproducible:
    # the snapshot-id (time.time), the table-uuid (uuid.uuid4), and the Avro
    # OCF sync marker (os.urandom, one random 16-byte block separator per
    # manifest / manifest-list file). Pin all three.
    with mock.patch("time.time", return_value=epoch_seconds), mock.patch(
        "uuid.uuid4", return_value=fixed_uuid(name)
    ), mock.patch("os.urandom", return_value=AVRO_SYNC_MARKER):
        module.build(location_uri=f"file:///iceberg-geo-testbed/{name}")
    source = SUBMODULE / "data" / name
    target = DEST / name
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target)
    print(f"regenerated {name} -> {target}")


def main() -> int:
    if not (SUBMODULE / "testbed" / "__init__.py").exists():
        print(
            "submodule not checked out; run:\n"
            "  git submodule update --init "
            "internal/parquetry-testkit/fixtures-gen/iceberg-geo-testbed",
            file=sys.stderr,
        )
        return 1
    sys.path.insert(0, str(SUBMODULE))
    DEST.mkdir(parents=True, exist_ok=True)
    for index, name in enumerate(FIXTURES):
        regenerate_fixture(index, name)
    print(f"\ndone: {len(FIXTURES)} fixtures written under {DEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
