#!/usr/bin/env python3
"""Build the NE STAC demo data in both flavors the STAC DataStore can open.

Turns the five Natural Earth GeoParquet layers under
`integrations/parquetry-geoserver/demo/data/ne/` into a small STAC catalog
committed under `.../demo/data/`, in the two shapes the STAC DataStore factory
auto-detects by URI extension. The JSON catalog entry point (`catalog.json`)
sits at the data root and is served over HTTP; the item-table (`items.parquet`)
sits under `ne/index/`, inside the same object-store bucket as the data parts.
Each item's data asset points at an absolute `s3://naturalearth/<layer>.parquet`
URI the store resolves per asset, not relative to the catalog; the collections
live under `stac/`.

Each NE layer is its own STAC collection (collection id = layer name) holding a
single item. The five layers have wildly different attribute schemas, and one
feature type per file is the only correct mapping: a merged collection over
heterogeneous-schema files would resolve one representative schema and corrupt
the read of every other part.

  - The JSON catalog: `catalog.json` links five child collections, one per layer
    (`stac/<layer>/collection.json`), each linking its one item
    (`items/<layer>.json`, relative to its collection). This is the shape
    `JsonStacReader` parses:
    a catalog with child links to collections, each with item links, and each
    item document holding a bbox and a single GeoParquet data asset.
  - The stac-geoparquet item-table `items.parquet`: one row per layer in the
    shape the stac-geoparquet specification defines - `id`, a WKB `geometry`,
    the `bbox` struct of named corners, an `assets` struct keyed by asset name,
    and `collection` set to the layer name - plus the `stac-geoparquet` and
    `geo` footer metadata keys. This is the shape `GeoParquetStacReader` reads;
    it groups rows by `collection`, one collection per layer.

Both flavors publish the same five collections named by layer, and both point
each item's data asset at the same external GeoParquet part. The default asset
base is `s3://naturalearth`, the s3proxy bucket the demo serves the NE parts
from; `--href-base` overrides it (a local smoke test points it at a directory of
the NE parquet files reachable through file storage).

Per-layer bboxes are read from each NE file's GeoParquet footer metadata
(`geo.columns[primary_column].bbox`), never hardcoded.

Determinism: two runs produce byte-identical output. The JSON documents are
built as fixed-order dicts and written with `json.dumps(indent=2)` (no
`sort_keys`; Python preserves insertion order), and `items.parquet` is written
with the pinned zstd toolchain in `requirements.txt`. There are no wall-clock or
entropy inputs.

Reproduce the committed demo data:

    cd internal/parquetry-testkit/fixtures-gen
    python3 -m venv /tmp/ne-warehouse-venv
    source /tmp/ne-warehouse-venv/bin/activate
    pip install -r requirements.txt
    python ne-warehouse/build_stac_demo.py
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
# ne-warehouse -> fixtures-gen -> parquetry-testkit -> internal -> repo root.
REPO_ROOT = HERE.parents[3]
DATA_DIR = REPO_ROOT / "integrations" / "parquetry-geoserver" / "demo" / "data"
NE_DIR = DATA_DIR / "ne"
# The catalog entry points (catalog.json, items.parquet) sit at the output root,
# beside the NE parts' parent - the common container the STAC store reads every
# asset relative to. Collections and item documents live under this subdirectory.
STAC_SUBDIR = "stac"

# The five NE layers, in a stable order. This order fixes the catalog child
# links and the item-table row order. Each layer is one STAC collection.
LAYERS = ["boundary_lines_land", "coastlines", "countries", "disputed_areas", "populated_places"]

CATALOG_ID = "natural-earth"
CATALOG_DESCRIPTION = "Natural Earth demo STAC catalog"
LICENSE = "public-domain"
STAC_VERSION = "1.0.0"
DATETIME = "2024-01-01T00:00:00Z"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
DEFAULT_HREF_BASE = "s3://naturalearth"

GEOMETRY_COLUMN = "geometry"
DATA_ASSET_KEY = "data"

# The item-table columns in the stac-geoparquet shape. The bbox corners are
# typed float64 to keep an integer-valued corner a double the reader reads with
# getDouble; the asset dictionary is a struct keyed by asset name, one key here.
BBOX_TYPE = pa.struct(
    [
        ("xmin", pa.float64()),
        ("ymin", pa.float64()),
        ("xmax", pa.float64()),
        ("ymax", pa.float64()),
    ]
)
ASSETS_TYPE = pa.struct(
    [
        (DATA_ASSET_KEY, pa.struct([("href", pa.string()), ("type", pa.string())])),
    ]
)
ITEM_TABLE_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        (GEOMETRY_COLUMN, pa.binary()),
        ("bbox", BBOX_TYPE),
        ("assets", ASSETS_TYPE),
        ("collection", pa.string()),
    ]
)

# The footer metadata an item-table declares: the stac-geoparquet document with
# no collections member, which leaves each collection's extent to derive from
# its item bboxes, and the GeoParquet entry naming the primary geometry column.
STAC_GEOPARQUET_METADATA = {"version": "1.0.0"}
GEO_METADATA = {
    "version": "1.1.0",
    "primary_column": GEOMETRY_COLUMN,
    "columns": {
        GEOMETRY_COLUMN: {"encoding": "WKB", "geometry_types": ["Polygon"]},
    },
}


def main() -> int:
    args = parse_args()
    if not NE_DIR.is_dir():
        print(f"NE source layers not found under {NE_DIR}", file=sys.stderr)
        return 1

    out_dir = Path(args.out).resolve() if args.out else DATA_DIR
    href_base = args.href_base.rstrip("/")

    bboxes = {layer: read_layer_bbox(layer) for layer in LAYERS}
    write_json_catalog(out_dir, bboxes, href_base)
    write_item_table(out_dir, bboxes, href_base)
    verify(out_dir, bboxes)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the NE STAC demo data (JSON catalog + item-table).")
    parser.add_argument(
        "--out",
        default=None,
        help="output root directory (default: the committed demo/data)",
    )
    parser.add_argument(
        "--href-base",
        default=DEFAULT_HREF_BASE,
        help=f"base URL each item's data asset points at (default: {DEFAULT_HREF_BASE})",
    )
    return parser.parse_args()


def read_layer_bbox(layer: str) -> list[float]:
    """The layer's primary-geometry bbox from its GeoParquet footer metadata."""
    metadata = pq.ParquetFile(NE_DIR / f"{layer}.parquet").metadata.metadata
    geo = json.loads(metadata[b"geo"])
    primary = geo["primary_column"]
    return geo["columns"][primary]["bbox"]


def write_json_catalog(out_dir: Path, bboxes: dict[str, list[float]], href_base: str) -> None:
    """Write the JSON flavor: the catalog and, per layer, its collection and one
    item document, in the layout JsonStacReader navigates by relative link."""
    write_json(out_dir / "catalog.json", catalog_document())
    for layer in LAYERS:
        collection_dir = out_dir / STAC_SUBDIR / layer
        items_dir = collection_dir / "items"
        items_dir.mkdir(parents=True, exist_ok=True)
        write_json(collection_dir / "collection.json", collection_document(layer, bboxes[layer]))
        write_json(items_dir / f"{layer}.json", item_document(layer, bboxes[layer], href_base))


def catalog_document() -> dict:
    """The root catalog: a self link and one child link per layer collection."""
    child_links = [{"rel": "child", "href": f"{STAC_SUBDIR}/{layer}/collection.json"} for layer in LAYERS]
    return {
        "type": "Catalog",
        "stac_version": STAC_VERSION,
        "id": CATALOG_ID,
        "description": CATALOG_DESCRIPTION,
        "links": [
            {"rel": "self", "href": "catalog.json"},
            *child_links,
        ],
    }


def collection_document(layer: str, bbox: list[float]) -> dict:
    """One layer's collection: its own spatial extent and its single item link,
    the href relative to the collection document."""
    return {
        "type": "Collection",
        "stac_version": STAC_VERSION,
        "id": layer,
        "title": layer_title(layer),
        "description": f"Natural Earth {layer_title(layer).lower()} as GeoParquet",
        "license": LICENSE,
        "extent": {
            "spatial": {"bbox": [bbox]},
            "temporal": {"interval": [[DATETIME, None]]},
        },
        "links": [
            {"rel": "self", "href": "collection.json"},
            {"rel": "item", "href": f"items/{layer}.json"},
        ],
    }


def item_document(layer: str, bbox: list[float], href_base: str) -> dict:
    """One item: its bbox and a single GeoParquet data asset for the layer."""
    return {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "id": layer,
        "bbox": bbox,
        "properties": {"datetime": DATETIME},
        "geometry": None,
        "links": [],
        "assets": {
            "data": {
                "href": f"{href_base}/{layer}.parquet",
                "type": PARQUET_MEDIA_TYPE,
                "title": layer_title(layer),
                "roles": ["data"],
            }
        },
    }


def write_item_table(out_dir: Path, bboxes: dict[str, list[float]], href_base: str) -> None:
    """Write the stac-geoparquet flavor: one item-table row per layer, each row's
    collection set to the layer name, under the item-table footer metadata."""
    rows = [item_row(layer, bboxes[layer], href_base) for layer in LAYERS]
    table = pa.Table.from_pylist(rows, schema=ITEM_TABLE_SCHEMA.with_metadata(footer_metadata()))
    item_table = item_table_path(out_dir)
    item_table.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, item_table, compression="zstd")


def footer_metadata() -> dict[bytes, bytes]:
    """The file-level key-value metadata, as the compact JSON both keys hold."""
    return {
        b"stac-geoparquet": json.dumps(STAC_GEOPARQUET_METADATA).encode(),
        b"geo": json.dumps(GEO_METADATA).encode(),
    }


def item_table_path(out_dir: Path) -> Path:
    """The stac-geoparquet item-table location: a subdirectory of the NE bucket. It
    sits under a subdirectory, not at the bucket root, because the GeoParquet
    directory store lists top-level `*.parquet` in the same bucket as layers and
    would reject the item-table as a non-GeoParquet layer. The stac-geoparquet store
    reads it from `s3://naturalearth/index/items.parquet`."""
    return out_dir / "ne" / "index" / "items.parquet"


def item_row(layer: str, bbox: list[float], href_base: str) -> dict:
    """One item-table row: the item id, the geometry and bbox of the layer's
    extent, its data asset, and its collection (the layer name)."""
    return {
        "id": layer,
        GEOMETRY_COLUMN: wkb_polygon(bbox),
        "bbox": {"xmin": bbox[0], "ymin": bbox[1], "xmax": bbox[2], "ymax": bbox[3]},
        "assets": {
            DATA_ASSET_KEY: {
                "href": f"{href_base}/{layer}.parquet",
                "type": PARQUET_MEDIA_TYPE,
            }
        },
        "collection": layer,
    }


def wkb_polygon(bbox: list[float]) -> bytes:
    """The bbox as a closed little-endian WKB polygon: the byte-order flag, the
    polygon type code, one ring, and its five points (the four corners
    counter-clockwise, the first repeated to close the ring)."""
    xmin, ymin, xmax, ymax = bbox
    ring = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin)]
    header = struct.pack("<BIII", 1, 3, 1, len(ring))
    return header + b"".join(struct.pack("<dd", x, y) for x, y in ring)


def layer_title(layer: str) -> str:
    """A readable title from a layer slug, e.g. 'boundary_lines_land' ->
    'Boundary lines land'."""
    return layer.replace("_", " ").capitalize()


def write_json(path: Path, document: dict) -> None:
    """Write one JSON document with a fixed key order and a trailing newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n")


def verify(out_dir: Path, bboxes: dict[str, list[float]]) -> None:
    """Self-checks over the written outputs: the catalog links one collection per
    layer, each collection has its single item with a bbox and a GeoParquet data
    asset, and the item-table has one row per layer with collection = layer."""
    verify_json_catalog(out_dir, bboxes)
    verify_item_table(out_dir, bboxes)
    print(f"\ndone: JSON catalog + item-table, one collection per NE layer ({len(LAYERS)}), under {out_dir}")


def verify_json_catalog(out_dir: Path, bboxes: dict[str, list[float]]) -> None:
    catalog = load_json(out_dir / "catalog.json")
    child_hrefs = [link["href"] for link in catalog["links"] if link["rel"] == "child"]
    expected = [f"{STAC_SUBDIR}/{layer}/collection.json" for layer in LAYERS]
    if child_hrefs != expected:
        raise SystemExit(f"catalog child links {child_hrefs} do not match the layers")

    for layer in LAYERS:
        collection = load_json(out_dir / STAC_SUBDIR / layer / "collection.json")
        if collection["type"] != "Collection" or collection["id"] != layer:
            raise SystemExit(f"{layer}/collection.json is not a Collection with id {layer!r}")
        item_hrefs = [link["href"] for link in collection["links"] if link["rel"] == "item"]
        if item_hrefs != [f"items/{layer}.json"]:
            raise SystemExit(f"collection {layer} item links {item_hrefs} unexpected")

        item = load_json(out_dir / STAC_SUBDIR / layer / "items" / f"{layer}.json")
        if item["id"] != layer:
            raise SystemExit(f"item {layer}.json has id {item['id']!r}")
        if item["bbox"] != bboxes[layer]:
            raise SystemExit(f"item {layer} bbox {item['bbox']} does not match the footer bbox")
        if "data" not in item["assets"] or item["assets"]["data"]["type"] != PARQUET_MEDIA_TYPE:
            raise SystemExit(f"item {layer} has no GeoParquet data asset")
    print(f"ok  JSON catalog {CATALOG_ID!r} -> {len(LAYERS)} per-layer collections {LAYERS}")


def verify_item_table(out_dir: Path, bboxes: dict[str, list[float]]) -> None:
    parquet_file = pq.ParquetFile(item_table_path(out_dir))
    verify_item_table_metadata(parquet_file)
    table = parquet_file.read()
    if table.column_names != ITEM_TABLE_SCHEMA.names:
        raise SystemExit(f"item-table columns {table.column_names} do not match the reader contract")
    rows = table.to_pylist()
    if len(rows) != len(LAYERS):
        raise SystemExit(f"item-table has {len(rows)} rows, expected {len(LAYERS)}")
    for row, layer in zip(rows, LAYERS):
        verify_item_row(row, layer, bboxes[layer], href_of(row))
    print(f"ok  item-table items.parquet with {len(rows)} rows, one collection per layer")


def verify_item_table_metadata(parquet_file: pq.ParquetFile) -> None:
    """The footer holds both keys the readers look for."""
    footer = parquet_file.metadata.metadata or {}
    for key in footer_metadata():
        if key not in footer:
            raise SystemExit(f"item-table footer has no {key.decode()!r} metadata key")


def verify_item_row(row: dict, layer: str, expected_bbox: list[float], href: str) -> None:
    """One row names its layer, repeats the layer's footer bbox in both the bbox
    struct and the geometry, and points its data asset at the layer's part."""
    bbox = row["bbox"]
    corners = [bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]]
    if row["id"] != layer or row["collection"] != layer or corners != expected_bbox:
        raise SystemExit(f"item-table row for {layer} does not match its item document")
    if row[GEOMETRY_COLUMN] != wkb_polygon(expected_bbox):
        raise SystemExit(f"item-table row for {layer} has a geometry that is not its bbox polygon")
    if not href.endswith(f"/{layer}.parquet"):
        raise SystemExit(f"item-table row for {layer} points its data asset at {href!r}")


def href_of(row: dict) -> str:
    """The row's data-asset href, from the assets struct keyed by asset name."""
    return row["assets"][DATA_ASSET_KEY]["href"]


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


if __name__ == "__main__":
    raise SystemExit(main())
