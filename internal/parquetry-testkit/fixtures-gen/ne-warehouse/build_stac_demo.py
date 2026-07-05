#!/usr/bin/env python3
"""Build the NE STAC demo data in both flavors the STAC DataStore can open.

Turns the five Natural Earth GeoParquet layers under
`integrations/parquetry-geoserver/demo/data/ne/` into a small STAC catalog
committed under `.../demo/data/stac/`, in the two shapes the STAC DataStore
factory auto-detects by URI extension:

  - A static JSON catalog (`catalog.json` -> `ne/collection.json` ->
    `ne/items/<layer>.json`), the shape `JsonStacReader` parses: a catalog with
    a child link to one collection, the collection with one item link per layer,
    and each item document holding a bbox and a single GeoParquet data asset.
  - A stac-geoparquet item-table (`items.parquet`), the shape
    `GeoParquetStacReader` reads: one row per layer with the columns `item_id`,
    `collection`, `bbox_xmin`, `bbox_ymin`, `bbox_xmax`, `bbox_ymax`, and
    `asset_href`.

Both flavors describe the same five items in the collection `ne`, and both point
each item's data asset at the same external GeoParquet part. The default asset
base is `http://web/ne`, the compose-internal nginx hostname the demo image
serves the NE parts from; `--href-base` overrides it (a local smoke test points
it at a directory of the NE parquet files reachable through file storage).

Per-layer bboxes are read from each NE file's GeoParquet footer metadata
(`geo.columns[primary_column].bbox`), never hardcoded. The collection's spatial
extent is the union of the five layer bboxes.

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
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
# ne-warehouse -> fixtures-gen -> parquetry-testkit -> internal -> repo root.
REPO_ROOT = HERE.parents[3]
NE_DIR = REPO_ROOT / "integrations" / "parquetry-geoserver" / "demo" / "data" / "ne"
STAC_DIR = REPO_ROOT / "integrations" / "parquetry-geoserver" / "demo" / "data" / "stac"

# The five NE layers, in a stable order. This order fixes the item links in the
# collection, the item-table row order, and the collection extent union.
LAYERS = ["boundary_lines_land", "coastlines", "countries", "disputed_areas", "populated_places"]

CATALOG_ID = "natural-earth"
COLLECTION_ID = "ne"
COLLECTION_TITLE = "Natural Earth"
COLLECTION_DESCRIPTION = "Natural Earth vector layers published as GeoParquet"
CATALOG_DESCRIPTION = "Natural Earth demo STAC catalog"
LICENSE = "public-domain"
STAC_VERSION = "1.0.0"
DATETIME = "2024-01-01T00:00:00Z"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
DEFAULT_HREF_BASE = "http://web/ne"

# The item-table columns GeoParquetStacReader requires. The bbox columns are
# typed float64 to keep an integer-valued bbox value a double the reader reads
# with getDouble.
ITEM_TABLE_SCHEMA = pa.schema(
    [
        ("item_id", pa.string()),
        ("collection", pa.string()),
        ("bbox_xmin", pa.float64()),
        ("bbox_ymin", pa.float64()),
        ("bbox_xmax", pa.float64()),
        ("bbox_ymax", pa.float64()),
        ("asset_href", pa.string()),
    ]
)


def main() -> int:
    args = parse_args()
    if not NE_DIR.is_dir():
        print(f"NE source layers not found under {NE_DIR}", file=sys.stderr)
        return 1

    out_dir = Path(args.out).resolve() if args.out else STAC_DIR
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
        help="output directory (default: the committed demo/data/stac)",
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
    """Write the JSON flavor: the catalog, its one collection, and one item
    document per layer, in the layout JsonStacReader navigates by relative link."""
    collection_dir = out_dir / COLLECTION_ID
    items_dir = collection_dir / "items"
    items_dir.mkdir(parents=True, exist_ok=True)

    write_json(out_dir / "catalog.json", catalog_document())
    write_json(collection_dir / "collection.json", collection_document(bboxes))
    for layer in LAYERS:
        write_json(items_dir / f"{layer}.json", item_document(layer, bboxes[layer], href_base))


def catalog_document() -> dict:
    """The root catalog: a self link and one child link to the collection."""
    return {
        "type": "Catalog",
        "stac_version": STAC_VERSION,
        "id": CATALOG_ID,
        "description": CATALOG_DESCRIPTION,
        "links": [
            {"rel": "self", "href": "catalog.json"},
            {"rel": "child", "href": f"{COLLECTION_ID}/collection.json"},
        ],
    }


def collection_document(bboxes: dict[str, list[float]]) -> dict:
    """The collection: the union spatial extent and one item link per layer, each
    href relative to the collection document."""
    item_links = [{"rel": "item", "href": f"items/{layer}.json"} for layer in LAYERS]
    return {
        "type": "Collection",
        "stac_version": STAC_VERSION,
        "id": COLLECTION_ID,
        "title": COLLECTION_TITLE,
        "description": COLLECTION_DESCRIPTION,
        "license": LICENSE,
        "extent": {
            "spatial": {"bbox": [union_bbox(bboxes)]},
            "temporal": {"interval": [[DATETIME, None]]},
        },
        "links": [
            {"rel": "self", "href": "collection.json"},
            *item_links,
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
    """Write the stac-geoparquet flavor: one item-table row per layer."""
    rows = [item_row(layer, bboxes[layer], href_base) for layer in LAYERS]
    table = pa.Table.from_pylist(rows, schema=ITEM_TABLE_SCHEMA)
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_dir / "items.parquet", compression="zstd")


def item_row(layer: str, bbox: list[float], href_base: str) -> dict:
    """One item-table row: the item id, its collection, its flat bbox, and the
    href of its GeoParquet data part."""
    return {
        "item_id": layer,
        "collection": COLLECTION_ID,
        "bbox_xmin": bbox[0],
        "bbox_ymin": bbox[1],
        "bbox_xmax": bbox[2],
        "bbox_ymax": bbox[3],
        "asset_href": f"{href_base}/{layer}.parquet",
    }


def union_bbox(bboxes: dict[str, list[float]]) -> list[float]:
    """The bounding box enclosing every layer bbox."""
    values = list(bboxes.values())
    return [
        min(box[0] for box in values),
        min(box[1] for box in values),
        max(box[2] for box in values),
        max(box[3] for box in values),
    ]


def layer_title(layer: str) -> str:
    """A readable asset title from a layer slug, e.g. 'boundary_lines_land' ->
    'Boundary lines land'."""
    return layer.replace("_", " ").capitalize()


def write_json(path: Path, document: dict) -> None:
    """Write one JSON document with a fixed key order and a trailing newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n")


def verify(out_dir: Path, bboxes: dict[str, list[float]]) -> None:
    """Self-checks over the written outputs: the JSON catalog links a collection
    with one item per layer, each item document has a bbox and a GeoParquet data
    asset, and the item-table has the required columns with one row per layer."""
    verify_json_catalog(out_dir, bboxes)
    verify_item_table(out_dir, bboxes)
    print(f"\ndone: JSON catalog + item-table for {len(LAYERS)} NE layers under {out_dir}")


def verify_json_catalog(out_dir: Path, bboxes: dict[str, list[float]]) -> None:
    catalog = load_json(out_dir / "catalog.json")
    child = link_href(catalog, "child")
    if child != f"{COLLECTION_ID}/collection.json":
        raise SystemExit(f"catalog child link is {child!r}, expected the collection")

    collection = load_json(out_dir / child)
    if collection["type"] != "Collection":
        raise SystemExit(f"{child} is not a Collection document")
    item_hrefs = [link["href"] for link in collection["links"] if link["rel"] == "item"]
    if len(item_hrefs) != len(LAYERS):
        raise SystemExit(f"collection has {len(item_hrefs)} item links, expected {len(LAYERS)}")

    collection_dir = (out_dir / child).parent
    for layer in LAYERS:
        item = load_json(collection_dir / "items" / f"{layer}.json")
        if item["id"] != layer:
            raise SystemExit(f"item {layer}.json has id {item['id']!r}")
        if item["bbox"] != bboxes[layer]:
            raise SystemExit(f"item {layer} bbox {item['bbox']} does not match the footer bbox")
        if "data" not in item["assets"] or item["assets"]["data"]["type"] != PARQUET_MEDIA_TYPE:
            raise SystemExit(f"item {layer} has no GeoParquet data asset")
    print(f"ok  JSON catalog {CATALOG_ID!r} -> collection {COLLECTION_ID!r} with {len(LAYERS)} items")


def verify_item_table(out_dir: Path, bboxes: dict[str, list[float]]) -> None:
    table = pq.read_table(out_dir / "items.parquet")
    if table.column_names != ITEM_TABLE_SCHEMA.names:
        raise SystemExit(f"item-table columns {table.column_names} do not match the reader contract")
    rows = table.to_pylist()
    if len(rows) != len(LAYERS):
        raise SystemExit(f"item-table has {len(rows)} rows, expected {len(LAYERS)}")
    for row, layer in zip(rows, LAYERS):
        expected = bboxes[layer]
        actual = [row["bbox_xmin"], row["bbox_ymin"], row["bbox_xmax"], row["bbox_ymax"]]
        if row["item_id"] != layer or row["collection"] != COLLECTION_ID or actual != expected:
            raise SystemExit(f"item-table row for {layer} does not match its item document")
    print(f"ok  item-table items.parquet with {len(rows)} rows in collection {COLLECTION_ID!r}")


def link_href(document: dict, rel: str) -> str | None:
    for link in document["links"]:
        if link["rel"] == rel:
            return link["href"]
    return None


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


if __name__ == "__main__":
    raise SystemExit(main())
