# NE demo-data generators

Two generators turn the five Natural Earth GeoParquet layers under
`integrations/parquetry-geoserver/demo/data/ne/` into committed demo data. Both
share the venv below.

- `build_ne_warehouse.py` builds the static Iceberg warehouse (this file's main
  subject).
- `build_stac_demo.py` builds the STAC catalog in the two flavors the STAC
  DataStore opens; see [STAC demo data](#stac-demo-data).

## NE Iceberg warehouse generator

`build_ne_warehouse.py` turns the five Natural Earth GeoParquet layers in
`integrations/parquetry-geoserver/demo/data/ne/` into a static Apache Iceberg
warehouse committed under
`integrations/parquetry-geoserver/demo/data/iceberg-warehouse/`. The demo
GeoServer image serves this warehouse through the parquetry Iceberg DataStore.

The warehouse publishes one dotted dataset name per layer:

| dataset | data files | rows |
| --- | --- | --- |
| `ne.boundary_lines_land` | 1 | 390 |
| `ne.coastlines` | 1 | 1428 |
| `ne.countries` | 8 (one per continent) | 242 |
| `ne.disputed_areas` | 1 | 28 |
| `ne.populated_places` | 1 | 1251 |

Every table is Iceberg format-version 3 with a native `geometry` column
(CRS84, WKB in Parquet's native Geometry logical type). `countries` is split
into one data file per `CONTINENT` value, each with its own geometry bounds
(`packed_xy_le`) recorded in the manifest, which lets a bbox query prune whole
continent files.

The generator reuses the pinned `iceberg-geo-testbed` submodule's static-catalog
writer (`testbed._static_catalog.write_static_catalog`), imported the same way
`../regenerate.py` imports it.

## Run

```bash
cd internal/parquetry-testkit/fixtures-gen
python3 -m venv /tmp/ne-warehouse-venv
source /tmp/ne-warehouse-venv/bin/activate
pip install -r requirements.txt
python ne-warehouse/build_ne_warehouse.py
```

Requires the submodule checkout (shared with `regenerate.py`):

```bash
git submodule update --init \
    internal/parquetry-testkit/fixtures-gen/iceberg-geo-testbed
```

## Determinism

Two runs produce byte-identical output. The only wall-clock / entropy inputs are
the snapshot-id (`time.time`), the table-uuid (`uuid.uuid4`), and the Avro OCF
sync marker (`os.urandom`); all three are pinned per table, exactly as
`regenerate.py` pins them for the testbed goldens. The recorded table `location`
is a synthetic root (`file:///warehouse/ne/<table>`), not this checkout's
absolute path. The output does not depend on where the repository lives: the
Iceberg reader treats `location` as a prefix to strip and re-roots each data
file onto wherever the warehouse is physically opened.

Verify a clean regeneration:

```bash
WH=../../../integrations/parquetry-geoserver/demo/data/iceberg-warehouse
cp -r "$WH" /tmp/wh-run1
python ne-warehouse/build_ne_warehouse.py
diff -r /tmp/wh-run1 "$WH" && echo BYTE-IDENTICAL
```

Byte-identity also depends on the pinned toolchain in `requirements.txt`
(pyarrow stamps its version into each Parquet footer); do not bump those
versions without regenerating.

## STAC demo data

`build_stac_demo.py` builds a small STAC catalog over the same five NE layers,
committed under `integrations/parquetry-geoserver/demo/data/stac/` in the two
shapes the STAC DataStore factory auto-detects by URI extension. Each layer is
its own STAC collection (collection id = layer name) with a single item, because
the five layers have wildly different attribute schemas and one feature type per
file is the only correct mapping:

- a static JSON catalog: `catalog.json` links five child collections, one per
  layer (`<layer>/collection.json`), each linking its one item
  (`<layer>/items/<layer>.json`); the shape `JsonStacReader` parses;
- a stac-geoparquet item-table `items.parquet`, one row per layer with the
  columns `item_id`, `collection`, `bbox_xmin`, `bbox_ymin`, `bbox_xmax`,
  `bbox_ymax`, `asset_href` and `collection` = the layer name; the shape
  `GeoParquetStacReader` reads (it groups rows by `collection`).

Both flavors publish the same five collections named by layer and point each
item's data asset at the same external GeoParquet part. The type names a store
lists are the five layer names. Per-layer bboxes come from each NE file's
GeoParquet footer metadata, never hardcoded.

The committed data points every asset at `http://web/<layer>.parquet`'s base
`http://web/ne`, the compose-internal nginx hostname the demo image serves the
NE parts from. `--href-base` overrides that base; `--out` overrides the output
directory.

```bash
cd internal/parquetry-testkit/fixtures-gen
source /tmp/ne-warehouse-venv/bin/activate
python ne-warehouse/build_stac_demo.py
```

### Determinism

Two runs produce byte-identical output. The JSON documents are built as
fixed-order dicts and written with `json.dumps(indent=2)` (no `sort_keys`;
Python preserves insertion order), and `items.parquet` is written with the same
pinned zstd toolchain the warehouse uses. There are no wall-clock or entropy
inputs.

### Open both flavors locally

The factory roots file storage at the catalog's container and rejects asset keys
that escape it. A local open therefore needs the NE parts reachable under that
container. Generate into a scratch directory that holds a copy of the parts, and
point `--href-base` at that copy:

```bash
SCRATCH=$(mktemp -d)
mkdir "$SCRATCH/parts"
cp ../../../integrations/parquetry-geoserver/demo/data/ne/*.parquet "$SCRATCH/parts/"
python ne-warehouse/build_stac_demo.py --out "$SCRATCH" \
    --href-base "file://$SCRATCH/parts"
# geoparquet-stac=file://$SCRATCH/catalog.json   opens the JSON flavor
# geoparquet-stac=file://$SCRATCH/items.parquet  opens the item-table flavor
```
