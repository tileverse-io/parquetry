# NE Iceberg warehouse generator

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
