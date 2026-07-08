# Parquet in GeoServer - demo

GeoServer 3.0 serving [Natural Earth](https://www.naturalearthdata.com/) as live WMS/WFS layers,
read through the parquetry plugin from three sources - [GeoParquet](https://geoparquet.org/) files,
an [Apache Iceberg](https://iceberg.apache.org/) warehouse, and a [STAC](https://stacspec.org/)
catalog - each served both from local disk and over S3/HTTP.

## Run it

You only need Docker.

```bash
docker compose up
```

The first run builds the image (it downloads GeoServer and a Java 25 runtime - a few minutes,
needs internet). Later runs start instantly. Then open:

- Web admin: <http://localhost:8080/geoserver>  (user `admin`, password `geoserver`)
- Layer preview: Web admin -> Layer Preview -> `parquetry:world`

Stop it with `Ctrl-C`, or `docker compose down` to remove the container.

## What you get

Six workspaces, each reading Natural Earth through the parquetry plugin from a different source:

| workspace | reads from | shows off |
|---|---|---|
| `parquetry` | local GeoParquet files baked into the image | the GeoParquet read path with bbox pruning |
| `parquetry-s3` | the same GeoParquet over S3 (bundled s3proxy) | GeoParquet on cloud object storage |
| `iceberg` | a local Apache Iceberg warehouse | Iceberg reads: schema evolution, merge-on-read deletes, deletion vectors, row lineage |
| `iceberg-s3` | the same warehouse over S3 (bundled s3proxy) | an Iceberg warehouse on cloud object storage |
| `stac-json` | a static STAC catalog (JSON) over HTTP, assets on S3 | a public catalog with data on a credentialed object store |
| `stac-geoparquet` | a STAC items table (GeoParquet) on S3, assets on S3 | an item-table catalog colocated with its data |

### GeoParquet layers (`parquetry`, `parquetry-s3`)

Five Natural Earth layers (plus a `world` layer group):

| layer | geometry | features |
|---|---|---|
| `countries` | polygons | 242 |
| `boundary_lines` | lines | 390 |
| `coastlines` | lines | 1428 |
| `disputed_areas` | polygons | 28 |
| `populated_places` | points | 1251 |

### Iceberg layers (`iceberg`, `iceberg-s3`)

One Iceberg warehouse read as a single catalog, exposing eight tables across two namespaces. The
`ne.*` tables are the five Natural Earth layers; the `bonus.*` tables highlight Iceberg read
features:

| layer | shows off |
|---|---|
| `ne.countries`, `ne.coastlines`, `ne.boundary_lines_land` | plain Iceberg table reads |
| `ne.disputed_areas` | serves 23 rows through merge-on-read deletes |
| `ne.populated_places` | an evolved schema (attributes added after the table was created) |
| `bonus.deletion_vectors` | attribute-only; deletion vectors (WFS only) |
| `bonus.equality` | attribute-only; equality deletes (WFS only) |
| `bonus.geometry_lineage` | synthetic points exercising Iceberg row lineage |

The two attribute-only tables have no geometry column and are published for WFS only.

### STAC layers (`stac-json`, `stac-geoparquet`)

Both workspaces publish the five Natural Earth layers as STAC collections. Each item's asset href
is an absolute `s3://naturalearth/<layer>.parquet` URI, and the store resolves each asset through a
Storage keyed on the asset's own container URI, with the backend auto-detected per URI - not
relative to the catalog. The two workspaces demonstrate two auth patterns:

- `stac-json` reads a static JSON catalog over HTTP from the bundled `web` (nginx) service
  (`catalog.json`, per-collection `collection.json`, and item documents), while its GeoParquet
  assets live on the bundled `s3proxy` under `s3://naturalearth/`. This is a public HTTP catalog
  with its data on a credentialed object store.
- `stac-geoparquet` reads a stac-geoparquet items table (`s3://naturalearth/index/items.parquet`)
  and its assets both from `s3://naturalearth/` on `s3proxy` - an item-table catalog colocated with
  its data on the object store, covered by one credential set. The items table sits under `index/`
  rather than the bucket root because the GeoParquet directory store lists top-level `*.parquet` in
  that bucket as layers and a bare item table is not a feature file.

The store takes a single `storage.*` credential set. The catalog and its data must therefore be
reachable with one set: a public HTTP catalog plus private object-store data (`stac-json`), or an
item-table catalog colocated with the data on the object store (`stac-geoparquet`). A catalog on
one authenticated backend plus data on a different authenticated backend (two distinct credential
sets) is not supported.

### Reading from S3 the way you would in production

The `parquetry-s3` store has **no credentials**. GeoServer obtains them from the AWS
default credential chain - here, `secrets/aws/credentials` mounted as `~/.aws` - exactly as it
would use an instance role or environment credentials on real AWS. The bundled `s3proxy`
emulator is configured to accept those same demo keys and serves `data/ne` directly as the
bucket `naturalearth` and `data/iceberg-warehouse` as the bucket `warehouse` (bind mounts, no
upload); the `iceberg-s3` store reads that `warehouse` bucket the same credential-free way. To
point at real AWS instead, drop the `s3proxy` service, set your own `secrets/aws/credentials` (or
environment credentials), and edit the `storage.s3.*` parameters of the S3 stores.

Only `stac-json` reads its catalog over HTTP: the bundled `web` (nginx) service serves the STAC
JSON documents at `http://web/`. Its GeoParquet assets, and both the catalog and assets of
`stac-geoparquet`, are read from the `naturalearth` bucket on `s3proxy` the same credential-free
way as the `parquetry-s3` and `iceberg-s3` stores.

Example requests:

```bash
# WMS GetMap - the world map as a PNG
curl -o world.png \
  "http://localhost:8080/geoserver/parquetry/wms?service=WMS&version=1.1.1&request=GetMap\
&layers=parquetry:world&bbox=-180,-90,180,90&width=1000&height=500&srs=EPSG:4326&format=image/png"

# WFS GetFeature - GeoJSON
curl "http://localhost:8080/geoserver/parquetry/wfs?service=WFS&version=2.0.0&request=GetFeature\
&typeNames=parquetry:populated_places&count=5&outputFormat=application/json"
```

## Add your own GeoParquet layers (REST API)

The plugin registers a `GeoParquet` datastore type. Create stores and layers for your own
datasets through the GeoServer REST API (the examples use the default `admin` / `geoserver`
credentials). A store points at a single `.parquet` file, or at a directory of files (see
[Directory stores](#directory-stores) below).

### Connection parameters

| key | meaning |
|---|---|
| `geoparquet` | the dataset URI: `file:///path/to/file.parquet` or `s3://bucket/key.parquet` |
| `storage.provider` | `s3`, `azure`, `gcs`, `http`, or `file` (auto-detected from the URI when unset) |
| `storage.s3.region` | AWS region (required for S3, including S3-compatible services) |
| `storage.s3.endpoint` | only for S3-compatible services (MinIO, Cloudflare R2, the bundled s3proxy) |
| `storage.s3.force-path-style` | `true` for most S3-compatible services |

No credential parameters are needed when using the AWS default credential chain (environment
variables, `~/.aws/credentials`, or an instance role). To use static keys instead, add
`storage.s3.aws-access-key-id` and `storage.s3.aws-secret-access-key`; for a named profile, add
`storage.s3.default-credentials-profile`.

### 1. Create a workspace

```bash
curl -u admin:geoserver -XPOST -H "Content-Type: application/xml" \
  http://localhost:8080/geoserver/rest/workspaces \
  -d '<workspace><name>mydata</name></workspace>'
```

### 2a. A store over a local file

```bash
curl -u admin:geoserver -XPOST -H "Content-Type: application/xml" \
  http://localhost:8080/geoserver/rest/workspaces/mydata/datastores \
  -d '<dataStore><name>places</name><type>GeoParquet</type>
        <connectionParameters>
          <entry key="geoparquet">file:///data/places.parquet</entry>
        </connectionParameters>
      </dataStore>'
```

### 2b. A store over S3 with the default credential chain

The store has **no keys**; GeoServer obtains credentials from the AWS default chain. On real
AWS you only need the region:

```bash
curl -u admin:geoserver -XPOST -H "Content-Type: application/xml" \
  http://localhost:8080/geoserver/rest/workspaces/mydata/datastores \
  -d '<dataStore><name>places</name><type>GeoParquet</type>
        <connectionParameters>
          <entry key="geoparquet">s3://my-bucket/path/places.parquet</entry>
          <entry key="storage.provider">s3</entry>
          <entry key="storage.s3.region">us-east-1</entry>
        </connectionParameters>
      </dataStore>'
```

For an S3-compatible service (MinIO, R2, or the bundled `s3proxy`) also add the endpoint and
path-style, e.g. against the bundled emulator:

```xml
          <entry key="storage.s3.endpoint">http://s3proxy:80</entry>
          <entry key="storage.s3.force-path-style">true</entry>
```

### 3. Publish the layer

A single-file store exposes one feature type named after the file (without the `.parquet`
extension). Publish it - GeoServer computes the bounds from the data:

```bash
curl -u admin:geoserver -XPOST -H "Content-Type: application/xml" \
  http://localhost:8080/geoserver/rest/workspaces/mydata/datastores/places/featuretypes \
  -d '<featureType><name>places</name><nativeName>places</nativeName><srs>EPSG:4326</srs></featureType>'
```

The layer is then served at `mydata:places` (WMS and WFS). To discover the available feature
type name in a store, use:

```bash
curl -u admin:geoserver \
  "http://localhost:8080/geoserver/rest/workspaces/mydata/datastores/places/featuretypes.json?list=available"
```

If a batch of REST calls starts returning HTTP 401, GeoServer's brute-force protection is
throttling repeated logins from a non-local address; space the calls out.

### Directory stores

A store's `uri` can also point at a directory. With `layer-grouping=file` each top-level
`.parquet` file in the directory becomes its own layer, named after the file; this is how the
bundled `parquetry` and `parquetry-s3` workspaces serve their five Natural Earth layers from one
store. Without `layer-grouping`, a directory of files that share a schema reads as a single
merged layer.

## Notes

Replacing the bundled data: swap the files under `data/ne/` (and adjust the `ne` datastore under
`geoserver-data/workspaces/parquetry/`), then `docker compose up --build`.

Credentials are the GeoServer defaults (`admin` / `geoserver`); change them for anything beyond a
local demo.
