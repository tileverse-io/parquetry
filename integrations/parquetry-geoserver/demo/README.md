# GeoParquet in GeoServer - demo

A one-command demo that serves [Natural Earth](https://www.naturalearthdata.com/) as live WMS
and WFS layers in [GeoServer](https://geoserver.org/), reading from
[GeoParquet](https://geoparquet.org/) files through the parquetry GeoParquet plugin. No GeoServer
install, no manual configuration.

## What it is

A self-contained Docker image: GeoServer 3.0 plus the parquetry GeoParquet plugin, on Java 25,
plus a bundled `s3proxy` S3 emulator. Two workspaces (mirroring GeoServer's stock `ne` workspace,
but reading GeoParquet) are baked in. Working layers are served the moment the stack boots:

| layer | geometry | features | default style |
|---|---|---|---|
| `countries` | polygons | 242 | countries_mapcolor9 |
| `boundary_lines` | lines | 390 | boundary_lines |
| `coastlines` | lines | 1428 | coastline |
| `disputed_areas` | polygons | 28 | disputed |
| `populated_places` | points | 1251 | populated_places |

Each appears in two workspaces, plus a `world` layer group ("World Map") in each:

- **`parquetry`** - reads all five layers from a single GeoParquet directory store
  (`layer-grouping=file`, one layer per file) on the local filesystem (baked into the image).
- **`parquetry-s3`** - reads the same data over S3 from the bundled `s3proxy` emulator, again as a
  single directory store. The store has no credentials; GeoServer uses the AWS default credential
  chain (`secrets/aws/credentials` mounted as `~/.aws`), and s3proxy serves `data/ne` as the bucket
  `naturalearth` (a bind mount, no upload). This mirrors a real deployment reading from S3 with an
  instance role or environment credentials.

## Prerequisites

- Docker (with Compose)
- To rebuild the plugin from source: a JDK 25 (the bundled `./mvnw` is used; nothing else)

## Run it

The `Makefile` is one level up, in the `parquetry-geoserver` module directory. From there:

```bash
make demo
```

That builds the plugin zip, builds the image, and starts GeoServer. Then open:

- Web admin: <http://localhost:8080/geoserver>  (user `admin`, password `geoserver`)
- Layer preview: Web admin -> Layer Preview -> `parquetry:world`

Stop it with `make down`. Follow startup logs with `make logs`. If the image is already built,
`docker compose up` from this directory is enough.

## Build a customer zip

```bash
make dist
```

produces `target/parquetry-geoserver-demo.zip`: a self-contained bundle (Dockerfile, the prebuilt
plugin zip, the preconfigured data directory and the data, and a compose file). The recipient
needs only Docker - they unzip it and run `docker compose up`, with no checkout of this
repository and no image registry. The first `up` builds the image locally.

## Try the services

WMS GetMap (the world map):

```bash
curl -o world.png \
  "http://localhost:8080/geoserver/parquetry/wms?service=WMS&version=1.1.1&request=GetMap\
&layers=parquetry:world&bbox=-180,-90,180,90&width=1000&height=500&srs=EPSG:4326&format=image/png"
```

WFS GetFeature (GeoJSON):

```bash
curl "http://localhost:8080/geoserver/parquetry/wfs?service=WFS&version=2.0.0&request=GetFeature\
&typeNames=parquetry:populated_places&count=5&outputFormat=application/json"
```

## The data

The GeoParquet files in `data/ne/` are converted from the Natural Earth GeoPackage that ships
with the GeoServer release (`data/release/data/ne/natural_earth.gpkg`) using GDAL. Regenerate them
with `data/generate.sh` (see that script for the source path and conversion options). To use your
own data, drop GeoParquet files in `data/ne/` and adjust the `ne` datastore under
`geoserver-data/workspaces/parquetry/`.

## How it is built

- `make plugin` runs `./mvnw -pl :parquetry-geoserver -Passembly package`, producing the standard
  GeoServer-plugin zip (the plugin jar plus its runtime dependencies; libraries GeoServer already
  ships are excluded). The zip is copied to `build/plugin.zip`.
- The `Dockerfile` downloads the official GeoServer 3.0 platform-independent binary, drops the old
  bundled Marlin renderer (incompatible with JDK 24+), unzips the plugin into `WEB-INF/lib`, and
  bakes in the preconfigured `parquetry` workspace and the GeoParquet data.
- GeoServer runs on Java 25 with the runtime options proven by GeoServer Cloud
  (`--enable-preview`, the required `--add-opens`, and the Apple-M4/Docker SVE workaround).
- GeoServer regenerates the rest of its data directory (services, default styles, security) on
  first boot; only the `parquetry` workspace is committed here.

## Notes

- Credentials are the GeoServer defaults (`admin` / `geoserver`); change them for anything beyond
  a local demo.
- This serves GeoParquet from local files. Reading from object storage (S3/Azure/GCS/HTTP) through
  the same plugin is supported via the datastore's `storage.*` parameters; an opt-in cloud profile
  (S3 via s3proxy, no upload step) is planned for this demo.
