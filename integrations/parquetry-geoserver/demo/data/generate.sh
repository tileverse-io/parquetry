#!/usr/bin/env bash
#
# Regenerate the demo's GeoParquet layers from the Natural Earth GeoPackage that ships with
# the GeoServer release data directory (the same data behind GeoServer's stock "ne" workspace).
#
# Requires GDAL (ogr2ogr). Pass the path to natural_earth.gpkg, or set GPKG.
#
#   ./generate.sh /path/to/natural_earth.gpkg
#
# GDAL writes a GeoParquet 1.1 bbox covering (a float32 geom_bbox sidecar struct) by default.
# parquetry prunes spatial filters against it (row-group and page level) without decoding the WKB,
# and the GeoTools store keeps the covering columns out of the published feature type.

set -euo pipefail

GPKG="${1:-${GPKG:-$HOME/git/geoserver/geoserver/data/release/data/ne/natural_earth.gpkg}}"
OUT="$(cd "$(dirname "$0")" && pwd)/ne"

if [ ! -f "$GPKG" ]; then
  echo "GeoPackage not found: $GPKG" >&2
  exit 1
fi

mkdir -p "$OUT"
for layer in boundary_lines_land coastlines countries disputed_areas populated_places; do
  echo "converting $layer ..."
  rm -f "$OUT/$layer.parquet"
  ogr2ogr -f Parquet "$OUT/$layer.parquet" "$GPKG" "$layer"
done
echo "done -> $OUT"
