#!/usr/bin/env bash
#
# Regenerate the demo's GeoParquet layers from the Natural Earth GeoPackage that ships with
# the GeoServer release data directory (the same data behind GeoServer's stock "ne" workspace).
#
# Requires GDAL (ogr2ogr). Pass the path to natural_earth.gpkg, or set GPKG.
#
#   ./generate.sh /path/to/natural_earth.gpkg
#
# WRITE_COVERING_BBOX=NO: GDAL otherwise writes a GeoParquet 1.1 bbox covering as float32, which
# the current parquetry release cannot compare against double query bounds (it throws on spatial
# filters). Omitting the covering makes parquetry evaluate spatial filters on the WKB directly.
# Drop this option once parquetry supports float32 covering columns.

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
  ogr2ogr -f Parquet -lco WRITE_COVERING_BBOX=NO "$OUT/$layer.parquet" "$GPKG" "$layer"
done
echo "done -> $OUT"
