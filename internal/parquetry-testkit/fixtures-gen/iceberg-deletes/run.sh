#!/usr/bin/env bash
# Launch the Iceberg merge-on-read positional-delete fixture builder in Docker.
#
# Reuses apache/sedona:1.6.1 (Spark 3.4.1 / Scala 2.12) only for its bundled
# Spark; the Iceberg runtime is pulled via --packages (cached in ~/.ivy2 after
# the first run). No Sedona/geo dependency is used here.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

ICEBERG_PKG="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.7.1"
PYFILE="${1:-$HERE/build_positional_deletes.py}"

mkdir -p "$HOME/.ivy2"

exec docker run --rm \
  --entrypoint /opt/spark/bin/spark-submit \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  -e PYSPARK_PYTHON=python3 \
  -v "$HERE":"$HERE" \
  -v "$HOME/.ivy2":/root/.ivy2 \
  -w "$HERE" \
  apache/sedona:1.6.1 \
    --packages "$ICEBERG_PKG" \
    --conf spark.driver.memory=2g \
    "$PYFILE"
