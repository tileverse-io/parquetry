#!/bin/bash
set -e

# Removes the built site and the generated diagrams. The virtual environment stays.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "${SCRIPT_DIR}"

echo "Cleaning the Parquetry documentation build"

rm -rf site/
find structurizr/exports -name "*.puml" -delete 2>/dev/null || true
rm -f src/assets/images/structurizr/structurizr-*.svg

echo "Done. Rebuild with ./build.sh"
