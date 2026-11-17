#!/bin/bash
set -e

# Serves the Parquetry documentation at http://127.0.0.1:8000 and reloads on changes.
# Run ./build.sh once before: it creates the virtual environment and the diagrams.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"

cd "${SCRIPT_DIR}"

if [ ! -d "${VENV_DIR}" ]; then
    echo "ERROR: virtual environment not found. Run ./build.sh first."
    exit 1
fi

source "${VENV_DIR}/bin/activate"

if ! command -v zensical >/dev/null 2>&1; then
    echo "ERROR: zensical not found in the virtual environment. Run ./build.sh first."
    exit 1
fi

echo "Serving the Parquetry documentation at http://127.0.0.1:8000 (Ctrl+C stops it)"
zensical serve "$@"
