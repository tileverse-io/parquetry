#!/bin/bash
set -e

# Builds the Parquetry documentation site end to end:
# Python virtual environment, C4 diagrams, then the static site.
#
# GOOGLE_ANALYTICS_KEY, a Google Analytics measurement ID, adds analytics to the
# pages. The deploy job sets it; local builds leave it out.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"

main() {
    cd "${SCRIPT_DIR}"
    echo "Building the Parquetry documentation"

    check_dependencies
    prepare_python_environment
    generate_diagrams
    build_site

    echo ""
    echo "Documentation built at ${SCRIPT_DIR}/site/"
    echo "Serve it locally with ./serve.sh"
}

check_dependencies() {
    require_command python3 "Python 3 is required"
    require_command docker "Docker is required to generate the diagrams"
}

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "ERROR: $2"
        exit 1
    fi
}

prepare_python_environment() {
    echo ""
    echo "Preparing the Python virtual environment at ${VENV_DIR}"
    if [ ! -d "${VENV_DIR}" ]; then
        python3 -m venv "${VENV_DIR}"
    fi
    source "${VENV_DIR}/bin/activate"
    python -m pip install --quiet --upgrade pip
    pip install --quiet -r requirements.txt
}

generate_diagrams() {
    echo ""
    echo "Generating the C4 diagrams"
    "${SCRIPT_DIR}/structurizr/structurizr-generate-diagrams.sh"
}

build_site() {
    echo ""
    echo "Building the site"
    zensical --version
    zensical build --clean --strict
}

main "$@"
