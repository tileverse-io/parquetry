#!/bin/bash
set -e

# Exports the Structurizr workspaces to PlantUML, then renders the PlantUML files to SVG.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPORTS_DIR="${SCRIPT_DIR}/exports"
SVG_DIR="${SCRIPT_DIR}/../src/assets/images/structurizr"
# A pinned tag keeps a new upstream release from changing the diagrams of unchanged sources.
STRUCTURIZR_IMAGE="structurizr/structurizr:2026.10.26"

main() {
    mkdir -p "${EXPORTS_DIR}"
    docker pull --quiet "${STRUCTURIZR_IMAGE}"

    export_workspace "workspace.dsl"
    if [ -f "${SCRIPT_DIR}/dynamic-views.dsl" ]; then
        export_workspace "dynamic-views.dsl"
    fi
    require_files "${EXPORTS_DIR}" "puml"

    "${SCRIPT_DIR}/plantuml-generate-svg.sh"
    require_files "${SVG_DIR}" "svg"

    echo "Generated diagrams:"
    ls -1 "${SVG_DIR}"/*.svg
}

export_workspace() {
    local workspace="$1"
    echo "Exporting ${workspace} to PlantUML"
    docker run --rm --user "$(id -u):$(id -g)" \
        -v "${SCRIPT_DIR}:/usr/local/structurizr" \
        "${STRUCTURIZR_IMAGE}" export \
        -workspace "/usr/local/structurizr/${workspace}" \
        -format plantuml/c4plantuml \
        -output /usr/local/structurizr/exports
}

require_files() {
    local directory="$1"
    local extension="$2"
    if ! ls "${directory}"/*."${extension}" >/dev/null 2>&1; then
        echo "ERROR: no .${extension} files were generated in ${directory}"
        exit 1
    fi
}

main "$@"
