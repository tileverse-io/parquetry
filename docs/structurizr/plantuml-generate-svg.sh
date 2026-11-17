#!/bin/bash
set -e

# Renders the PlantUML files under exports/ to SVG files under src/assets/images/structurizr/.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPORTS_DIR="${SCRIPT_DIR}/exports"
SVG_DIR="${SCRIPT_DIR}/../src/assets/images/structurizr"
# A pinned tag keeps a new upstream release from changing the diagrams of unchanged sources.
PLANTUML_IMAGE="plantuml/plantuml:1.2026.10"

# The legend of C4-PlantUML lists its default element colors, unused by the
# Parquetry styles. The Structurizr exporter always writes the SHOW_LEGEND call,
# even with the c4plantuml.legend property set to false.
remove_legend() {
    local puml_file="$1"
    sed -i.bak '/^SHOW_LEGEND(/d' "${puml_file}"
    rm -f "${puml_file}.bak"
}

mkdir -p "${SVG_DIR}"
docker pull --quiet "${PLANTUML_IMAGE}"

for puml_file in "${EXPORTS_DIR}"/*.puml; do
    base_name="$(basename "${puml_file}" .puml)"
    remove_legend "${puml_file}"
    echo "Rendering ${base_name}.svg"
    docker run --rm --user "$(id -u):$(id -g)" \
        -v "${EXPORTS_DIR}:/work" \
        "${PLANTUML_IMAGE}" -tsvg "/work/${base_name}.puml"
    mv -f "${EXPORTS_DIR}/${base_name}.svg" "${SVG_DIR}/${base_name}.svg"
done
