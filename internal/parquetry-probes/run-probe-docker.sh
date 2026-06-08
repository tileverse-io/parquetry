#!/usr/bin/env bash
#
# Run a read-comparison probe inside a Docker container with real CPU and memory
# limits (cgroup-enforced): "cores" and "heap" then reflect what a pod actually sees.
#
# Why Docker: -XX:ActiveProcessorCount only changes what Runtime.availableProcessors()
# reports; the OS scheduler still spreads work across every physical core. Only a
# container CPU quota (--cpus) is a real core limit, and -m is a real memory limit.
#
# The probes are standalone main() apps shaded into a single self-contained jar
# (target/probes.jar). The container needs only that jar and the parquet file - no
# Maven, no local repository, no classpath assembly.
#
set -euo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$MODULE_DIR" rev-parse --show-toplevel)"
JAR="${MODULE_DIR}/target/probes.jar"

# Defaults
IMAGE="eclipse-temurin:25-jre"
CORES="4"
HEAP="2g"
MEMORY=""            # container -m; defaults to HEAP + 1g headroom
CONCURRENCY="8"
WARMUP="1"
MEASURE="3"
ENGINES=""
SCENARIOS=""
DECODE_AHEAD=""
ATTR_COLUMN=""
ATTR_VALUE=""
GEOMETRY_COLUMN=""
BBOX_COLUMN=""
CX=""
CY=""
R=""
FILE="${REPO_ROOT}/buildings.parquet"
PROBE="read"         # read | columnar
REBUILD=0
SILENT=0

usage() {
  cat <<'EOF'
run-probe-docker.sh - run a read-comparison probe under real Docker CPU/memory limits

USAGE:
  run-probe-docker.sh [options]

OPTIONS:
  --probe NAME       Which probe: "read" (row path) or "columnar". Default: read
  --engines LIST     Comma-separated, e.g. "parquetry" or "parquetry,parquet-java". Default: all.
  --file PATH        Parquet file to read. Default: <repo>/buildings.parquet

  Running environment (the simulated pod and the container/build):
    --cores N        Pin to N CPUs (docker --cpuset-cpus=0..N-1) - a real core restriction, not just a
                     bandwidth cap. Default: 4. (--cpus caps aggregate CPU time but lets the scheduler
                     spread threads across every core; --cpuset-cpus actually restricts which cores run.)
    --heap SIZE      JVM max heap (-Xmx), e.g. 1g, 2g, 512m. Default: 2g
    --memory SIZE    Container memory limit (docker -m). Default: heap + 1g headroom.
                     This is the pod-real cap the JVM sees as the container limit.
    --image NAME     Container image. Default: eclipse-temurin:25-jre
    --rebuild        Rebuild target/probes.jar before running.

  Probe tuning (measurement methodology and filters):
    --concurrency N  Reads in flight per engine/scenario. Default: 8
    --warmup N       Warmup passes. Default: 1
    --measure N      Measured passes. Default: 3
    --scenarios LIST Comma-separated: NO_FILTER,ATTRIBUTE,SPATIAL,ATTRIBUTE_AND_SPATIAL. Default: all.
    --decode-ahead N parquetry only: per-read decode-ahead window (row groups decoded concurrently per read). 1 makes a
                     read effectively serial - isolates intra-read parallelism from inter-read concurrency. Default: the
                     built-in heuristic (clamp by cores and heap budget).
    --attribute-column COL  Column for the ATTRIBUTE filter. Unset => ATTRIBUTE scenarios are skipped.
    --attribute-value VAL   Equality value for --attribute-column. Both are required to enable ATTRIBUTE.
    --geometry-column COL   Geometry column for SPATIAL. Default: GeoParquet primary column, else first
                            Geometry/Geography leaf. Unset and undetectable => SPATIAL scenarios skipped.
    --cx X --cy Y --r R     Query-diamond centre and half-diagonal for SPATIAL. Default: a central region
                            derived from the file's GeoParquet bbox (often sparse for clustered data; pass
                            these for a representative query, e.g. --cx -89.2 --cy 13.7 --r 0.15).
    --bbox-column PREFIX    GeoParquet 1.1 bbox covering struct for the parquet-java pushdown. Default: bbox.

  -s, --silent       Print only the table (suppress the header, "skipping", and footnote lines).
  -h, --help         This help.

EXAMPLES:
  # 8 concurrent reads, 8 real cores, 2 GB pod, both engines, all scenarios:
  ./run-probe-docker.sh --cores 8 --heap 2g --concurrency 8

  # GeoServer-pod stress: 2 real cores, 1 GB heap in a 1.5 GB container, parquetry only:
  ./run-probe-docker.sh --cores 2 --heap 1g --memory 1500m --engines parquetry

  # One scenario, single read, for churn profiling:
  ./run-probe-docker.sh --cores 1 --concurrency 1 --scenarios NO_FILTER --engines parquetry

  # Columnar path:
  ./run-probe-docker.sh --probe columnar --cores 8 --heap 2g

BUILD:
  The jar is built with:
    ./mvnw -Pprobes -pl :parquetry-probes -am package -DskipTests
  This script builds it automatically when missing or with --rebuild.

CORE LIMITING NOTES:
  - --cpuset-cpus pins the container to specific CPUs (true restriction + Runtime.availableProcessors()
    reports N). --cpus only caps aggregate bandwidth; the OS still spreads threads across all cores
    (you see many cores lightly loaded summing to N) - which is why "cores=4" looked unpinned.
  - On macOS/Windows the container runtime (OrbStack, Docker Desktop, ...) runs a Linux VM: the host CPU
    monitor shows the VM's vCPUs scheduled across host cores, NOT the container's limit. The cpuset is a
    real cgroup limit inside the VM regardless; verify it from the probe's printed availableProcessors=N.
    The VM must expose >= N CPUs: OrbStack exposes all host cores by default; Docker Desktop is fixed at
    Settings > Resources > CPUs.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --probe) PROBE="$2"; shift 2 ;;
    --cores) CORES="$2"; shift 2 ;;
    --heap) HEAP="$2"; shift 2 ;;
    --memory) MEMORY="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --measure) MEASURE="$2"; shift 2 ;;
    --engines) ENGINES="$2"; shift 2 ;;
    --scenarios) SCENARIOS="$2"; shift 2 ;;
    --decode-ahead) DECODE_AHEAD="$2"; shift 2 ;;
    --attribute-column) ATTR_COLUMN="$2"; shift 2 ;;
    --attribute-value) ATTR_VALUE="$2"; shift 2 ;;
    --geometry-column) GEOMETRY_COLUMN="$2"; shift 2 ;;
    --bbox-column) BBOX_COLUMN="$2"; shift 2 ;;
    --cx) CX="$2"; shift 2 ;;
    --cy) CY="$2"; shift 2 ;;
    --r) R="$2"; shift 2 ;;
    --file) FILE="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --rebuild) REBUILD=1; shift ;;
    -s|--silent) SILENT=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

case "$PROBE" in
  read|columnar) ;;
  *) echo "--probe must be 'read' or 'columnar', got: $PROBE" >&2; exit 2 ;;
esac

if [[ ! -f "$FILE" ]]; then
  echo "Probe file not found: $FILE" >&2
  exit 1
fi
FILE="$(cd "$(dirname "$FILE")" && pwd)/$(basename "$FILE")"

if [[ $REBUILD -eq 1 || ! -f "$JAR" ]]; then
  echo ">> Building $JAR ..." >&2
  # Lombok's annotation processor calls the terminally-deprecated sun.misc.Unsafe in the build JVM; allow it quietly.
  ( cd "$REPO_ROOT" \
    && MAVEN_OPTS="${MAVEN_OPTS:-} --sun-misc-unsafe-memory-access=allow" \
       ./mvnw -q -Pprobes -pl :parquetry-probes -am package -DskipTests )
fi

if [[ -z "$MEMORY" ]]; then
  # default container limit: heap + 1g headroom for native/metaspace/threads/page cache
  case "$HEAP" in
    *g|*G) MEMORY="$(( ${HEAP%[gG]} * 1024 + 1024 ))m" ;;
    *m|*M) MEMORY="$(( ${HEAP%[mM]} + 1024 ))m" ;;
    *) MEMORY="3g" ;;
  esac
fi

FILE_NAME="$(basename "$FILE")"

JVM_FLAGS=(
  --enable-preview
  --enable-native-access=ALL-UNNAMED
  # Silence the terminally-deprecated sun.misc.Unsafe warning from Hadoop's vendored Guava (AbstractFuture).
  --sun-misc-unsafe-memory-access=allow
  -Dfile.encoding=UTF-8
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  # Arrow (DuckDB's columnar arm) needs direct ByteBuffer access for its off-heap allocator.
  --add-opens=java.base/java.nio=ALL-UNNAMED
  "-Xmx${HEAP}"
)

PROBE_PROPS=(
  "-Dparquetry.probe.file=/data/${FILE_NAME}"
  "-Dparquetry.probe.concurrency=${CONCURRENCY}"
  "-Dparquetry.probe.warmup=${WARMUP}"
  "-Dparquetry.probe.measure=${MEASURE}"
)
[[ -n "$ENGINES" ]] && PROBE_PROPS+=("-Dparquetry.probe.engines=${ENGINES}")
[[ -n "$SCENARIOS" ]] && PROBE_PROPS+=("-Dparquetry.probe.scenarios=${SCENARIOS}")
[[ -n "$DECODE_AHEAD" ]] && PROBE_PROPS+=("-Dparquetry.probe.decodeAhead=${DECODE_AHEAD}")
[[ -n "$ATTR_COLUMN" ]] && PROBE_PROPS+=("-Dparquetry.probe.attribute.column=${ATTR_COLUMN}")
[[ -n "$ATTR_VALUE" ]] && PROBE_PROPS+=("-Dparquetry.probe.attribute.value=${ATTR_VALUE}")
[[ -n "$GEOMETRY_COLUMN" ]] && PROBE_PROPS+=("-Dparquetry.probe.geometry.column=${GEOMETRY_COLUMN}")
[[ -n "$BBOX_COLUMN" ]] && PROBE_PROPS+=("-Dparquetry.probe.bbox.column=${BBOX_COLUMN}")
[[ -n "$CX" ]] && PROBE_PROPS+=("-Dparquetry.probe.cx=${CX}")
[[ -n "$CY" ]] && PROBE_PROPS+=("-Dparquetry.probe.cy=${CY}")
[[ -n "$R" ]] && PROBE_PROPS+=("-Dparquetry.probe.r=${R}")
[[ $SILENT -eq 1 ]] && PROBE_PROPS+=("-Dparquetry.probe.silent=true")

if [[ $SILENT -ne 1 ]]; then
  echo ">> probe=${PROBE} cores=${CORES} heap=${HEAP} container=${MEMORY} concurrency=${CONCURRENCY}" >&2
  echo ">> file=${FILE}" >&2
fi

status=0
docker run --rm -i \
  --cpuset-cpus="0-$((CORES - 1))" \
  -m "${MEMORY}" \
  -v "${JAR}:/probes.jar:ro" \
  -v "${FILE}:/data/${FILE_NAME}:ro" \
  "${IMAGE}" \
  java "${JVM_FLAGS[@]}" "${PROBE_PROPS[@]}" -jar /probes.jar "${PROBE}" || status=$?

# 137 = 128 + SIGKILL: the container OOM-killer hit the -m limit and killed the JVM before it could print.
if [[ $status -eq 137 ]]; then
  echo "" >&2
  echo "!! OOM-KILLED (exit 137): the run exceeded the ${MEMORY} container memory limit and the kernel killed the" >&2
  echo "   JVM with SIGKILL before it could print a table. This is an OFF-HEAP overrun: -Xmx (${HEAP}) bounds only" >&2
  echo "   the Java heap, not native/off-heap memory, and a container OOM-kill is not a catchable Java OutOfMemoryError." >&2
  echo "   DuckDB's internal buffer pool is off-heap and scales with concurrency (~1+ GB per connection), so" >&2
  echo "   ${CONCURRENCY} concurrent scans can blow past ${MEMORY} that -Xmx never sees. Lower --concurrency or raise" >&2
  echo "   --memory; that an engine's off-heap footprint can SIGKILL a pod the JVM heap limit cannot protect is the" >&2
  echo "   point this probe exists to show." >&2
fi
exit $status
