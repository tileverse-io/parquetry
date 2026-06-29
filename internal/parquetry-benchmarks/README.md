# parquetry-benchmarks

JMH microbenchmarks for the parquetry read and write paths, plus a few comparison
probes (see below). Build and dev only: this module is never published to Maven
Central; the normal `./mvnw verify` compiles the benchmarks (which never run in
CI) and its only tests are the sysprop-controlled probes, which skip unless you
point them at a file.

## Build the runner

The benchmarks run from a shaded, self-contained jar produced only under the
`benchmarks` profile:

```bash
./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
```

This writes `internal/parquetry-benchmarks/target/benchmarks.jar` with
`org.openjdk.jmh.Main` as its entry point.

## Run

parquetry compiles with Java preview features and uses the Foreign Function &
Memory API; both the launching JVM and the JVMs that JMH forks need the preview
and native-access flags, plus `--sun-misc-unsafe-memory-access=allow` to silence
JMH's own deprecated-`Unsafe` warning. Pass them on the launching command. JMH
inherits the launching JVM's arguments into each fork, hence the forked benchmark
JVMs receive them too without any per-benchmark `@Fork` configuration:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -jar internal/parquetry-benchmarks/target/benchmarks.jar
```

`make benchmarks` builds the runner and runs the full suite in one step.

Pass a regular expression to run a subset, and standard JMH options to tune the
run:

```bash
# one benchmark class
java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -jar internal/parquetry-benchmarks/target/benchmarks.jar PagePruningBenchmark

# list everything the jar contains
java ... -jar .../benchmarks.jar -l

# quick smoke run (1 fork, 1 warmup, 3 short measurement iterations)
java ... -jar .../benchmarks.jar PagePruningBenchmark -f 1 -wi 1 -i 3 -r 1 -w 1

# pin a parameter, add the GC profiler, write JSON
java ... -jar .../benchmarks.jar PagePruningBenchmark -p layout=SORTED -prof gc -rf json -rff results.json
```

Each benchmark declares its own warmup/measurement/fork defaults via
annotations; a plain run needs no tuning flags.

## Sanity check (smoke)

Every benchmark has a `smoke` parameter (default `false`). When `true`, each
benchmark shrinks its own fixture to a few thousand rows while keeping every
code path it exercises (the same parameter axes, multiple pages and row groups,
a built bloom filter, a predicate that survives into the page-pruning tier).
This is for catching breakage, not for timing.

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -jar internal/parquetry-benchmarks/target/benchmarks.jar \
  -p smoke=true -f 0 -wi 1 -i 1 -r 1 -foe true
```

`-f 0` runs in-process, `-wi 1 -i 1 -r 1` does one short warmup and one short
measurement iteration, and `-foe true` makes the run exit non-zero if any
benchmark throws (the point of the check). It asserts that every benchmark
still compiles, shades, and executes, not how fast it runs.

Run it locally with `make benchmarks-smoke` (builds the runner, then runs the
smoke). CI splits the two phases into `make build-benchmarks` and
`make run-benchmarks-smoke`, which are the same targets the one-shot composes.

## Benchmarks

| Class | Measures | Key parameters |
|-------|----------|----------------|
| `DecodeBenchmark` | Raw per-column decode throughput in isolation: a single-column file read in full through `readBatches`, every decoded value folded into a sink, with no filter, projection narrowing, or record assembly in the way. This is the only benchmark that times raw decode rather than the filter, pruning, or spatial machinery around it, and the harness for confirming a change to the decode path stays neutral on throughput. The `scenario` axis pairs a primitive kind with a forced on-disk encoding (PLAIN / DICTIONARY / DELTA / BYTE_STREAM_SPLIT, only the pairings a kind supports); `nullable` contrasts the all-present PLAIN fast path (values sliced straight from the page) against the null-positioning spread plus validity bitmap. The matrix is wide on purpose -- pin `scenario`/`nullable` to time the slice a change touches. | `scenario` (`INT32`/`INT64`/`FLOAT`/`DOUBLE`/`BOOLEAN`/`BYTE_ARRAY`/`FLBA` x the encodings each supports), `nullable` (`true`/`false`) |
| `PagePruningBenchmark` | What column-index page pruning buys a filtered read: writes one row group with many small pages, then reads a 1%-selective band predicate with the COLUMN_INDEX tier on vs. off. | `layout` (`SORTED` clusters matches into a few pages; `SHUFFLED` scatters them across every page, leaving nothing to prune), `useColumnIndex` (`true`/`false`) |
| `FilterTierBenchmark` | How far each metadata tier reduces a selective point lookup over a ten-row-group file. The result shows page pruning (COLUMN_INDEX) dominating for point lookups while row-group pruning (STATS/BLOOM) helps little. | `tiers` (`RECORD_ONLY`, `STATS`, `COLUMN_INDEX`, `BLOOM`, `ALL`) |
| `LateMaterializationBenchmark` | Whether decoding output columns only for predicate-matching rows (late materialization) beats decoding all surviving rows up front. Uses a 16-column wide table. The win is largest on `SHUFFLED` data where column-index page pruning cannot skip pages and late materialization is the only lever; on `SORTED` data page pruning already skips non-matching pages and the two arms approach parity. | `layout` (`SORTED` clusters matches; `SHUFFLED` scatters them, defeating page pruning), `selectivity` (`POINT`, `P1`, `P10`), `lateMaterialize` (`true`/`false`) |
| `SpatialPruningBenchmark` | Row-group pruning benefit of spatially clustered GeoParquet data. A parquetry-written file holds native per-row-group geometry bounding boxes; a `bboxIntersects` query skips row groups whose bbox is disjoint from the query only when the data is spatially clustered. `CLUSTERED` writes each row group into a distinct grid tile -- the query hits two of sixteen row groups; `SHUFFLED` randomises row order, making every row group's bbox span the full extent and forcing record-level WKB evaluation on every row. The `geometrySize` axis adds `LARGE` (2000-vertex dense ring) to expose how WKB-walk cost scales with vertex count on the unclusterd arm. | `layout` (`CLUSTERED` one row group per grid tile; `SHUFFLED` every bbox spans the full extent), `geometrySize` (`SMALL` 5-point rectangle; `LARGE` 2000-vertex ring) |
| `SpatialGateBenchmark` | Cost of the exact geometry gate (`GeometryFilter`) in isolation, with no geometry engine. All rows are inside the query bbox, making the pruning tier a no-op; the gate alone controls which rows materialise. `gate=off` reads with a plain bbox predicate -- every candidate row decodes all output columns. `gate=on` applies the synthetic `GeometryFilter` -- rows the gate rejects never decode their wide output columns. The `passRate` axis (HIGH ~90 %, LOW ~10 %) sets how many rows pass; `width` (4 or 16 extra INT32 columns) scales per-row materialisation cost; `geometrySize` scales the WKB decode in the gate itself. | `gate` (`off`/`on`), `passRate` (`HIGH`/`LOW`), `width` (`4`/`16`), `geometrySize` (`SMALL`/`LARGE`) |
| `WkbReadBenchmark` | Head-to-head decode cost of two WKB-to-JTS readers over one representative geometry per (shape, size): `JTS_PACKED` is JTS's own `WKBReader` over a packed-coordinate `GeometryFactory` (the production-realistic baseline, fed a `byte[]`); `CUSTOM` is `MemorySegmentWkbReader`, which reads straight from a read-only `MemorySegment` onto packed sequences with one bulk copy per ring. The time gap is the headline (the custom reader is roughly an order of magnitude faster on dense geometries); per-op allocation is near-parity on LARGE because the packed `double[]` backing both readers create dominates. Run with `-prof gc` to read `gc.alloc.rate.norm`. | `shape` (`POINT`/`LINESTRING`/`POLYGON`/`MULTIPOLYGON`), `geometrySize` (`SMALL` ~12 vertices; `LARGE` ~2000), `reader` (`JTS_PACKED`/`CUSTOM`) |
| `JtsSpatialFilterBenchmark` | End-to-end cost of three query strategies using the real JTS geometry engine and a non-axis-aligned diamond query polygon whose bbox over-selects. `BBOX_ONLY` reads all bbox-candidates (coarse superset); `IN_CORE_GATE` pushes `JtsGeometryFilter.intersects` into the read pipeline and avoids materialising the other columns of rows the exact JTS test rejects; `APP_SIDE_FILTER` reads with `BBOX_ONLY` (full materialization of all candidates) and then re-applies the exact JTS test in the stream. `IN_CORE_GATE` and `APP_SIDE_FILTER` return the same exact rows; `BBOX_ONLY` returns a superset. The `layout` axis shows row-group pruning effectiveness; `selectivity` (HIGH small diamond, LOW large diamond) scales how many rows the exact test rejects; `geometrySize` scales per-row WKB decode cost; `output` (`WKB`/`JTS`) selects whether the geometry output column is kept as raw bytes or parsed into a JTS `Geometry` (the JTS-minus-WKB gap on `IN_CORE_GATE` is the surviving rows' output parse, bounding what reusing the gate's decoded geometry as output would save). | `mode` (`BBOX_ONLY`/`IN_CORE_GATE`/`APP_SIDE_FILTER`), `layout` (`CLUSTERED`/`SHUFFLED`), `selectivity` (`HIGH`/`LOW`), `geometrySize` (`SMALL`/`LARGE`), `output` (`WKB`/`JTS`) |
| `CountBenchmark` | The optimized `ParquetFileReader.count(predicate)` path against the `read(predicate).count()` baseline over a sorted `id INT64 + value DOUBLE` table of several row groups. The two `@Benchmark` methods (`optimizedCount`, `readCountBaseline`) form the optimized-vs-baseline axis; the `path` parameter selects how the count resolves. `ALWAYS_TRUE` sums per-row-group counts from metadata; `MATCHED` (`id >= 0`, sorted non-null) proves every row group matches and again counts from metadata; `ELIMINATED` (`id` above every group's max) prunes all row groups; `RESIDUAL` (`id > rows / 2`) forces record-level evaluation on the undecided middle groups; `IS_NOT_NULL` settles from the metadata null counts. COMPARISON, SPATIAL, IS_NULL, and PARTIAL paths are deferred: they need non-sorted, nullable, or geometry fixtures that do not exist, and this class does not cover them. | `path` (`ALWAYS_TRUE`/`MATCHED`/`ELIMINATED`/`RESIDUAL`/`IS_NOT_NULL`) |
| `FetchSpillBenchmark` | The resident-memory cost of reading a large-row-group file under a deliberately tiny fetch budget. A mandatory fetch buffers a whole row group; pinning the fetch budget tiny (via `ResourceLimits.fixed`, whose derived budget is ten percent of the stated memory) forces every mandatory fetch off pooled native RAM and onto a mapped on-disk file (reclaimable page cache). The signal is peak resident set size (RSS), reported as the `peakRssKib` secondary counter: the `concurrency x row-group-span` overflow lands on reclaimable mmap rather than anonymous RAM. RSS is read from the OS with `ps` because file-backed mappings are invisible to the JVM's heap and allocation counters; it includes the JVM's own resident baseline (heap, metaspace, code cache), which dominates a small smoke fixture. This is a sizing tool, not a correctness check. | `concurrency` (`1`/`2`/`4`, overlapping reads per op) |

## Decode baseline

A reference point for `DecodeBenchmark`, to compare a decode-path change against. Average time per full read of a
1,000,000-row single-column file (`ms/op`, lower is faster). Measured on a dev host (Temurin 25.0.2, default annotation
settings: 2 warmup + 3 measurement iterations, one fork); error bars are wide on a few rows at this iteration count.
Re-measure on the target host before drawing conclusions.

| scenario | all-valid | nullable (~10% null) |
|----------|-----------|----------------------|
| `INT32_PLAIN` | 2.2 | 7.0 |
| `INT32_DICTIONARY` | 9.8 | 12.4 |
| `INT32_DELTA` | 10.0 | 12.3 |
| `INT64_PLAIN` | 2.5 | 8.4 |
| `INT64_DICTIONARY` | 10.2 | 12.9 |
| `INT64_DELTA` | 10.1 | 12.7 |
| `FLOAT_PLAIN` | 2.2 | 7.8 |
| `FLOAT_BYTE_STREAM_SPLIT` | 10.0 | 12.6 |
| `DOUBLE_PLAIN` | 3.1 | 7.4 |
| `DOUBLE_BYTE_STREAM_SPLIT` | 10.1 | 12.6 |
| `BOOLEAN_PLAIN` | 2.2 | 6.7 |
| `BINARY_PLAIN` | 21.2 | 19.0 |
| `BINARY_DICTIONARY` | 5.9 | 12.5 |
| `BINARY_DELTA` | 6.0 | 12.3 |
| `FLBA_PLAIN` | 16.4 | 15.5 |
| `FLBA_DICTIONARY` | 8.5 | 15.3 |

What the shape confirms (the benchmark discriminates the paths a decode change would touch): the all-present PLAIN
fixed-width path is the floor (~2-3 ms), because it slices values straight from the page; dictionary, delta, and
byte-stream-split each add ~7-8 ms of decode; variable-length `BINARY_PLAIN` is the heaviest and, tellingly, slower than
`BINARY_DICTIONARY` (index decode beats re-parsing length-prefixed bytes); and on the fixed-width PLAIN path `nullable`
roughly triples the time, the cost of the null-positioning spread and the validity bitmap over the all-valid live-page
slice.

## Fetch-spill characterization

`FetchSpillBenchmark` generates its own fixture in `@Setup` from
`LargeRowGroupFixture`, the writer pinning the row group to a byte budget
(`WriteOptions.RowGroupSize.bytes`). No external file and no committed test data
are needed: the fixture is written to a temp directory and deleted in
`@TearDown`. The default fixture pins a 128 MiB row group; the `smoke` shrink
uses a 4 MiB row group while keeping the same valve and concurrency axes.

Run it from the shaded jar:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -jar internal/parquetry-benchmarks/target/benchmarks.jar FetchSpillBenchmark
```

`peakRssKib` is whole-process resident memory, not the spilled-fetch delta in
isolation: it counts the JVM's own resident baseline (heap, metaspace, code
cache) plus whatever pages of the mapped spill files the kernel keeps resident.
The read it characterizes is whether peak RSS stays bounded as concurrency
climbs (the overflow being reclaimable mmap) rather than growing by a full
row-group span per concurrent read (which it would if every fetch took anonymous
RAM).

Smoke results (`-p smoke=true -f 0 -wi 1 -i 1 -r 1 -w 1`, in-process, one
iteration, on the 4 MiB-row-group fixture) -- measured, characterization only,
not a forked measurement run:

| concurrency | peakRssKib (measured, smoke) |
|-------------|------------------------------|
| 1           | 620800                       |
| 2           | 1026528                      |
| 4           | 1040048                      |

A forked default run on the 128 MiB fixture is the real measurement; the table
above proves the benchmark executes and reports a per-concurrency RSS number.
Re-run with the default fixture and add a labeled measured row when a
representative host is available.

## Probes

Probes are not JMH benchmarks. They are sysprop-controlled JUnit tests that read
a file you supply and print a comparison table; they are skipped under a normal
build and never run in CI. They live in the `io.tileverse.parquetry.probes`
package under `src/test` (with their heavier read dependencies at test scope),
separate from the `benchmarks` package and never in the shaded JMH jar.

| Class | Measures | Inputs |
|-------|----------|--------|
| `ReadComparisonProbe` | Read-path comparison of parquetry, parquet-java 1.17.0 (via `LocalInputFile`, no Hadoop filesystem), and DuckDB (in-process JDBC with `enable_profiling`) over one local file under four filter scenarios: `NO_FILTER`, `ATTRIBUTE` (`subtype='commercial'`), `SPATIAL` (exact geometry intersect with a diamond whose bbox over-selects), and `ATTRIBUTE_AND_SPATIAL`. Each engine materialises the full projection (nested columns included); parquetry uses `JtsGeometryFilter` (bbox prune + exact gate, the GeoTools path), parquet-java pushes a numeric `bbox` prefilter then re-checks JTS app-side, DuckDB uses `ST_Intersects`. Row counts must agree across engines (correctness check). Prints rows, end-to-end wall, JVM heap allocated during the run, JVM peak heap, and DuckDB's self-reported scan latency and peak buffer memory. | `-Dparquetry.probe.file` (required), `.subtype`, `.cx`/`.cy`/`.r` (query diamond), `.warmup`/`.measure`, `.engines` (comma-separated subset), `.scenarios` (comma-separated subset, e.g. to skip the memory-hungry `NO_FILTER` under a tight heap) |
| `ColumnarReadComparisonProbe` | Columnar sibling of `ReadComparisonProbe`: compares only the columnar APIs to isolate raw decode throughput and memory, full scan only. parquetry reads `ParquetSource.readBatches` (vectorized typed `ColumnVector`s); parquet-java reads each leaf `ColumnDescriptor` through `ColumnReadStoreImpl` + `ColumnReader` (column-major, no `Group`/record assembly, with a no-op `DummyRecordConverter`). Both touch every leaf value, recursing nested vectors down to their primitive and binary leaves. DuckDB is skipped (no columnar JDBC equivalent). Row counts must agree across engines. Prints rows, wall, JVM heap allocated during the run, JVM peak heap. | `-Dparquetry.probe.file` (required), `.warmup`/`.measure`, `.engines` (`parquetry,parquet-java`; `duckdb` ignored) |

Run it (give the test forks enough heap; a full unfiltered nested scan is memory-hungry):

```bash
./mvnw -pl :parquetry-benchmarks -am test -Dtest=ReadComparisonProbe \
  -Dparquetry.probe.file=/path/to/buildings.parquet \
  -DextraArgLine="-Xmx4g"
```

Caveats baked into the table: `wall` is the consumer-side cost -- every row
materialised and every requested column read out (`ResultSet.getObject` per
column for DuckDB, exactly what a row-oriented consumer such as a GeoTools
datastore over DuckDB would pay). DuckDB's `duckScan` is its internal engine
scan from the profiler, shown for context only: a JDBC consumer still pays the
full `wall` to pull rows out, and cannot obtain results at the `duckScan` rate.
`alloc` is heap allocated by all threads during the run (`-Xmx`-independent
churn); for DuckDB it is the JDBC consumer's per-row boxing, while `duckMem` is
DuckDB's native buffer pool. Peak heap reflects occupancy with uncollected
garbage at a high `-Xmx`, and the decisive memory signal is whether a scenario
completes at a pod-sized heap (`-Xmx2g`); the DuckDB spatial scenarios need its
`spatial` extension and skip cleanly when it cannot be installed.

`ColumnarReadComparisonProbe` runs the same way (it shares the `parquetry.probe.*`
properties), full scan only:

```bash
./mvnw -pl :parquetry-benchmarks -am test -Dtest=ColumnarReadComparisonProbe \
  -Dparquetry.probe.file=/path/to/buildings.parquet \
  -Dparquetry.probe.engines=parquetry,parquet-java \
  -DextraArgLine="-Xmx4g"
```

## Adding or changing a benchmark

Keep this README's benchmark table in step with the code; a benchmark that is
not listed here is one a teammate will not know exists.

- Generate fixtures in a `@Setup` method, not in the timed `@Benchmark`. Write
  synthetic files with `ParquetFileWriter` and read them through a
  `RangeReader` from `StorageFactory`, mirroring `PagePruningBenchmark`.
- Do not repeat the preview / native-access flags in `@Fork(jvmArgsAppend = ...)`.
  The launching JVM must pass them (the jar's classes are preview-compiled), and
  JMH inherits the launching JVM's arguments into each fork, hence the fork already
  has them; repeating them only doubles the reported VM options.
- Return a value from each `@Benchmark` (or use a `Blackhole`) to keep the JIT
  from eliminating the work being measured.
