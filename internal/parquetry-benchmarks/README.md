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
| `SpatialPruningBenchmark` | Row-group pruning benefit of spatially clustered GeoParquet data. A parquetry-written file holds native per-row-group geometry bounding boxes; a `bboxIntersects` query skips row groups whose bbox is disjoint from the query only when the data is spatially clustered. `CLUSTERED` writes each row group into a distinct grid tile -- the query hits two of sixteen row groups; `SHUFFLED` randomises row order, making every row group's bbox span the full extent and forcing record-level WKB evaluation on every row. The `geometrySize` axis adds `LARGE` (2000-vertex dense ring) to expose how WKB-walk cost scales with vertex count on the unclusterd arm. | `layout` (`CLUSTERED` one row group per grid tile; `SHUFFLED` every bbox spans the full extent), `geometrySize` (`SMALL` 5-point rectangle; `LARGE` 2000-vertex ring) |
| `SpatialGateBenchmark` | Cost of the exact geometry gate (`GeometryFilter`) in isolation, with no geometry engine. All rows are inside the query bbox, making the pruning tier a no-op; the gate alone controls which rows materialise. `gate=off` reads with a plain bbox predicate -- every candidate row decodes all output columns. `gate=on` applies the synthetic `GeometryFilter` -- rows the gate rejects never decode their wide output columns. The `passRate` axis (HIGH ~90 %, LOW ~10 %) sets how many rows pass; `width` (4 or 16 extra INT32 columns) scales per-row materialisation cost; `geometrySize` scales the WKB decode in the gate itself. | `gate` (`off`/`on`), `passRate` (`HIGH`/`LOW`), `width` (`4`/`16`), `geometrySize` (`SMALL`/`LARGE`) |
| `WkbReadBenchmark` | Head-to-head decode cost of two WKB-to-JTS readers over one representative geometry per (shape, size): `JTS_PACKED` is JTS's own `WKBReader` over a packed-coordinate `GeometryFactory` (the production-realistic baseline, fed a `byte[]`); `CUSTOM` is `MemorySegmentWkbReader`, which reads straight from a read-only `MemorySegment` onto packed sequences with one bulk copy per ring. The time gap is the headline (the custom reader is roughly an order of magnitude faster on dense geometries); per-op allocation is near-parity on LARGE because the packed `double[]` backing both readers create dominates. Run with `-prof gc` to read `gc.alloc.rate.norm`. | `shape` (`POINT`/`LINESTRING`/`POLYGON`/`MULTIPOLYGON`), `geometrySize` (`SMALL` ~12 vertices; `LARGE` ~2000), `reader` (`JTS_PACKED`/`CUSTOM`) |
| `WkbEnvelopeBenchmark` | Per-geometry cost of the WKB envelope paths a read pays on every row, over a batch-shaped fixture (one backing segment holding 1,024 building-like WKB values addressed per row by offset and length, the shape in which a decoded page reaches the decimation gate and the bbox predicate). `compute` is the decimation gate's full 2D envelope; `computeThroughBatchedCaller` is the same envelope at the gate's own call depth, through the `BinaryView` handed to it by a vector, and guards the inlining budget on which the walk depends; `matchesDecidedAtFirstVertex` is a whole-world `BboxIntersects`, the bbox predicate's production shape, decided at the first vertex and therefore the fixed per-call cost; `matchesFullWalk` is a `BboxCoveredBy` with every geometry inside the box, the relation that walks every vertex; `read` is `MemorySegmentWkbReader`, the materializer's path, which must not regress. The defaults pin the buildings point; pass the other values with `-p`. The reported time is per geometry. | `vertices` (`12`; also `5`/`40`/`200`), `shape` (`POLYGON`; also `MULTIPOLYGON`), `byteOrder` (`LE`; also `BE`), `memory` (`NATIVE`; also `HEAP`) |
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

## WKB envelope baseline

The reference point for `WkbEnvelopeBenchmark`: ns per geometry (lower is faster), one fork, 3 x 2 s warmup and
5 x 2 s measurement, on a dev host (Temurin 25.0.2). Re-measure on the target host before drawing conclusions. The
"before" column is the envelope walking coordinates through layouts held in mutable fields (the generic VarHandle
path on every ordinate); the "after" column is the shared WKB cursor, whose ordinate accessors read the segment
through a constant layout, called by a flat walk from a caller that hands over the value's window in the batch's
backing segment. A change to the envelope or the reader is compared against the "after" column.

| method | fixture | before (ns/geometry) | after (ns/geometry) |
|--------|---------|----------------------|---------------------|
| `compute` | 12 vertices, polygon, LE, native | 176.6 | 15.8 |
| `compute` | 12 vertices, polygon, BE, native | 181.0 | 18.8 |
| `compute` | 12 vertices, polygon, LE, heap | 181.1 | 16.3 |
| `compute` | 200 vertices, polygon, LE, native | 2326.3 | 252.9 |
| `computeThroughBatchedCaller` | 12 vertices, polygon, LE, native | n/a | 16.4 |
| `computeThroughBatchedCaller` | 200 vertices, polygon, LE, native | n/a | 318.2 |
| `matchesDecidedAtFirstVertex` | 12 vertices, polygon, LE, native | 37.8 | 3.7 |
| `matchesFullWalk` | 12 vertices, polygon, LE, native | 160.7 | 16.6 |
| `read` | 12 vertices, polygon, LE, native | 16.3 | 17.8 |
| `read` | 200 vertices, polygon, LE, native | 130.2 | 129.3 |

Three "after" rows are too imprecise to compare against closely: `matchesDecidedAtFirstVertex` at the pinned point
(3.670 +/- 0.582, 16 percent), which is expected of a benchmark whose whole cost is one call's fixed overhead, and
both `read` rows (17.801 +/- 1.588 at 12 vertices, 8.9 percent; 129.308 +/- 7.553 at 200 vertices, 5.8 percent).
The two 200-vertex rows of `compute` and `computeThroughBatchedCaller` belong to the wider run-to-run spread of the
paragraph below. Within one run `computeThroughBatchedCaller` measured 318.2 +/- 30.7, 9.7 percent. The remaining
five rows hold within five percent, 4.8 percent at worst. Two "before" rows are looser still:
`matchesDecidedAtFirstVertex` measured 37.8 +/- 33.2 and `compute` at 200 vertices 2326.3 +/- 424.9, an 18 percent
spread.

The 200-vertex fixture also moves more between runs than within one. Over three runs of one session `compute`
measured 252.9, 312.6 and 286.3, and `computeThroughBatchedCaller` 318.2 and 239.9, against within-run errors of
3 to 10 percent. On that fixture only a move of more than about 20 percent means anything, and the two arms are
indistinguishable there; the pinned point is the row to compare against.

`read` at 12 vertices is the one row that moved the wrong way, 16.301 +/- 0.337 before against 17.801 +/- 1.588
after. The intervals overlap, which leaves the difference unestablished by this run; an earlier A/B put it at about
1 ns and attributed it to the JTS reader sharing the cursor with the envelope rather than owning a private one.
It stays parked and unresolved. At 200 vertices the reader is unchanged.

The inlining depth of the walk is what separates the two columns as much as the constant layouts do. The coordinate
read ends in the JDK's layout access, whose innermost alignment check compiles to a plain load only when the JIT
inlines it; one extra call frame anywhere between the caller's loop and the ordinate accessor pushes that check past
the depth limit and turns every ordinate into a real call, which measured 59 ns instead of 16 at the pinned point.
`computeThroughBatchedCaller` is the guard on that: it reads every row through the same `BinaryView` method
reference held by the decimation gate, two frames below the row loop, which is the gate's own call depth. A score
near `compute`'s means the gate is on the fast path (16.4 against 16.2 in the same run); a score several times
higher means a frame was added between the row loop and the ordinate accessor.

### Access-style exploration (measured once on 2026-09-14, walkers not kept)

Three throwaway walkers folded the same rings through different coordinate access, to bound what the mutable-layout
walk costs. At the pinned point `compute` took 176.6 ns through the old mutable-layout cursor, 18.4 ns reading the
segment directly through constant layouts, 22.4 ns bulk-copying each run into a reusable scratch array, and 37.8 ns
through a `ByteBuffer` view; at 200 vertices 2326.3 / 382.8 / 376.9 / 435.3. The shipped cursor reads directly
through constant layouts, which the big-endian arm decides: the direct read holds at 20.8 ns while the bulk copy
jumps to 63.4 ns, because a copy through a byte-swapping layout cannot take the bulk path. On a heap-backed segment
both match their native counterparts (19.8 and 23.1 ns). Whether the bulk copy is still a lever for large geometries
is not settled by these numbers: the shipped walk runs 200-vertex polygons in 252.9 ns against the exploration folds'
382.8 and 376.9, but those walkers ran one frame deeper, through an `EnvelopeWalker` field, and one frame is worth up
to 4x on this path. Read the comparison as indicative only.

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
