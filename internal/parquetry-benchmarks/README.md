# parquetry-benchmarks

JMH microbenchmarks for the parquetry read and write paths. Build and dev only:
this module is never published to Maven Central, and the normal `./mvnw verify`
only compiles it (it has no JUnit tests, and benchmarks never run in CI).

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
| `PagePruningBenchmark` | What column-index page pruning buys a filtered read: writes one row group with many small pages, then reads a 1%-selective band predicate with the COLUMN_INDEX tier on vs. off. | `layout` (`SORTED` clusters matches into a few pages; `SHUFFLED` scatters them across every page, leaving nothing to prune), `useColumnIndex` (`true`/`false`) |
| `FilterTierBenchmark` | How far each metadata tier reduces a selective point lookup over a ten-row-group file. The result shows page pruning (COLUMN_INDEX) dominating for point lookups while row-group pruning (STATS/BLOOM) helps little. | `tiers` (`RECORD_ONLY`, `STATS`, `COLUMN_INDEX`, `BLOOM`, `ALL`) |
| `LateMaterializationBenchmark` | Whether decoding output columns only for predicate-matching rows (late materialization) beats decoding all surviving rows up front. Uses a 16-column wide table. The win is largest on `SHUFFLED` data where column-index page pruning cannot skip pages and late materialization is the only lever; on `SORTED` data page pruning already skips non-matching pages and the two arms approach parity. | `layout` (`SORTED` clusters matches; `SHUFFLED` scatters them, defeating page pruning), `selectivity` (`POINT`, `P1`, `P10`), `lateMaterialize` (`true`/`false`) |

## Adding or changing a benchmark

Keep this README's benchmark table in step with the code; a benchmark that is
not listed here is one a teammate will not know exists.

- Generate fixtures in a `@Setup` method, not in the timed `@Benchmark`. Write
  synthetic files with `ParquetWriter` and read them through a
  `RangeReader` from `StorageFactory`, mirroring `PagePruningBenchmark`.
- Do not repeat the preview / native-access flags in `@Fork(jvmArgsAppend = ...)`.
  The launching JVM must pass them (the jar's classes are preview-compiled), and
  JMH inherits the launching JVM's arguments into each fork, hence the fork already
  has them; repeating them only doubles the reported VM options.
- Return a value from each `@Benchmark` (or use a `Blackhole`) to keep the JIT
  from eliminating the work being measured.
