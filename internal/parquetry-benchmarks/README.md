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
Memory API. Both the launching JVM and the JVMs that JMH forks therefore need
the preview and native-access flags. The forked-JVM flags are baked into each
benchmark (`@Fork(jvmArgsAppend = ...)`); the launching JVM needs them on the
command line:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED \
  -jar internal/parquetry-benchmarks/target/benchmarks.jar
```

Pass a regular expression to run a subset, and standard JMH options to tune the
run:

```bash
# one benchmark class
java --enable-preview --enable-native-access=ALL-UNNAMED \
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

## Benchmarks

| Class | Measures | Key parameters |
|-------|----------|----------------|
| `PagePruningBenchmark` | What column-index page pruning buys a filtered read: writes one row group with many small pages, then reads a 1%-selective band predicate with the COLUMN_INDEX tier on vs. off. | `layout` (`SORTED` clusters matches into a few pages; `SHUFFLED` scatters them across every page, leaving nothing to prune), `useColumnIndex` (`true`/`false`) |
| `FilterTierBenchmark` | How far each metadata tier reduces a selective point lookup over a ten-row-group file. The result shows page pruning (COLUMN_INDEX) dominating for point lookups while row-group pruning (STATS/BLOOM) helps little. | `tiers` (`RECORD_ONLY`, `STATS`, `COLUMN_INDEX`, `BLOOM`, `ALL`) |

## Adding or changing a benchmark

Keep this README's benchmark table in step with the code; a benchmark that is
not listed here is one a teammate will not know exists.

- Generate fixtures in a `@Setup` method, not in the timed `@Benchmark`. Write
  synthetic files with `ParquetWriter` and read them through a
  `RangeReader` from `StorageFactory`, mirroring `PagePruningBenchmark`.
- Put `@Fork(jvmArgsAppend = {"--enable-preview", "--enable-native-access=ALL-UNNAMED"})`
  on every benchmark: the forked JVMs load preview-compiled classes and fail
  without it.
- Return a value from each `@Benchmark` (or use a `Blackhole`) to keep the JIT
  from eliminating the work being measured.
