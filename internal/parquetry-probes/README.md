# parquetry-probes

Internal, non-published probes for read-path characterization. Unlike
`parquetry-benchmarks` (JMH, steady-state CPU), probes measure wall-clock
behavior of whole read pipelines, usually against remote or latency-injected
storage.

## Build

```bash
MAVEN_OPTS="--sun-misc-unsafe-memory-access=allow" \
  ./mvnw -Pprobes -pl :parquetry-probes -am package -DskipTests
```

The shaded runner lands at `internal/parquetry-probes/target/probes.jar`.

## Run pattern

Every probe is a `main` class driven by `-Dprobe.*` / `-Dparquetry.probe.*`
system properties and needs the repo's preview/native flags:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED -Dfile.encoding=UTF-8 \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-exports=java.net.http/jdk.internal.net.http=ALL-UNNAMED \
  <-D properties> -cp internal/parquetry-probes/target/probes.jar <MainClass>
```

(The `--add-exports` line lets the storage layer shut down its per-file HTTP
client cleanly; without it every close logs a reflective-access stack trace
and inflates close-path timings.)

## Probes

| main class | measures | key properties |
|---|---|---|
| `ProbeMain` | single-file read scenarios (engines, decode-ahead, prefetch) | `parquetry.probe.{url,concurrency,warmup,measure,decodeAhead,prefetchDepth,httpCache,blockAligned,...}` |
| `MultiFileReadProbe` | N-file remote reads: per-file baseline vs the multi-file fan-out | `probe.{baseUrl,fileCount,pipeline,mode,maxConcurrentFiles}` |

### MultiFileReadProbe

Reads `<baseUrl>/f000.parquet` .. `f{N-1}.parquet`. Two pipelines:
`pipeline=perFile` (N independent single-file reads, the client-side
baseline; rejects `probe.maxConcurrentFiles`) and `pipeline=fileset` (one
`ParquetSource` over all N files, fan-out width = `probe.maxConcurrentFiles`,
phase-decomposed output: build/open/read/close). `mode=count` is footer-only;
`mode=drain` reads every column of every batch. `fileset` at `K=1` vs
`perFile` is the merge's no-overhead regression check.

Latency-injected fixture recipe:

```bash
mkdir fixture && for i in $(seq -w 0 63); do cp small.parquet fixture/f0$i.parquet; done
httpserv.sh -d fixture -p 18080 --latency 50ms -s
java ... -Dprobe.baseUrl=http://localhost:18080 -Dprobe.fileCount=64 \
  -Dprobe.pipeline=fileset -Dprobe.mode=drain -Dprobe.maxConcurrentFiles=8 \
  -cp internal/parquetry-probes/target/probes.jar \
  io.tileverse.parquetry.probes.MultiFileReadProbe
```
