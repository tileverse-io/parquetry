# Why pure-JVM GeoParquet access: off-heap memory and pod safety

A GeoParquet datastore backed by a native engine - DuckDB reached through JDBC, an
Arrow stream, or a JNI bridge - holds most of its working memory **off-heap**,
where the JVM heap limit (`-Xmx`) cannot bound it and the container memory limit
(`-m`, a cgroup) enforces it with a **SIGKILL**. Under the concurrency a server
pod actually sees, that off-heap memory can hard-kill the process with no warning,
no stack trace, and no chance to recover. A pure-JVM reader keeps the working set
on-heap and `-Xmx`-bounded, where the same overload degrades gracefully - a
catchable `OutOfMemoryError` or budgeted backpressure, one failed request - rather
than killing the pod.

This is the production-safety reason to read GeoParquet in pure Java. It is the
reason to prefer a pure-JVM datastore over one backed by an embedded native engine
for a long-lived, multi-tenant pod (a GeoServer instance, say). It is **not** an
argument for parquetry over another pure-JVM reader such as parquet-java; both
share the property described here. See [Memory and tuning](memory-and-tuning.md)
for the budgets parquetry bounds itself with, and [Native memory and
spill](native-memory-and-spill.md) for how it bounds its own (small, explicit)
off-heap term.

---

## 1. The failure, reproduced

The read-comparison probe demonstrates it directly. A single DuckDB scan of a
619 MB GeoParquet file reports ~1.3 GB of **off-heap** buffer memory (its internal
buffer pool), against ~40 MB of JVM heap:

```
engine            wall(ms)    alloc(MB) peakHeap(MB)  duckMem(MB)
duckdb              3904.4       2021.6        267.4       1231.2
```

`duckMem` is off-heap and invisible to `-Xmx`. Now serve four of those scans at
once on a pod sized like production - four CPUs, a 2 GB heap, a 4 GB container:

```
./internal/parquetry-probes/run-probe-docker.sh \
    --probe columnar --engines duckdb \
    --cores 4 --memory 4g --heap 2g --concurrency 4 -s

!! OOM-KILLED (exit 137): the run exceeded the 4g container memory limit and the
   kernel killed the JVM with SIGKILL before it could print a table.
```

Four concurrent scans hold roughly `4 x 1.3 GB` off-heap, past the 4 GB container
limit, and the kernel kills the JVM. Exit code 137 is `128 + 9` (SIGKILL). No
table prints, because there is no code path left to print it.

---

## 2. Why `-Xmx` cannot protect you

Three facts compose into the kill:

- **The memory is off-heap.** A native engine's buffer pool and the Arrow buffers
  it exports live in native memory, outside the Java heap. `-Xmx` caps only the
  heap and never sees or limits this memory.
- **The container limit counts everything.** The cgroup `-m` limit counts heap
  *plus* off-heap *plus* metaspace, threads, and the native engine's pool. When
  the sum crosses the limit, the kernel OOM-killer fires.
- **A container OOM-kill is a SIGKILL, not an `OutOfMemoryError`.** There is no
  `catch`, no `finally`, no shutdown hook, no chance to return a 503 and shed
  load. The process is gone between one instruction and the next.

This is the same off-heap term [Native memory and spill](native-memory-and-spill.md)
describes for parquetry's own fetch buffers - the difference is that parquetry
bounds and spills its term against the container limit, while an embedded native
engine's pool is sized by the engine for throughput, not by your pod's ceiling.

---

## 3. Concurrency is what turns a fit into a kill

The single-scan footprint that fits at concurrency 1 is not the number that
matters. A server admits many requests at once - a GeoServer pod under the
control-flow extension admits about `2 x cores` - and an embedded engine pays its
off-heap buffer pool **per concurrent query**. Peak off-heap is therefore roughly:

```
concurrency  x  per-query off-heap footprint
```

So the workload that runs comfortably in a desktop notebook at concurrency 1 is
the workload that SIGKILLs the pod at concurrency 4-8. The probe's `--concurrency`
axis walks exactly this transition: raise it until exit 137 appears. The heap
limit you so carefully set is irrelevant to where that line falls.

---

## 4. The pure-JVM alternative fails gracefully

A pure-JVM reader holds its working set on the heap, or in an explicitly budgeted
and reclaimable off-heap term (see [Native memory and
spill](native-memory-and-spill.md)). Overload then manifests as one of:

- a **catchable** `OutOfMemoryError` on the request that overflowed - logged,
  turned into an error response, the pod still serving every other request;
- **budget backpressure** - the read parks or spills rather than allocating without
  bound, staying under the limit by design (see [Memory and
  tuning](memory-and-tuning.md));

never a pod-wide SIGKILL. The failure domain is one request, not the process.

That is the whole argument: with a pure-JVM reader you choose a heap limit and the
runtime keeps you under it; with an embedded native engine the dangerous memory is
the memory your heap limit cannot see, and the enforcement mechanism is a kill,
not an exception.

To be precise about scope: parquetry and parquet-java both have this property
because both decode in the JVM. parquetry additionally bounds its own off-heap
fetch term and auto-sizes both budgets from a fraction of the container limit
([Memory and tuning](memory-and-tuning.md)); that is a parquetry-vs-parquet-java
detail, separate from the pure-JVM-vs-native-engine point of this document.

---

## 5. Reproduce it yourself

The probe and its container runner make the kill repeatable under real cgroup CPU
and memory limits:

```
# fits at concurrency 1 - note duckMem (off-heap) dwarfs peakHeap:
./internal/parquetry-probes/run-probe-docker.sh \
    --probe columnar --engines duckdb,parquetry,parquet-java --concurrency 1 -s

# raise concurrency until DuckDB SIGKILLs the container (-m it cannot see past):
./internal/parquetry-probes/run-probe-docker.sh \
    --probe columnar --engines duckdb --cores 4 --memory 4g --heap 2g --concurrency 4 -s
```

`run-probe-docker.sh --help` documents the limits; the probe pins real cores
(`--cpuset-cpus`) and a real container memory limit (`-m`), making the kill the one
a pod would actually take, not a simulation.

---

*Scope: why off-heap memory makes an embedded-native-engine reader unsafe for a
bounded, concurrent pod, and why a pure-JVM reader is not. How parquetry sizes and
bounds its own two memory terms is in [memory-and-tuning.md](memory-and-tuning.md)
and [native-memory-and-spill.md](native-memory-and-spill.md).*
