# Why pure-JVM GeoParquet access: off-heap memory and pod safety

This document is about one place a GeoParquet reader runs: a long-lived,
multi-tenant server pod that serves features and tiles under concurrency, its CPU and
memory assigned by an orchestrator rather than fixed by hand. GeoServer is the
canonical such server and **GeoServer Cloud** its elastic, dynamically-scheduled form.
A *datastore* is the component a server uses to read one data format; a GeoParquet
datastore reads GeoParquet. Whether a reader is safe in that pod comes down to how it
bounds memory - a concern specific to this deployment, not to every use of the
library.

A GeoParquet datastore backed by a native engine - DuckDB reached through JDBC, an
Arrow stream, or a JNI bridge - holds most of its working memory **off-heap**,
where the JVM heap limit (`-Xmx`) cannot see it and the container memory limit
(`-m`, a cgroup) enforces it with a **SIGKILL**.

That memory is not unbounded: a native engine like DuckDB exposes its own limits
(a buffer-pool `memory_limit`, a `threads` cap), and within a single engine those
limits work - the pool is capped, shared across that engine's concurrent queries,
and an overflow becomes a catchable error or a spill to disk rather than a kill.
The difficulty is that the bound is **per engine instance**. A server hosts many
instances, nothing sums them to the one limit that actually kills you - the
container's total - and the per-instance limits must be assigned by hand. That
hand-assignment is workable for a fixed deployment whose resources never change,
and it falls apart for a pod whose resources are assigned dynamically.

A pure-JVM reader sidesteps the whole exercise: it bounds its entire working set
from the single container limit it reads at startup, with no per-store tuning, and
the same image fits whatever the scheduler hands it. See
[Memory and tuning](memory-and-tuning.md) for the budgets parquetry bounds itself
with, and [Native memory and spill](native-memory-and-spill.md) for how it bounds
its own small, explicit off-heap term.

---

## 1. The memory that kills is off-heap, and the container kills with a signal

Three facts compose into the failure:

- **The working memory is off-heap.** A native engine's buffer pool and any Arrow
  buffers it exports live in native memory, outside the Java heap. `-Xmx` caps only
  the heap and never sees this memory.
- **The container limit counts everything.** The cgroup `-m` limit counts heap
  *plus* off-heap *plus* metaspace, threads, and the native engine's pool. When the
  sum crosses the limit, the kernel OOM-killer fires.
- **A container OOM-kill is a SIGKILL, not an `OutOfMemoryError`.** There is no
  `catch`, no `finally`, no shutdown hook, no chance to return a 503 and shed load.
  The process is gone between one instruction and the next.

The read-comparison probe shows the off-heap term directly. A single **unconfigured**
DuckDB scan of a 619 MB GeoParquet file reports ~1.3 GB of off-heap buffer memory
against ~40 MB of JVM heap:

```
engine            wall(ms)    alloc(MB) peakHeap(MB)  duckMem(MB)
duckdb              3904.4       2021.6        267.4       1231.2
```

`duckMem` is off-heap and invisible to `-Xmx`.

---

## 2. You can bound it - but per engine, not per pod

DuckDB lets you cap that pool: a per-store `memory_limit` (e.g. `1GB`) and a
`threads` cap, applied to each datastore's engine. This genuinely fixes the
single-store case. The buffer pool stops at the limit, concurrent queries on that
store share it rather than each paying their own copy, and when the limit is reached
DuckDB spills or raises a catchable out-of-memory error through JDBC. One bounded
store fails the way a pure-JVM reader does: one failed request, not a dead pod.

What no per-store knob bounds is the **sum**. Each configured GeoParquet store is its
own engine instance with its own `memory_limit` and `threads`, unaware of the others.
The quantity that crosses the cgroup limit and triggers the SIGKILL is the total
across every store plus the heap plus the rest of the JVM - and there is no setting
for that total. Safety is therefore a partition the operator computes and maintains
by hand:

```
Σ memory_limit(store_i)  +  JVM heap  +  JVM off-heap  +  spill headroom  <=  container memory
Σ threads(store_i)                                                        <=  available cores
```

---

## 3. A static partition does not match a dynamic pod

Every property of that partition is hostile to an elastic deployment:

- **Static split, dynamic load.** Each slice is fixed at configuration time, but
  which store is hot shifts request to request. To be safe each slice must cover its
  store's peak, and the peaks sum - yet they rarely peak together. The operator is
  left to either over-provision every store (wasting most of the pod) or
  under-provision and take a SIGKILL the moment two scans coincide.
- **Hand-maintained and incremental.** Stores are added one at a time by admins
  configuring a single store in isolation. Add the sixth GeoParquet layer and the
  budget is silently blown unless all five existing slices are recomputed. Nothing
  in the configuration flow recomputes the global ceiling.
- **The default is unsafe many times over.** Unset, each engine defaults to a large
  fraction of detected RAM, sized as if it owned the machine. N stores each claiming
  most of the box overflows a shared container immediately.

A fixed-size deployment can live with this. If the pod always has the same CPU and
memory and an operator hand-tunes every limit once, the partition holds - at the cost
of the over-provisioning and the manual upkeep above. That is a real and valid way to
run it.

It breaks down precisely when the pod's resources are **assigned dynamically** -
autoscaled, sized differently across a fleet, or scheduled elastically, as on
**GeoServer Cloud**. The per-store limits would have to be recomputed for every
pod shape, and they are not: the store configuration is fixed while the ceiling moves
under it. A split tuned for a 4 GB pod SIGKILLs a 2 GB one and wastes a 16 GB one. The
property you want - one image that is safe on whatever the scheduler assigns it -
is exactly what a static per-store partition cannot provide.

---

## 4. The pure-JVM alternative bounds from the one number that moves

A pure-JVM reader shares a single budget across every store and every request,
because the JVM heap is global: one `-Xmx` bounds all of them at once, with no
per-store split to compute. parquetry extends that regime to its one small off-heap
term, deriving its budgets (fetch, decode, the segment pool) from a fraction of the
container's memory limit read at startup. The budget is process-wide and fungible:
add a twentieth GeoParquet layer and there is nothing to tune; give the pod a
different size and the budgets re-derive from the new limit with no configuration
change.

Overload then manifests as one of:

- a **catchable** `OutOfMemoryError` on the request that overflowed - logged, turned
  into an error response, the pod still serving every other request;
- **budget backpressure** - the read parks or spills rather than allocating without
  bound, staying under the limit by design (see [Memory and
  tuning](memory-and-tuning.md));

never a pod-wide SIGKILL. The failure domain is one request, not the process.

This is a pure-JVM-vs-native-engine point, not a parquetry-vs-parquet-java one: any
reader that decodes in the JVM shares the global-heap budget and the graceful-failure
property. parquetry's contribution is to bring its off-heap fetch term under the same
auto-sized, container-derived ceiling, keeping the off-heap from quietly
reintroducing the per-instance problem this document is about.

---

## 5. Reproduce it yourself

The probe and its container runner make both cases repeatable under real cgroup CPU
and memory limits:

```
# the unconfigured default - duckMem (off-heap) dwarfs peakHeap:
./internal/parquetry-probes/run-probe-docker.sh \
    --probe columnar --engines duckdb,parquetry,parquet-java --concurrency 1 -s

# raise concurrency until an unbounded DuckDB store SIGKILLs the container:
./internal/parquetry-probes/run-probe-docker.sh \
    --probe columnar --engines duckdb --cores 4 --memory 4g --heap 2g --concurrency 4 -s
```

The second run holds roughly `4 x 1.3 GB` off-heap past the 4 GB container limit and
the kernel kills the JVM with SIGKILL (exit code 137 = `128 + 9`); no table prints,
because there is no code path left to print it. Setting a `memory_limit` on the store
avoids that single-store kill - and leaves the multi-store, dynamic-pod partition of
section 3 as the part no per-store knob resolves.

`run-probe-docker.sh --help` documents the limits; the probe pins real cores
(`--cpuset-cpus`) and a real container memory limit (`-m`), making the kill the one a
pod would actually take, not a simulation.

---

*Scope: why a native-engine reader's off-heap memory is safe in a bounded, concurrent
pod only under a static, hand-assigned, per-store partition - and why a pure-JVM
reader, bounded from the single container limit it reads at startup, needs no such
partition and fits a dynamically sized pod unchanged. How parquetry sizes and bounds
its own two memory terms is in [memory-and-tuning.md](memory-and-tuning.md) and
[native-memory-and-spill.md](native-memory-and-spill.md).*
