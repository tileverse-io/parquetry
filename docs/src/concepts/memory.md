# Memory

!!! abstract "In short"
    Parquetry reads the memory limit of the container at startup and derives its native memory budgets from the memory left beside the heap.
    Readers share these budgets, and a pod of another size gets budgets scaled to its own limit.
    This page describes the budgets, their defaults and how to size them.

This page explains how Parquetry bounds the memory of its reads, and how these
bounds relate to a container memory limit. It is for developers and operators
running Parquetry in a long-lived server process. The settings are in
[Tune memory](../guides/tune-memory.md).

The guiding rule: **a read is bounded by budgets scaled from the memory left
beside the heap by the container, and the native buffers under those budgets
leave the process the moment they are freed.** The second half holds where the
pool maps its buffers, on Linux and macOS with native access allowed (section
3). On malloc, freed memory may stay with the process. The default target is a
server process with 1-2 GB for the reader, not "load the file." The fetch and
decode stages controlled by these budgets are described in
[The read path](read-path.md).

---

## 1. One budget for the process

A server such as GeoServer hosts many data stores in one process, and the
container holds the process to one memory limit. Parquetry derives its native
memory budgets from that limit. This section explains why, and what goes wrong
with limits set per store.

Three facts about containers combine into the failure:

- **`-Xmx` sees the heap only.** The JVM heap limit caps the Java heap. Native
  buffers, thread stacks, metaspace and the memory of native libraries sit
  outside it.
- **The container limit counts the whole process.** The cgroup memory limit
  counts the heap and the native memory together.
- **The container kills with a signal.** When the process crosses the limit,
  the kernel kills it with SIGKILL. No `OutOfMemoryError` is thrown, no
  `finally` block runs, and the requests in flight get no response.

A store backed by an embedded native engine, reached through JDBC, an Arrow
stream or a JNI bridge, keeps most of its working memory outside the heap. Such
an engine has limits of its own, such as a cap on its buffer pool and on its
threads. Within one engine instance these limits work. The concurrent queries
of the instance share the pool, and an overflow becomes a spill to disk or an
error returned to the caller, not a kill.

These limits bound one engine instance, and a server runs one instance per
store. No setting bounds the sum of the instances, although the container
counts that sum. The operator splits the container memory by hand:

```
sum of the store memory limits + JVM heap + other native memory + spill room <= container memory
sum of the store thread limits                                               <= available cores
```

A pod of a fixed size can work with this split: the operator computes it once
and keeps it current. The split breaks down when one container hosts many
stores and a scheduler assigns its memory limit:

- **The split is fixed, and the load moves.** Each store needs room for its own
  peak, although the peaks rarely coincide. The operator either reserves most
  of the pod for idle stores, or risks a kill when two large scans meet.
- **Stores are added one at a time.** A new store changes the sum, and the
  configuration of one store does not recompute the others.
- **The pod size changes.** The split must be recomputed for each pod size. A
  split computed for a 4 GB pod is too large for a 2 GB pod and wastes most of
  a 16 GB pod.

A reader decoding in the JVM avoids the split for its heap, because one `-Xmx`
bounds the stores together. Parquetry brings its native buffers under the same
rule. At startup it reads the container limit and derives its fetch and decode
budgets from the memory left beside the heap (section 2). Readers opened with
the default runtime share these budgets. Adding a store adds nothing to tune,
and a pod of another size gets budgets scaled to its own limit.

When a budget is full, a read waits for room, then spills its buffers to a
memory-mapped file ([Native memory and spill](native-memory-and-spill.md)).
When the disk budget is full too, the read fails with a
`BudgetExhaustedException`. One request fails, and the process keeps serving the
others.

---

## 2. The budgets

A read holds memory in three places, and each has its own cap.

| Budget | Bounds | Counted against | Default |
|--------|--------|-----------------|---------|
| `FetchBudget` | off-heap bytes held by column-chunk fetches, in flight and prefetched | the container limit, invisible to `-Xmx` | a fifth of the memory beside the heap |
| off-heap `DecodeBudget` | off-heap bytes held by decoded values kept in native segments | the container limit | half of the memory beside the heap |
| on-heap `DecodeBudget` | heap bytes held by a read in speculatively decoded batches | `-Xmx` (the JVM heap) | `0.25` of `Runtime.maxMemory()` |

"The memory beside the heap" is the container limit minus the maximum heap
(`ResourceLimits.availableMemoryBytes()`). The heap grows to its maximum under
load whatever its live set, and the budgets must not count that memory a second
time. With the heap at half the container, as `-XX:MaxRAMPercentage=50` sets it,
the fetch cap is a tenth of the container and the off-heap decode cap a quarter.
[Tune memory](../guides/tune-memory.md#change-the-memory-figure)
replaces the memory figure used to derive the budgets.

The budgets belong to the `ParquetRuntime`. Readers opened with the default
runtime share one set: however many concurrent reads run through it, the totals
stay under one cap each. A reader opened with its own runtime gets its own
native budgets, and their sum across runtimes is the deployer's to keep inside
the container.
[Tune memory](../guides/tune-memory.md#give-readers-a-runtime-of-their-own)
builds a runtime with budgets of its own.

A mandatory fetch buffer, the one for a read's current row group, gets a
native segment while the fetch budget has room. When it has none, the fetch
waits up to `mandatoryBufferWait` (1 s by default) for other reads to release
room, in first-come order. The read holds no other fetch buffer while it waits,
and that is what keeps the wait from deadlocking. When the wait expires, the
buffer is a memory-mapped temp file (see [native memory and spill](native-memory-and-spill.md)),
and when the disk budget has no room either, the read fails with a
`BudgetExhaustedException`. A request larger than the whole budget skips the
wait and spills at once. A mandatory decode buffer never waits: it is a native
segment while the off-heap decode budget has room and a mapped file otherwise.
A speculative fetch, for the prefetch window or for decode-ahead, never waits
for fetch-budget room and never spills a fetch buffer: it yields while the
budget is full, and the row group is fetched when the consumer reaches it. The
decoded batches of a row group decoding ahead follow the decode budget's own
rule: a batch over that budget spills to disk, or waits for room when spilling
is off. The counters behind this are `FetchBudget.waitStats()` and
`DiskBudget.stats()`.

### The caches

Beside the budgets of the reads, the process keeps cached data on the heap, in
the two pools of the shared tileverse cache manager. The tileverse guide
[Configure for performance](https://tileverse.io/storage/how-to/configure/#caching-layers)
describes the pools, their settings and how the caches key the data of a file.

Parquetry keeps two caches in the metadata pool: the parsed footers
(`parquetry-footer-metadata`) and the page indexes learned from page headers
(`parquetry-learned-pages`, see [The read path](read-path.md#without-a-page-index)).
A learned page index weighs about 18 bytes a page, plus the bounds of its pages
and a fixed 288 bytes. An entry unused for ten minutes expires. An open over an
unnamed source never reaches the cache.

The byte-range cache keeps the ranges read from remote storage in the data pool.
The GeoTools stores of Parquetry (Parquet, STAC GeoParquet and Iceberg) turn it
on by default. `par`
and `FileSource.open` keep it off. Local files have no byte-range cache.

The metadata of a file is keyed by its name, with the version of the object when
the reader knows one, and by its length. Files opened through `FileSource` have
a version, from the listing or the stat lookup that found them. STAC assets, the
data files of an Iceberg table and some `par` commands open their files by key
alone. Without a version, a file rewritten under the same name at the same
length is served its old entries until they expire. Iceberg never rewrites a
data file in place.

The learned cache counts two misses and one load per learned chunk: its hit rate
reads low by design. [Tune memory](../guides/tune-memory.md#size-the-caches)
sizes the pools, turns the byte-range cache off and clears the caches.

---

## 3. Where the native memory comes from

Off-heap growth is what drives container OOM-kills: it counts against the pod's
memory limit but is invisible to `-Xmx`, and the kernel kills the process before
any `OutOfMemoryError` is thrown. Two things keep Parquetry's native footprint
equal to what it holds:

- The fetch buffers, the decompressed pages and the native decode buffers all
  come from one `SegmentPool`. The pool retains returned buffers under byte caps
  sized to the working set of a loaded server (a fifth of the memory beside the
  heap for the large class, a tenth for the small one) and frees what sits idle
  for a minute. A borrow is never refused. The pool's ceiling, equal to what the
  two native budgets allow in flight, limits retention instead: a returned
  buffer is kept only while borrowed plus retained bytes stay within it, hence
  the pool's footprint never exceeds the larger of the ceiling and the bytes in
  flight. Retention fills what the budgets leave idle instead of adding to them.
- A pool buffer of 64 KiB or more is an anonymous memory mapping, not a malloc
  allocation, on Linux and macOS. Memory freed to glibc's malloc does not leave
  the process: the freed chunk stays parked in the arena of the thread that
  allocated it, and a server with many threads keeps up to eight arenas per host
  CPU, each with its own high-water mark. Under concurrent reads, the memory
  kept that way can exceed the memory in use. A mapping is unmapped when its
  buffer is freed, and the pages go back to the kernel at once.

The mappings need native access
([Allow native access](../guides/tune-memory.md#allow-native-access)).

---

## 4. Sizing against a container limit

Peak process memory is approximately:

```
maxHeap (-Xmx; the heap grows to it under load)
  + the pool's memory, in flight and retained (at most FetchBudget + off-heap DecodeBudget)
  + decompressed pages (one per projected column of each row group being decoded)
  + JVM native baseline (metaspace, code cache, GC structures, thread stacks)
```

The pool's ceiling is the two budgets' total, and what the pool retains between
bursts counts inside it, not on top. The decompressed pages are borrowed from
the pool without a budget reservation: a read decoding `n` row groups ahead
holds up to `n` pages per projected column, each the file's page size, usually
about a megabyte. With `-XX:MaxRAMPercentage=50` the first term is half the
container and the pool takes at most 35% of it before fetches queue for room and
then spill to disk. About 15% is left for the pages, the JVM baseline and the
page cache.

The on-heap `DecodeBudget` and [the caches](#the-caches) are inside `-Xmx`, and
add nothing to the formula. The budgets bound speculative work and native
buffers, not the heap of the mandatory current row group: a read unable to fit
the working set of one row group inside `-Xmx` fails with an
`OutOfMemoryError`. [Size the container](../guides/tune-memory.md#size-the-container)
lists the sizing rules and the JVM settings beside Parquetry's own.

---

## 5. Where to look

Paths are relative to the `io/tileverse/parquetry` package of its module. The
`io/` files are in `parquetry-io`, the `geotools/` file in `parquetry-geotools`,
and the others in `parquetry-core`.

| Concern | Start in |
|---------|----------|
| Budgets and their defaults | `runtime/ParquetRuntime.java`, `io/limits/IoLimits.java` |
| Off-heap fetch budget | `runtime/FetchBudget.java` |
| Decode budgets | `runtime/DecodeBudget.java` |
| The pool and its memory | `io/SegmentPool.java`, `io/DefaultSegmentPool.java`, `io/MappedMemory.java` |
| Decode-ahead + promotion | `internal/read/ParallelDecodeCoordinator.java`, `internal/read/StreamingBatchSource.java` |
| Prefetch | `internal/read/RowGroupPrefetcher.java` |
| Metadata cache | `data/FooterMetadataCache.java`, `data/ParquetCaches.java` |
| Byte-range cache default of the GeoTools stores | `geotools/data/StorageParams.java` |

---

*Scope: bounding the memory of a single-file read. The general read path is in
[The read path](read-path.md).*
