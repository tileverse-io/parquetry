# Memory and tuning in parquetry

How a read bounds its memory, the dials that bound it, and how to size them
against a container limit. Read [The parquetry read path](read-path.md) first for
the fetch and decode stages these dials act on.

The guiding rule: **a read is bounded by budgets scaled from the memory left
beside the heap by the container, and the native buffers under those budgets
leave the process the moment they are freed.** The second half holds where the
pool maps its buffers, on Linux and macOS with native access allowed (section
2); on malloc, freed memory may stay with the process. The default target is a
server process with 1-2 GB for the reader, not "load the file."

---

## 1. The budgets

A read holds memory in three places, and each has its own cap.

| Budget | Bounds | Counted against | Default |
|--------|--------|-----------------|---------|
| `FetchBudget` | off-heap bytes held by column-chunk fetches, in flight and prefetched | the container limit, invisible to `-Xmx` | a fifth of the memory beside the heap |
| off-heap `DecodeBudget` | off-heap bytes held by decoded values kept in native segments | the container limit | half of the memory beside the heap |
| on-heap `DecodeBudget` | heap bytes a read may hold in speculatively decoded batches | `-Xmx` (the JVM heap) | `0.25` of `Runtime.maxMemory()` |

"The memory beside the heap" is the container limit minus the maximum heap
(`ResourceLimits.availableMemoryBytes()`). The heap grows to its maximum under
load whatever its live set, and the budgets must not count that memory a second
time. With the heap at half the container, as `-XX:MaxRAMPercentage=50` sets it,
the fetch cap is a tenth of the container and the off-heap decode cap a quarter.
The environment variable `PARQUETRY_IO_MAX_OFFHEAP` (or the system property
`parquetry.io.maxOffHeap`) replaces the figure the budgets derive from.

The budgets belong to the `ParquetRuntime`. Readers opened with the default
runtime share one set: however many concurrent reads run through it, the totals
stay under one cap each. A reader opened with its own runtime gets its own
native budgets, and their sum across runtimes is the deployer's to keep inside
the container. A runtime sets them explicitly:

```java
ParquetRuntime runtime = ParquetRuntime.builder()
        .fetchBudget(FetchBudget.ofBytes(256L << 20))
        .offHeapDecodeBudget(DecodeBudget.ofBytes(512L << 20))
        .decodeBudget(DecodeBudget.ofMaxMemoryFraction(0.25))
        .build();
```

A mandatory buffer, one the current row group needs to make progress, never
waits: a native segment while its budget has room, a memory-mapped temp file
when it has none (see [native memory and spill](native-memory-and-spill.md)).
When the disk budget has no room either, the read fails with an
`IllegalStateException` instead of parking. Only speculative work parks on a
full budget. That is what keeps a budget too small for a single row group from
deadlocking a read.

---

## 2. Where the native memory comes from

Off-heap growth is what drives container OOM-kills: it counts against the pod's
memory limit but is invisible to `-Xmx`, and the kernel kills the process before
any `OutOfMemoryError` is thrown. Two things keep parquetry's native footprint
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
  CPU, each with its own high-water mark. Under 32 concurrent reads the memory
  kept that way exceeded the memory in use. A mapping is unmapped when its
  buffer is freed, and the pages go back to the kernel at once.

The mappings need native access: run with `--enable-native-access=ALL-UNNAMED`.
Without it the JDK prints a warning at the first mapping, and a JVM started with
`--illegal-native-access=deny` falls back to malloc. Mapped memory does not
appear in the JVM's native memory tracking (`jcmd <pid> VM.native_memory`);
`SegmentPool.getDefault().stats()` reports what the pool retains. The system
property `parquetry.io.mappedBackings=false` keeps the buffers on malloc.

---

## 3. Sizing against a container limit

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
container and the pool takes at most 35% of it before fetches and decodes spill
to disk. About 15% is left for the pages, the JVM baseline and the page cache.
Measured with 32 concurrent bbox reads of a 3 GB file in a 2 GB container with
a 1200 MB heap: 0.77 GB of anonymous memory at the peak, against 1.34 GB with
the buffers on malloc, at a higher read rate.

- A heap set above half the container shrinks the budgets with the memory left
  beside it. A heap set close to the container limit leaves them an eighth of
  the container, and most fetches then go to the spill files: slow, not dead.
- The on-heap `DecodeBudget` is inside `-Xmx` and does not add to the formula; it
  bounds how much of the heap speculative decode-ahead may occupy.
- The heap scratch retained by parquetry's tileverse `ByteBufferPool` provider
  (the merge scratch of batched reads, a PMTiles reader's buffers) is also inside
  `-Xmx`: at most 32 MiB between borrows, released after 30 s idle.

A read that cannot fit one row group's working set inside `-Xmx` still fails
with an `OutOfMemoryError`: the budgets bound speculative work and native
buffers, not the heap of the mandatory current row group. Wide projections over
large geometry columns are the case to watch; raise `-Xmx` or narrow the
projection rather than the budgets.

Settings beside parquetry's own that matter in a container:

- `-XX:TrimNativeHeapInterval=<ms>` has the JVM return what is freed to malloc
  by other native users (zlib, the font engine, the networking stack). It does
  not hold the peak of a burst down: under load the arenas refill within the
  interval.
- `MALLOC_ARENA_MAX` bounds glibc's arenas for the whole process, at a cost for
  other allocator-heavy libraries in the same JVM.
- `-Djdk.nio.maxCachedBufferSize` caps the temporary direct buffer kept per
  thread by the JDK for channel reads into heap buffers.

---

## 4. Where to look

| Concern | Start in |
|---------|----------|
| Budgets and their defaults | `runtime/ParquetRuntime.java`, `io/limits/IoLimits.java` |
| Off-heap fetch budget | `runtime/FetchBudget.java` |
| Decode budgets | `runtime/DecodeBudget.java` |
| The pool and its memory | `io/SegmentPool.java`, `io/DefaultSegmentPool.java`, `io/MappedMemory.java` |
| Decode-ahead + promotion | `internal/read/ParallelDecodeCoordinator.java`, `internal/read/StreamingBatchSource.java` |
| Prefetch | `internal/read/RowGroupPrefetcher.java` |

---

*Scope: bounding the memory of a single-file read. The general read path is in
[read-path.md](read-path.md).*
