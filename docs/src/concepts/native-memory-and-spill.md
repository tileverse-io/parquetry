# Native memory and spill on the read path

!!! abstract "In short"
    A read holds native memory, outside the Java heap, to fetch and decompress column data.
    When the fetch budget is full, a read waits for room, up to one second by default, then writes its buffers to a memory-mapped temporary file.
    Under pressure, reads slow down or spill to disk instead of growing the native memory of the process.

This page explains how Parquetry bounds the native memory held by a read for
column-chunk fetch and decompression, why a long-lived server must not let that
memory grow step by step, and the spill design keeping it bounded under wide
row groups and concurrency. It is for operators sizing a server and for
developers working on the read path.

[Memory](memory.md) covers the budgets and how to size them, and
[The read path](read-path.md) the fetch and decode stages.

---

## 1. Where the off-heap term comes from

A read holds memory in two places: on-heap decoded batches, bounded by
`DecodeBudget` inside `-Xmx`, and **off-heap native buffers** for fetch and
decompression. The native buffers are the dangerous kind. They count against the
container's memory limit, are invisible to `-Xmx`, and the kernel kills the
process before the JVM can throw `OutOfMemoryError`. This page is about that
off-heap term.

A read fetches each row group's projected column chunks as one batch range read
into a single native segment borrowed from a `SegmentPool`. That segment holds
the bytes requested by the plan and nothing between them. Peak native fetch
memory is approximately:

```
concurrency  x  (current row group's requested bytes)  +  speculative prefetch
```

`FetchBudget` bounds both terms. A prefetch reserves the plan's first-to-last
byte span rather than its bytes, and the reservation is then a ceiling on the
scratch memory of the byte source as well; a prefetch is refused when the budget
has no room. The current row group's fetch is mandatory for progress: it reserves
the same span when the budget has room, waits a bounded time for room otherwise,
and takes a mapped file when the wait expires (section 3). Without that valve the
first term, `concurrency x requested bytes`, would have no cap of its own. With
small row groups it is modest. With row groups of 128 MB or more, common in
files written for analytics engines, a handful of concurrent reads would hold
gigabytes of native memory at once. That term is the subject of sections 3 and 4.

---

## 2. The pool holds the working set, and its memory leaves the process when freed

`SegmentPool` is a reuse cache. It serves the fetch buffers, the decompressed
pages and the native decode buffers of concurrent reads without allocating per
use; the high-water caps belong to `FetchBudget` and the off-heap `DecodeBudget`,
not to the pool. Two things about its memory are easy to get wrong, and a
long-running process pays for both.

**Retention.** A reuse cache keeping "the largest segment seen" converges to the
N biggest buffers seen so far, pinned for the JVM lifetime. The pool therefore
splits retention by size and caps each class by bytes, not by count: small
buffers (rounded capacity at most 4 MB: pages, decode buffers) under one cap,
large ones (row-group fetch buffers) under another, each sized from the memory
beside the heap to keep the working set of a loaded server pooled. A return
that would breach its cap frees the buffer at once, and a background reaper
frees what sits idle for a minute. A cap below the working set is the other
failure: the pool then frees on most returns and allocates on most borrows.

The caps bound what sits idle, not what the pool holds. A ceiling on the pool's
total, borrowed and retained bytes together, is set to what the fetch and
off-heap decode budgets allow in flight. A return never raises that total. A
pool working inside its ceiling therefore retains the returned buffers, and only borrows
beyond the budgets, such as the decompressed pages, make later returns free
their memory until the total is back under it. Retention never adds to the
figure already accounted for by the budgets. It fills the room left idle by
the budgets.

**Where the bytes come from.** A buffer freed to glibc's malloc does not leave
the process. glibc keeps the freed chunk in the arena of the thread that
allocated it, allows up to eight arenas per CPU of the host (the container's
CPU limit does not count), and raises its mmap threshold as large chunks are
freed. Multi-megabyte buffers then come from those arenas instead of mappings. Each
arena keeps its own high-water mark, and `malloc_trim` cannot shrink an arena's
top below a threshold raised by the same churn to tens of megabytes. Under
concurrent reads, the process then holds far more anonymous memory than the
memory tracked by the JVM. A pool buffer of 64 KiB or more is therefore an
anonymous memory mapping (`MappedMemory`, reachable as
`Segments.allocateNative`), unmapped the moment it is freed, on Linux and
macOS. Smaller buffers and other platforms stay on malloc.

See `parquetry-io/.../io/DefaultSegmentPool.java` and `.../io/MappedMemory.java`.

---

## 3. The overflow valve: spill to a mapped file

Section 2 bounds *retention*. It does not bound *peak*: the mandatory current row
group still allocates `concurrency x requested bytes`, and under wide row groups
that can exceed the container limit regardless of any budget. A hard byte cap on
the fetch path cannot be the answer, because the read holds buffers while
acquiring more (fetch, decompress, decode, release), and a blocking cap on that
path deadlocks - the in-order work cannot release until it finishes, and it
cannot finish without the buffer withheld by the cap.

The answer has two parts. The resource is never withheld for good: when the RAM
budget stays exhausted, the buffer is backed by a **memory-mapped temp file**
instead of anonymous native memory. The decompressor reads from a `MemorySegment`
either way and never branches on RAM-versus-file. The win is reclaimability:
the kernel can write file-backed pages back to the file and evict them under
pressure. Anonymous native memory leads to a hard OOM instead. And a spill is
the last resort, not the first: the fetch of a read's current row group waits a bounded time for other
reads to release room. Under more concurrent reads than the budget has room
for, a spill per excess fetch turns the reads into a storm of disk writes, and
the throughput collapses. Queueing the excess fetches instead keeps most of the
throughput of the budgeted concurrency.

The governing rule of the fetch side:

> **The in-order fetch waits a bounded time for budget room, holding no other
> fetch buffer while it waits; then it takes a mapped file; then it fails.**
> A speculative fetch, for the prefetch window or for decode-ahead, never waits
> for budget room and never spills: it yields while the budget is full. A
> mandatory decode buffer never waits either: RAM if its budget allows, else a
> mapped file, else failure.

The wait cannot deadlock because the reads holding budget room are consuming a
row group and release it without needing more fetch budget; a waiting read holds
none itself. The timeout (`ParquetRuntime.mandatoryBufferWait`, 1 s by default)
covers a stalled consumer. A request larger
than the whole budget never waits. When the disk budget is exhausted as well, the
read fails with `BudgetExhaustedException`: a server turns it into one failed
request. One shared disk budget and one spill directory serve both the fetch
spill described here and the decode-batch spill on the decode side; the
decode-batch spill keeps its own rule, controlled by `spillEnabled` (below).
[Spill at once, or wait](../guides/tune-memory.md#spill-at-once-or-wait) sets
the timeout and `spillEnabled`.

This is the *reclaimable-overflow* design: the fetch pipeline is left whole, and
the overflow lands on reclaimable page cache.

As built: `FetchBufferAllocator` and `DecodeBufferAllocator` take a RAM segment
from the `SegmentPool` when the budget allows and otherwise a `FetchSpillStore`
mmap. The decompressor's `MemorySegment` never branches on RAM-versus-file.
`DiskBudget` bounds the spilled bytes process-wide; `ParquetRuntime` reaps
orphaned spill files at construction (`FetchSpillStore.sweepOrphans`) and exposes
`spillEnabled` (on by default). The decode-batch spill (`BatchSpillStore`,
`StreamingBatchSource`, `ParallelDecodeCoordinator`) shares the same `DiskBudget`
and spill directory.

---

## 4. The spill store

The store handing out mapped buffers is kept simple and portable. It writes one
temp file per spilled buffer: create the file, `FileChannel.map` it into the
arena of the buffer, close the channel (the mapping outlives it), use the
buffer, `arena.close()` to unmap, delete the file. There is no offset arithmetic
and no fragmentation: the filesystem is the allocator. `FetchSpillStore` is this
store on the fetch side. `BatchSpillStore` is the decode-side store: one
append-only file per row group, created on the first spill.

The fetch spill gets its crash safety from a **startup sweep** of a dedicated
spill subdirectory, not from deleting open files. Each process writes its fetch
spill files under a directory named after its process id. A `kill -9` leaves
files behind, and the next start removes the directories of dead processes.
This works on Linux, macOS and Windows.

A decode-batch spill file sits directly in the spill directory and is deleted
when its store closes. The startup sweep does not cover it: a decode-batch
spill file left behind by a `kill -9` stays on disk. The default spill
directory is the JVM temporary directory (`java.io.tmpdir`), and only the
cleanup of the operating system or an operator removes such a file.

> A note on a tempting POSIX shortcut. Create, map, then unlink the file
> immediately: the mapping keeps the inode alive, the inode dies on unmap, and
> cleanup is automatic with no leftover files. But it is POSIX-only. On Windows
> an active mapping pins the file and the delete is deferred or refused. NIO's
> `DELETE_ON_CLOSE` only approximates it, and its interaction with a live
> mapping depends on the platform. The spill store does not use the shortcut:
> the startup sweep is its portable contract.

---

## 5. Sizing the limits: an injectable resource probe

The limits honored by these mechanisms - how much off-heap memory, how much
spill disk, and which directory - are physical facts about the pod. The
component allocating native segments and mapping files is the natural owner of
those limits, and it should not reach for `Runtime`, `FileStore`, or
environment variables directly. It follows the separation drawn by Guava
between a `Ticker` and the `Stopwatch` reading it:

- an injectable **resource-limits port** reports *raw machine facts* - available
  memory, usable disk on a path, processor count, spill directory - with a default
  implementation that reads the container (cgroup limit, free disk) and folds in
  environment-variable and system-property overrides, and a test fake that returns
  fixed values;
- a thin **policy layer** applies the conservative fractions to derive the caps,
  in one place rather than re-derived independently per budget;
- the budgets and the spill valve **consume** those caps and arbitrate; they own
  no sizing of their own.

The override path matters as much for tests as for production tuning: without it,
a budget or spill test auto-sizes to its host and spills on a small CI runner but
not on a large workstation. An injected fake gives these tests a deterministic
bound, independent of the machine running them.

As built: `ResourceLimits` is the port - `DefaultResourceLimits` reads the
container and folds in environment-variable and system-property overrides, and
`ResourceLimits.fixed` is the test fake. `IoLimits` is the policy layer that
applies the off-heap, spill, and decode fractions in one place. `ParquetRuntime`
derives its budgets from `IoLimits.from(resourceLimits)` in `build()`. A caller
can override the budgets and the spill directory one by one.

---

## 6. A spill store is not a content cache

A spill store and a disk cache both keep blocks in files.
They differ in what the blocks are for. A **content cache** keeps blocks to
serve future reads. It must identify content, share it between readers, notice
stale data and find its files again after a restart. A spill store is a
**transient valve**: its files hold anonymous scratch, freed the instant a read
is done. None of the cache concerns apply to it, and the spill store stays
small because it does not take them on.

A spill file is deleted on release, and a cache block stays until the cache
needs room. Parquetry keeps its spill store separate from the caches of the
storage layer. The caches are described in the [tileverse-storage documentation](https://tileverse.io/storage/).

---

*Scope: the off-heap fetch/decompression term of a read. The on-heap decode
budget and how to size both against a pod limit are in
[Memory](memory.md).*
