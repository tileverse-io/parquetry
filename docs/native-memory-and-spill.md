# Native memory and spill on the read path

How parquetry bounds the off-heap memory a read holds for column-chunk fetch and
decompression, why a long-lived pod must not let that memory ratchet, and the
spill design that keeps it bounded under wide row groups and concurrency.
Companion to [Memory and tuning](memory-and-tuning.md) (the budgets and how to
size them) and [The read path](read-path.md) (the fetch and decode stages).

> **Status legend.** *Implemented* describes shipped behavior. *Planned*
> describes designed-but-unbuilt work, recorded here to keep the design durable
> and reviewable before it is built.

---

## 1. Where the off-heap term comes from

A read holds memory in two places: on-heap decoded batches, bounded by
`DecodeBudget` inside `-Xmx`, and **off-heap native buffers** for fetch and
decompression. The native buffers are the dangerous kind. They count against the
container's memory limit, are invisible to `-Xmx`, and the kernel kills the
process before the JVM can throw `OutOfMemoryError`. This document is about that
off-heap term.

A read fetches each row group's projected column chunks as a small set of
coalesced range reads, each read into a native segment borrowed from a
`SegmentPool`. Peak native fetch memory is approximately:

```
concurrency  x  (current row group's coalesced span)  +  speculative prefetch
```

`FetchBudget` bounds only the speculative prefetch term. The current row group's
fetch is mandatory for progress and is never bounded, which leaves the first
term, `concurrency x span`, with no cap of its own. With small row groups it is
modest. With the 128 MB+ row groups that Spark, DuckDB, and large GeoParquet
writers emit, a handful of concurrent reads can hold gigabytes of native memory
at once. That term is the subject of sections 3 and 4.

---

## 2. The pool must not ratchet — *Implemented*

`SegmentPool` is a reuse cache, not a memory bound. Its job is to avoid
re-allocating native segments across fetches; the high-water cap belongs to
`FetchBudget`, not the pool. But a naive reuse cache quietly inflates the
resident baseline of a months-long pod.

The trap, in the default pool: borrowed segments are request-sized, and a
fixed-count free list that retains "the largest segment seen" plus a
smallest-large-enough reuse policy converges to **the N biggest buffers it ever
saw, pinned for the JVM lifetime** - one heavy burst permanently inflates the
baseline. Native memory held by an auto arena frees only through garbage
collection, and that collection runs on heap pressure. This memory is off-heap,
beyond the reach of that pressure, and is effectively never reclaimed.

The pool therefore splits retention by size:

- **Small buffers** (rounded capacity at most a threshold) are pooled and reused,
  with total retained bytes capped (`maxPooledBytes`), not capped by count. A
  return that would breach the byte cap is dropped and becomes
  garbage-collectable.
- **Large buffers** (above the threshold) are never pooled. Each is allocated in
  its own shared arena and freed **deterministically** when the borrower closes,
  with no dependency on garbage collection. The borrow/return lifecycle already
  closes every buffer at row-group end; nothing above the pool changes.

The result: retained native memory is bounded by a small byte cap, and a burst of
large fetches leaves nothing pinned once the reads that needed them finish.

See `parquetry-io/.../io/DefaultSegmentPool.java`.

---

## 3. The overflow valve: spill to a mapped file — *Planned*

Section 2 bounds *retention*. It does not bound *peak*: the mandatory current
row group still allocates `concurrency x span`, and under wide row groups that
can exceed the container limit regardless of any budget. A hard byte cap on the
fetch path cannot be the answer, because the read holds buffers while acquiring
more (fetch, decompress, decode, release), and a blocking cap on that path
deadlocks - the in-order work cannot release until it finishes, and it cannot
finish without the buffer the cap is withholding.

The deadlock-free answer is to make the resource non-blocking: when the RAM
budget is exhausted, the buffer is backed by a **memory-mapped temp file** instead
of anonymous native memory. The decompressor reads from a `MemorySegment` either
way and never branches on RAM-versus-file. The win is reclaimability: file-backed
pages are clean page cache the kernel can evict under pressure, where anonymous
native memory is a hard OOM. Spilling is forward progress, never a wait, and a
path that never waits cannot deadlock.

The governing rule, shared with the decode-side spill:

> **The in-order critical path always acquires its buffer** - RAM if the budget
> allows, else a mapped file - and never parks while the disk has headroom. Only
> *speculative* fetch-ahead and decode-ahead may park, and only when even the
> disk budget is exhausted.

Because the in-order path always advances, it always drains and releases, and any
parked speculative work eventually unparks. One shared disk budget, one spill
directory, and this one invariant serve both the fetch spill described here and
the decode-batch spill already designed on the decode side.

This is the *reclaimable-overflow* design: the fetch pipeline is left whole, the
overflow lands on reclaimable page cache, and bounding the in-order working set
more tightly (by streaming a row group's coalesced groups one at a time) is a
deferred, measurement-driven refinement rather than a prerequisite.

---

## 4. The spill store, as an optimization ladder — *Planned*

The store that hands out mapped buffers should start as the simplest correct,
portable thing and earn each optimization with a measurement. The rungs:

**Baseline.** One temp file per spilled buffer: create, `FileChannel.map` into the
buffer's own arena, close the channel (the mapping outlives it), use, `arena.close()`
to unmap, delete. No offset arithmetic, no fragmentation - the filesystem is the
allocator. Crash-safety comes from a **startup sweep** of a dedicated spill
subdirectory, not from deleting open files: a `kill -9` leaves files, and the next
start reaps orphans from dead PIDs. This is portable across Linux, macOS, and
Windows.

> A note on a tempting POSIX shortcut. Create, map, then unlink the file
> immediately: the mapping keeps the inode alive, the inode dies on unmap, and
> cleanup is automatic with no leftover files. But it is POSIX-only. On Windows an
> active mapping pins the file and the delete is deferred or refused; NIO's
> `DELETE_ON_CLOSE` only approximates it and the interaction with a live mapping is
> platform-specific. The startup sweep is the portable contract; the unlink trick
> stays a Linux-only nicety, not a dependency.

**First optimization (measured).** A capped pool of kept-open files, the file-level
twin of the segment pool in section 2: reuse amortizes the per-spill create and
delete syscalls; an async reaper closes files idle past a TTL, keeping a burst
from pinning descriptors; the cap is a **file-descriptor** budget, the scarce
resource in a container, more than a disk one. A pool miss degrades to the
baseline per-buffer-file path - it never parks, exactly as a segment-pool miss
allocates fresh.

**Sparse, single-class slots.** Make the kept-open files **sparse** and one
generous block-aligned size (for example 8 MiB or the max coalesced span) rather
than a tier of size classes. A sparse file holding a 64 KiB buffer physically
occupies only the pages it touches: the unused capacity costs nothing, and the
size tiers collapse to one reusable slot that serves any buffer up to the cap. On
release, `MemorySegment.unload()` (a `madvise(MADV_DONTNEED)`-class hint) sheds the
**resident pages** - the scarce, OOM-driving resource. Note it does not deallocate
the file's disk blocks; reclaiming idle-slot *disk* needs `fallocate(PUNCH_HOLE)`,
which is Linux-only and bound through a foreign-function downcall. Because disk is
plentiful and bounded by the disk budget, hole-punching is only a Linux extra to
keep idle disk near zero, not a correctness requirement.

Sparseness, like the unlink trick, is a Linux physical optimization that is
correctness-neutral elsewhere: automatic on Linux (the deployment target),
explicit and not exposed by NIO on Windows/NTFS, filesystem-dependent on macOS. A
non-Linux host simply uses more disk, still under the disk budget.

Each rung is additive over a baseline that is correct and portable on its own.

---

## 5. Sizing the limits: an injectable resource probe — *Planned*

The limits these mechanisms honor - how much off-heap memory, how much spill disk,
and which directory - are physical facts about the pod. The component that
allocates native segments and maps files is the natural owner of those limits, and
it should not reach for `Runtime`, `FileStore`, or environment variables directly.
Following the same separation Guava draws between a `Ticker` and the `Stopwatch`
that reads it:

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
not on a large workstation. Injecting a fake gives every such test a deterministic
bound independent of the machine it runs on.

---

## 6. Cross-reference: tileverse-storage `DiskCachingRangeReader`

tileverse-storage's `DiskCachingRangeReader` already implements most of the
section-4 *mechanism*: block-aligned files (one block per file), a size cap with
eviction that deletes the file (Caffeine's weighted LRU standing in for the idle
reaper), too-big-to-cache falling back to the delegate, parallel multi-block
load, startup re-adoption of existing files, and reads routed through the same
`ByteBufferPool`. The "first optimization" rung is essentially built there.

The reason it proved hard to get right is the reason it is *not* the same thing.
It is a **content cache** - it keeps blocks to serve future reads, which drags in
content identity, cross-instance sharing, staleness, partial-read key rewrites,
and re-adoption on startup. A spill store is a **transient valve**: its slots are
anonymous scratch, freed the instant a read is done. Nearly all of the cache's
complexity is cache semantics a spill store does not have.

The unifying view: a disk cache and a spill store are **one block-aligned file
store under two retention policies** - spill evicts on release (retention zero),
the cache evicts on size pressure (retention until cold). The revisit is not a
rewrite into a spill store; it is to **factor out a shared block-aligned file
store** - slot allocation, the size and file-descriptor cap, deletion - and let
the cache layer content-addressing on top while the spill store uses it bare,
both behind one disk budget.

Two mechanism upgrades move the shared store toward the section-4 design:

- **Map, don't copy.** The reader copies through `FileChannel.read`/`write` into a
  pooled buffer. The spill win is mmap - the slot *is* the `MemorySegment`,
  zero-copy, and `unload()` reclaims its pages. It already depends on
  `ByteBufferPool`, which now vends parquetry's native segments through the SPI
  provider, which is the natural seam for mapped slots.
- **Add the sparse / `unload()` / `PUNCH_HOLE` rung** from section 4 on top of the
  baseline `FileChannel` lifecycle it has today.

The traps section 4 names - portable crash cleanup over the non-portable
unlink-on-open shortcut, sparse-file portability, and `unload()` shedding pages
versus `PUNCH_HOLE` reclaiming disk - are the ones that shared store still has to
handle.

---

*Scope: the off-heap fetch/decompression term of a read. The on-heap decode
budget and how to size both against a pod limit are in
[memory-and-tuning.md](memory-and-tuning.md).*
