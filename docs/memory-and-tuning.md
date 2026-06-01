# Memory and tuning in parquetry

How a read bounds its memory, the two dials that bound it, and how to size them
against a container limit. Read [The parquetry read path](read-path.md) first for
the fetch and decode stages these dials act on.

The guiding rule: **a read is bounded by two budgets, one off-heap and one
on-heap, and both auto-scale from a single fraction of the container limit.** The
default target is a GeoServer pod with 1-2 GB for parquetry, not "load the file."

---

## 1. The two budgets

A read holds memory in two places, and each has its own cap.

| Budget | Bounds | Counted against | Default |
|--------|--------|-----------------|---------|
| `FetchBudget` | off-heap bytes a read may speculatively prefetch for column-chunk fetches | the container limit, invisible to `-Xmx` | `0.1` of `min(maxHeap, MaxDirectMemorySize)` |
| `DecodeBudget` | on-heap bytes a read may hold in speculatively decoded batches | `-Xmx` (the JVM heap) | `0.25` of `Runtime.maxMemory()` |

Both are shared process-wide: regardless of how many concurrent reads run, total
speculative fetch memory stays under one `FetchBudget` and total speculative decode
memory under one `DecodeBudget`. Both are set on `ReadOptions`:

```java
ReadOptions options = ReadOptions.builder()
        .decodeBudget(DecodeBudget.ofMaxMemoryFraction(0.25))
        .fetchBudget(FetchBudget.ofMaxMemoryFraction(0.1))
        .build();
```

Only speculative work reserves against either budget. The in-order current row
group (the one the consumer is reading) always makes progress and never reserves,
which is what prevents a budget too small for a single batch from deadlocking a
read. See the [read path](read-path.md#5-fetch-and-decode) for the promotion rule
that releases a parked decode-ahead worker when its row group becomes current.

---

## 2. Why the off-heap default is conservative

Off-heap growth is what drives container OOM-kills: it counts against the pod's
memory limit but is invisible to `-Xmx`. The JVM cannot reclaim it under heap
pressure, and the kernel kills the process before any `OutOfMemoryError` is thrown.
The `FetchBudget` default is therefore a small fraction (`0.1`) of
`min(maxHeap, MaxDirectMemorySize)` - large enough to overlap fetch with decode,
small enough that prefetch alone cannot push the process past its limit.

`DecodeBudget` is heap-visible: decoded batches live inside `-Xmx`, where the JVM
manages them. Its default (`0.25` of `Runtime.maxMemory()`) leaves the rest of the
heap for the consumer's working set. `Runtime.maxMemory()` reflects the container
limit, which lets the fraction auto-scale per pod.

---

## 3. The adaptive fraction pattern

Prefer `ofMaxMemoryFraction(...)` over a per-pod fixed byte cap. One fraction
policy auto-scales across heterogeneous pods, because both budgets derive their
basis from the running JVM's limit:

| Pod | Heap | `DecodeBudget` at `0.25` | `FetchBudget` at `0.1` |
|-----|------|--------------------------|------------------------|
| WMS | 4-6 GB | ~1-1.5 GB | ~0.4-0.6 GB |
| WFS | 2-4 GB | ~0.5-1 GB | ~0.2-0.4 GB |
| WebUI | 1-2 GB | ~0.25-0.5 GB | ~0.1-0.2 GB |

A fixed `ofBytes(...)` cap exists for the case where a deployment wants an explicit
ceiling regardless of the JVM limit, but it must be re-tuned per pod size; the
fraction policy does not.

---

## 4. Sizing against a container limit

Peak process memory is approximately:

```
maxHeap (-Xmx, includes the on-heap DecodeBudget)
  + FetchBudget (off-heap)
  + retained segment pool (off-heap)
  + JVM native baseline
```

To stay under a pod limit, keep `maxHeap + off-heap + margin <= pod limit`:

- Set `-Xmx` below the pod limit by the off-heap budget plus the native baseline
  plus a safety margin. The off-heap term is `FetchBudget` plus the retained
  segment pool; the native baseline is the JVM's own footprint (metaspace, thread
  stacks, GC structures).
- `DecodeBudget` is inside `-Xmx` and does not add to the formula beyond `-Xmx`;
  it bounds how much of the heap speculative decode-ahead may occupy, leaving the
  remainder for the consumer.
- `FetchBudget` adds to the off-heap term directly. Raising it improves
  fetch/decode overlap on high-latency byte sources (S3) at the cost of off-heap
  headroom.

A read that cannot fit one row group's working set inside `-Xmx` will still fail
with an `OutOfMemoryError` regardless of `DecodeBudget`: the budget bounds
speculative decode-ahead, not the mandatory current row group. Wide projections
over large geometry columns are the case to watch; raise `-Xmx` or narrow the
projection rather than the budget.

---

## 5. Where to look

| Concern | Start in |
|---------|----------|
| Read tunables | `data/ReadOptions.java` |
| Off-heap fetch budget | `data/read/FetchBudget.java` |
| On-heap decode budget | `data/read/DecodeBudget.java` |
| Decode-ahead + promotion | `data/read/ParallelDecodeCoordinator.java`, `data/read/StreamingBatchSource.java` |
| Prefetch | `data/read/RowGroupPrefetcher.java` |

---

*Scope: bounding the memory of a single-file read. The general read path is in
[read-path.md](read-path.md).*
