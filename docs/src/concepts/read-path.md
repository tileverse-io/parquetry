# The read path

!!! abstract "In short"
    A read runs in four steps: filter, fetch, decode and materialize.
    The filter step works on the metadata of the file and skips the row groups and pages unable to match; the fetch step then reads the byte ranges of what is left.
    A read returns a lazy stream, and fetches no column data until the consumer pulls rows.

This page follows a `read()` from a Parquet file on a byte source to a stream of
records, from the top down. It is for developers who tune reads, investigate a
slow query or work on the reader. The sections zoom in one level at a time:
stop wherever you have enough detail.

The principle of a read is **filter, then fetch, then decode, then
materialize.** The sections below detail these four steps.

---

## 1. System context

What talks to what. `parquetry-core` reads one file. It owns neither the bytes
nor the threads of the consumer.

```mermaid
flowchart LR
    consumer["Consumer<br/>(GeoTools, an app)"]
    core["parquetry-core<br/>ParquetSource / ParquetFileReader"]
    source["ByteRangeSource / RangeReader<br/>(local file, S3, Azure, GCS, HTTP)"]
    file[("Parquet / GeoParquet file")]

    consumer -->|"read(predicate, projection, options)"| core
    core -->|"byte ranges"| source
    source -->|"bytes"| file
    core -->|"Stream of ParquetRecord / ParquetRecordBatch"| consumer
```

- The consumer passes a **predicate** (what rows), a **projection** (what
  columns), and **ReadOptions** (tunables), and gets back a lazy `Stream`.
- Parquetry reads through a byte-range abstraction and never assumes a local
  file. The consumer opens and closes that source.
- The reader is immutable and thread-safe. A `read()` allocates its own
  per-call state.

[Architecture](../developer-guide/architecture.md) shows the modules and the
components of the read path as C4 diagrams.

---

## 2. The four-stage pipeline

A file is opened once. Each `read()` then runs four stages from left to right:
filter, fetch, decode and materialize. The diagram names the lead types of the
stages.

```mermaid
flowchart LR
    subgraph open["1. Open (once per file)"]
        footer["read footer + schema<br/>ParquetFormat.readFooter"]
    end
    subgraph filter["2. Filter (per read, metadata only)"]
        pipe["5-tier FilterPipeline<br/>-> ExplainPlan -> survivors + masks"]
    end
    subgraph fetch["3. Fetch (per read, IO)"]
        pre["plan + prefetch byte ranges<br/>RowGroupFetcher / RowGroupPrefetcher"]
    end
    subgraph decode["4. Decode (per read, CPU)"]
        dec["stream page-batches in file order<br/>ParallelDecodeCoordinator"]
    end
    subgraph mat["5. Materialize"]
        out["batches -> rows<br/>BatchPipeline + Materializer"]
    end

    open --> filter --> fetch --> decode --> mat
    mat -->|"Stream of records"| consumer["Consumer"]
```

| Stage | Does | Reads | Lead types |
|------|------|-------|-----------|
| Open | decode the footer once, build the schema | footer bytes | `ParquetFileReader`, `FileMetaData`, `ParquetSchema` |
| Filter | drop row groups and pages that cannot match, using metadata only | footer + index sections | `FilterPipeline`, `ExplainPlan`, `RowGroupSurvivor`, `RowMask` |
| Fetch | turn the surviving column chunks into the exact byte ranges named by the plan, prefetched | column-chunk bytes | `RowGroupFetcher`, `RowGroupPrefetcher`, `FetchedColumnChunk` |
| Decode | decompress + decode pages into columnar batches, streamed in file order, decoding upcoming row groups ahead within a heap budget | (in memory) | `ParallelDecodeCoordinator`, `DecodeBudget`, `BatchRowGroupReader`, `BatchColumnReader` |
| Materialize | flatten batches into the caller's record shape | (in memory) | `BatchPipeline`, `Materializer`, `ParquetRecord` |

Two cross-cutting helpers thread through filter, fetch, and decode:

- **`RowGroupChunks`** - a per-call view of one row group's column chunks that
  memoizes its index sections (offset index, column index, bloom filter). A
  section is read at most once, however many stages ask for it. A chunk
  without an offset index takes its pages from the metadata cache, once a read has learned them
  (section 4).
- **`ReadOptions`** - the options of one read: which filter tiers run, the batch
  size, the page-narrowed fetch. The buffer pool, the fetch and decode budgets,
  the prefetch depth and the decode-ahead window belong to the `ParquetRuntime`
  shared by the readers ([Memory](memory.md)).

---

## 3. One `read()`, end to end

The control flow for a single `read(predicate, projection, options)`. This is the
spine; later sections expand the boxes.

```mermaid
sequenceDiagram
    autonumber
    participant C as Consumer
    participant R as ParquetFileReader
    participant RP as ReadPlanner
    participant P as ReadPlan
    participant FP as FilterPipeline
    participant DC as ParallelDecodeCoordinator
    participant PF as RowGroupPrefetcher
    participant BP as BatchPipeline

    C->>R: read(predicate, projection, options)
    R->>R: resolve the projection -> scanProjection, outputSchema
    R->>RP: plan(predicate, scanProjection, observation)
    RP->>RP: rowGroupChunks (one view per row group)
    RP->>FP: evaluate(predicate, scanProjection)
    FP-->>RP: ExplainPlan (per row group: ELIMINATED / PARTIAL / FULL / MATCHED)
    RP-->>R: ReadPlan
    R->>P: survivors() -> survivors
    R->>P: decode(survivors, DecodeShape)
    P->>P: per-group RowMask (page skip)
    P->>DC: new coordinator(survivors, masks, maskedScan?)
    P-->>R: coordinator
    R->>BP: rows(coordinator, materializer, outputSchema, recordFilter)
    BP-->>C: Stream (lazy)

    Note over C,BP: nothing read yet, work happens as the stream is pulled

    C->>BP: pull next record
    BP->>DC: next() decoded row group (file order)
    DC->>PF: take(rowGroupIndex) fetched bytes
    PF-->>DC: RowGroupFetch (prefetched)
    DC->>DC: decode columns -> batches (parallel, page-skip, predicate in scan)
    DC-->>BP: DecodedRowGroup
    BP->>BP: flatten batch -> rows
    BP-->>C: ParquetRecord
```

Key points shown by the diagram:

- **The stream is lazy.** `read()` returns immediately; filtering at the metadata
  level has run, but no column bytes are fetched or decoded until the consumer
  pulls.
- **Filter expands the projection.** When the record-level filter is on, the
  *scan* projection is the caller's projection plus the predicate's columns.
  The predicate can then be evaluated on columns not requested by the caller.
  The *output* schema stays the caller's projection.
- **Outcomes flow forward.** Per row group, the plan yields `ELIMINATED`
  (dropped), `FULL` (read the rows), `PARTIAL` (read only `survivingRows`), or
  `MATCHED` (statistics proved that all rows match: read them, but skip the
  per-row record filter). `PARTIAL` becomes a `RowMask` that skips non-surviving
  pages during decode.
- **The plan assembles the decode.** A `ReadPlan` holds the outcome of the
  filter pipeline for one query. The entry points (`read`, `readBatches`,
  `count` and the filtered `bounds`) state the rest of their decode as a
  `DecodeShape` (masked scan, batch form, row positions to synthesize), and the
  plan builds the coordinator from both in `decode`.

`readBatches()` is the same spine without the last step: it returns the batches
themselves, holding exactly the rows matched by the predicate, narrowed to the
caller's projection. The row `read()` is `readBatches`' pipeline plus
row-flattening into the caller's record shape.

---

## 4. Filtering: the five tiers

The filter pipeline runs on **metadata only**: no column data is decoded here.
The pipeline runs a row group through five tiers, cheapest first. It stops as
soon as a tier proves that no row of the row group can match, or that all its
rows match.

```mermaid
flowchart TD
    start["row group"] --> norm["PredicateNormalizer<br/>(Not-to-leaves, fold Always, flatten And/Or)"]
    norm --> stats["STATS<br/>min/max from column metadata"]
    stats -->|eliminated| drop["drop row group"]
    stats -->|"all rows match"| matched["MATCHED"]
    stats -->|maybe| spatial["SPATIAL<br/>geometry bbox of the row group"]
    spatial -->|eliminated| drop
    spatial -->|maybe| ci["COLUMN_INDEX<br/>per-page min/max -> RowRanges"]
    ci -->|eliminated| drop
    ci -->|narrowed| partial["PARTIAL: survivingRows"]
    ci -->|maybe| bloom["BLOOM_FILTER<br/>probe Eq / In keys"]
    bloom -->|eliminated| drop
    bloom -->|maybe| record["RECORD_LEVEL<br/>(runs later, during decode)"]
    record --> survivor["survivor: FULL or PARTIAL"]
    partial --> survivor
```

- **Cheapest first.** STATS and SPATIAL read only the footer. COLUMN_INDEX and
  BLOOM_FILTER read small index sections, loaded lazily and memoized by
  `RowGroupChunks`. RECORD_LEVEL is the only tier needing decoded values. It
  runs later, during the decode ([The masked scan](#6-the-masked-scan)).
- **COLUMN_INDEX is the lever for selective reads.** It narrows a row group to the
  pages whose min/max bounds overlap the predicate, producing `survivingRows` (a
  `RowRanges`). That is what lets decode skip whole pages. A file without a page
  index gets the same lever from the second read of a row group, when that read
  scans only learned columns ([Without a page index](#without-a-page-index)).
- **STATS also proves the positive.** When the statistics of a row group prove
  that *all* its rows match, and not only that some might, the STATS tier stops
  the pipeline with the `MATCHED` outcome. A `MATCHED` row group is read in
  full but skips the per-row record filter, and [`count()`](counting.md)
  answers it from the row count, without decoding.
  [Counting](counting.md#3-proving-a-whole-row-group-matches) explains the
  deliberately conservative proof keeping this sound.
- **The output is an `ExplainPlan`** - the same structure returned by `explain()`
  and rendered by `toAsciiTable()` / `toJson()`. The `ReadPlan` of the query
  routes its row groups to the `RowGroupSurvivor` list consumed by the rest of
  the pipeline. A read takes its `survivors()`. A count takes its `residual()`
  and answers the `MATCHED` row groups from metadata.

`RowGroupChunks` is what keeps this honest: the tiers ask it for a column's
stats / page index / bloom filter, and it loads a section once and hands the
same instance to the decode-mask builder later.

The SPATIAL tier applies to bbox predicates on a GeoParquet geometry column.
Such a predicate also goes through a covering-column lowering step, which feeds
the STATS and COLUMN_INDEX tiers. See [Spatial filtering](spatial-filtering.md).

### Without a page index

The COLUMN_INDEX tier works from the page index of a chunk. The page index has
two sections. The offset index places the pages, and the column index holds
their min/max bounds. pyarrow 26.0.1 writes no page index by default. A
filtered read of such a file learns the pages from their page headers.
Pruning starts at the second read of a row group, when that read scans only
learned columns and the cache still holds their pages.

A read keeps the pages of a row group only when three rules hold:

1. Its predicate has a part evaluable by the COLUMN_INDEX tier, such as a
   comparison, an `In` or a null test.
2. The statistics of the row group do not already show each row passing that
   part.
3. That part drops at least one page of the row group.

The planner checks the first two rules when it plans the read, and a row group
failing them gets no learning pass. A full scan learns nothing.

- **No extra read.** Without a page index, a read fetches the scanned column
  chunks of a row group whole. The fetch thread parses the page headers out of
  those bytes, and the read fetches nothing more. Learning costs one more parse
  of the page headers, in memory. The fetch time shown by
  `par explain --analyze --format json` leaves the learning pass out, because
  the fetch clock stops before it.
- **One packed entry per chunk.** An entry lists the data pages of its chunk,
  with their offsets, sizes, first rows, encodings, null counts and min/max
  bounds. A chunk gets an entry for all its data pages or no entry at all. A read
  learns only row groups with flat scanned columns and no declared offset index.
  It keeps the entries of the scanned chunks of a row group together or not at
  all, because a row mask needs the pages of the scanned columns.
- **Trusted bounds only.** A read uses page bounds only where the reader
  applies an order to them and can decode them. Long binary bounds are cut to
  64 bytes, and a cut bound still holds for its page. Geometry, geography and
  INT96 columns keep page locations and null counts, without bounds.
- **One page view.** The kept entries go into the metadata cache of the
  process, beside the parsed footers
  ([Memory](memory.md#the-caches)). A later read
  of the row group takes them as it takes a declared page index. The
  COLUMN_INDEX tier and the `RowMask` do not tell the two sources apart. The
  fetch of the surviving pages differs in one point, because learned pages record
  their encodings and can leave a dictionary page out
  ([Fetch and decode](#5-fetch-and-decode)). An `explain()` after the first read
  shows the decisions of the next one.
- **Turning it off.** A read with the COLUMN_INDEX tier off neither learns pages
  nor uses learned pages ([Turn a tier off](../guides/filter.md#turn-a-tier-off)
  shows the switch). A metadata pool with a budget of 0 turns learning off
  for the process ([Size the caches](../guides/tune-memory.md#size-the-caches)). A
  source without a name (`ByteRangeSource.sourceIdentifier()` is empty) learns
  nothing. A file with an offset index on each chunk learns
  nothing and looks nothing up.

The figures below come from one run of the integration test
`PageIndexLearningIT`, in `parquetry-core`. The test writes a file of five row groups of
1,000 rows in uncompressed pages of 100 rows. Its name column holds
plain-encoded strings of 80 characters. In one run of the test, a query fetched
99,388 bytes on its first read and 9,613 on its second, for eleven rows on one
page. Over the same rows written with a page index, the same query fetched
10,288 bytes, its index sections included. The test checks ratios between these
figures, not the figures themselves.

A declared page index prunes from the first read and does not depend on the
cache. Learning is a bridge for the files written without one.
[Lay out files for selective queries](../guides/file-layout.md) shows how to
write a page index, with Parquetry and with other tools.

---

## 5. Fetch and decode

Surviving row groups still hold raw bytes. Fetch turns them into exactly the
byte ranges named by the plan; decode turns those into columnar batches, in
parallel, while preserving file order.

```mermaid
flowchart TD
    subgraph fetchsub["Fetch (IO, virtual threads)"]
        plan["RowGroupFetcher.planFor<br/>the exact ranges to read, in file order"]
        pref["RowGroupPrefetcher<br/>fetch ahead within FetchBudget"]
        fetched["RowGroupFetch<br/>FetchedColumnChunk slices (pooled buffers)"]
        plan --> pref --> fetched
    end

    subgraph decsub["Decode (CPU, shared pool)"]
        coord["ParallelDecodeCoordinator<br/>stream page-batches in file order;<br/>decode ahead within DecodeBudget"]
        rg["BatchRowGroupReader<br/>one BatchColumnReader per projected leaf"]
        col["BatchColumnReader<br/>page-by-page via PageCursor"]
        page["PageDecoder<br/>PLAIN / RLE_DICTIONARY / DELTA / ..."]
        coord --> rg --> col --> page
    end

    fetched --> coord
    page --> batch["ParquetRecordBatch<br/>(ColumnVector per column)"]
```

What the pieces guarantee:

- **`RowGroupFetcher`** asks for the ranges of a row group's plan in one batch
  call, each as its own range, and hands out zero-copy `FetchedColumnChunk` views
  into pooled buffers. The storage layer joins nearby ranges into fewer fetches
  ([batched reads](https://tileverse.io/storage/how-to/configure/#batched-reads)),
  and serves the cached ones from memory when the byte-range cache is on
  ([Memory](memory.md#the-caches)).
- **`RowGroupPrefetcher`** fetches upcoming row groups ahead of consumption on a
  per-read virtual-thread pool, bounded by a process-wide `FetchBudget` that caps
  the off-heap bytes held by column-chunk fetches. A prefetch is refused when the
  budget is full; the current row group's fetch waits a bounded time for room and
  spills to a mapped file when the wait expires.
- **`ParallelDecodeCoordinator`** decodes several row groups concurrently on a
  shared CPU pool but returns them in file order; the stream stays ordered. A
  decode worker streams one page-batch at a time into a small bounded hand-off
  (two batches) and parks under backpressure when the consumer is slow.
  A single row group is never fully resident; only the in-flight window of its
  batches is.
- **`BatchColumnReader`** is page-at-a-time. A page is decompressed into a
  short-lived confined `Arena`, decoded into heap arrays, and the arena is closed
  before the next page. With a `RowMask`, `PageCursor` skips non-surviving pages
  outright, and surviving pages are compacted to the surviving rows.
- **In a chunk fetched whole, a dictionary decodes on the first page using it, on
  the thread decoding that page, until the read has used a dictionary of its
  column.** The fetch reads and checks the header of a fetched dictionary page,
  and the page stays compressed in the fetch buffer. `BatchColumnReader` decodes
  it for the first page needing its values and keeps the result for the chunk. A
  page in another encoding reads no dictionary, and neither does a page holding
  only nulls. Before that first use, a flat column read without a row mask keeps
  its dictionary undecoded in two cases: no surviving row reaches a
  dictionary-encoded page of a column mixing encodings, or an output-only column
  or a leaf of a later stage (section 6) has no surviving row in a row group or a
  run of pages. A leaf of the first stage
  decodes its pages in order and needs its dictionary. An output column with only
  dictionary-encoded pages needs it too, in a row group with a surviving row. A
  repeated column and a reader under a row mask load their pages to size their
  windows. At a page boundary the scan can read one row alone, because a repeated
  column defers the last row of its page or a column stands on a page with no
  rows. An output column loads its page for that row, even when the row fails. A
  loaded dictionary-encoded page holding a value decodes the dictionary, with or
  without a surviving row. A corrupt dictionary page fails a read only when the
  read reaches a page using it. An unreadable dictionary page header fails the
  fetch, as does a header declaring a negative size or value count, or a payload
  past the fetched bytes.
- **A read decodes a dictionary ahead, on the fetch thread, once it has used a
  dictionary of that column.** A row group fetched before that first use stays
  lazy. A read has few decode workers, at most three ahead of the consumer by
  default, and the fetch threads add parallelism on a long read. A
  dictionary-encoded column of a long scan uses its dictionary in each row
  group. Decoding it during the fetch costs no more CPU, and the reader finds it
  ready. A short read of whole chunks has no earlier row group to learn from. Its
  dictionaries stay lazy, and a file without a page index keeps the saving. A
  dictionary fetched as its own prefix on a narrowed chunk decodes ahead when a
  fetched page uses it, and that page counts as a use of the column. The thread
  that read the bytes decompresses them faster than another core. A writer falls
  back to PLAIN once a chunk outgrows its dictionary, and a fetch of pages after
  the fallback leaves the dictionary undecoded. The fetch finds such a page by
  reading the headers of the fetched pages. A header failing to parse counts as
  no use, and a read reaching the page raises the failure. A dictionary decoded
  ahead and left unused costs that decode and nothing more. The decode ahead
  borrows its scratch from the pool, and a borrow never waits. It never fails the
  fetch. When it fails, the page stays undecoded, and a read reaching a page using
  it raises the failure.
- **A narrowed fetch reads a dictionary page when a fetched page uses it.** The
  dictionary page sits in the prefix of its chunk, the bytes before the first
  data page. A fetch narrowed by a row mask leaves the prefix out when each
  surviving page is known not to use it. Learned pages record the encoding of
  each page, and their answer is exact. A declared page index records no
  encoding. The `encoding_stats` of the chunk give the count of dictionary-encoded
  pages, and a page at or past that count is taken to use no dictionary.
  parquet-java 1.18.0 and Arrow C++ 26.0.1 (the writer of pyarrow) fall back
  to PLAIN at most once per chunk, after the dictionary-encoded pages. The
  format does not promise that order,
  and the fetch checks it. The prefix keeps its room in the fetch buffer, and the
  budget sees the same bytes, but the first call leaves the prefix unread. Once
  the pages arrive, the fetch thread reads their headers from memory. When a
  fetched page is dictionary-encoded, the fetch reads the prefix in one more
  call, into the room already held. A wrong count therefore costs one call and
  never a row, and a right count costs no call. A page header failing to parse
  on that check fails the fetch, as it would fail the decode. A declared index
  without a count reads the prefix with the pages. Parquetry's writer records no
  `encoding_stats`, and its files read this way. A chunk fetched whole holds its
  dictionary page.

**Memory contract.** Three budgets bound a read.

- `FetchBudget` caps the *off-heap* bytes held by column-chunk fetches, for the
  current row group and the prefetched ones alike. Off-heap memory counts
  against the container limit and is invisible to `-Xmx`.
- The off-heap `DecodeBudget` (`ParquetRuntime.offHeapDecodeBudget`) caps the
  *off-heap* bytes holding decoded values in native segments.
- The on-heap `DecodeBudget` (`ParquetRuntime.decodeBudget`) caps the
  *on-heap* bytes held by a read in speculatively decoded batches, inside
  `-Xmx`.

The on-heap `DecodeBudget` controls speculative decode-ahead only. The current
row group, the one being read by the consumer, never reserves it, and that
prevents deadlock on the decode side. When the consumer advances to a row
group decoding ahead, the coordinator promotes it, and it stops reserving.
`ParquetRuntime.maxDecodeAhead` is a concurrency cap on decode worker slots,
not a memory bound. A speculative row group decodes ahead only when a slot is
free and the budget has room. Under slot contention, a read decodes inline on
the consumer thread, synchronously and still within its memory bounds.

The design target is a GeoServer pod with 1-2 GB for Parquetry, not "load the
file." The peak memory of the process, and the sizing of the budgets against a
container limit, are in
[Memory](memory.md#4-sizing-against-a-container-limit).

---

## 6. The masked scan

Decoding the output columns of the surviving rows and then dropping the
non-matches wastes work under a selective predicate. A filtered read therefore
decodes a row group in **one pass that applies the predicate as it goes**: the
filter columns decode, the predicate runs over them, and an output column materializes
a value only where the predicate kept the row. One reader per row group, one
walk over the bytes. `ParallelDecodeCoordinator` chooses the reader of each row
group. A row group needing the predicate takes `MaskedScanRowGroupReader`, and
any other row group takes `BatchRowGroupReader`.

```mermaid
flowchart TD
    outcome{"row group outcome"}
    outcome -->|"MATCHED<br/>(statistics proved that all rows match)"| classic["BatchRowGroupReader<br/>full decode, no predicate"]
    outcome -->|"FULL / PARTIAL"| win

    subgraph scan["MaskedScanRowGroupReader"]
        win["next window: the rows served by the<br/>current page of each reader, capped by<br/>the batch size"]
        filt["decode the FIRST stage's columns<br/>over the whole window"]
        eval["evaluate the first stage over the window<br/>(VectorizedPredicateEvaluator)"]
        rowsLeft{"rows left?"}
        later["next LATER stage: decode its own columns<br/>over the whole window, evaluate the rows left"]
        step["no row left: the remaining stages and the<br/>output columns step over the window"]
        out["OUTPUT-only columns: decode the value<br/>slots covered by the survivors, step the<br/>page decoder over the rest"]
        gather["SHARED columns: gather the survivors<br/>out of the window already decoded"]
        win --> filt --> eval --> rowsLeft
        rowsLeft -->|"yes, a stage remains"| later
        later --> rowsLeft
        rowsLeft -->|"none"| step
        step --> win
        rowsLeft -->|"yes, no stage remains"| out
        out --> gather
    end

    gather --> batches["batch of surviving rows,<br/>dense and in row order"]
    classic --> narrow["narrow to the caller's projection"]
    narrow --> batches
```

- **A window is page-bounded, not row-group-bounded.** It is the run of rows served
  by the current page of each scanned column, capped by the read's batch size. A
  window whose rows all fail the predicate emits nothing and materializes no output
  value.
- **A filter column waits for the conditions before it.** The conditions of an
  `And` split into up to three stages (`ScanStages`). Conditions on fixed-width
  columns run first, conditions on `BYTE_ARRAY` columns next, and per-row geometry
  tests last. The file schema sets this order, and it holds for the whole read. A
  condition on a repeated column or a row position joins the first stage, and a
  predicate other than an `And` is one stage. A later stage reads its own columns
  only for a window with rows left, and it evaluates those rows alone. With no row
  left its columns step over the window, counting rows against the page header
  (`ValueDecode.EAGER_ON_DEMAND`). A page of a later stage therefore decompresses
  only for a window with rows left. A page with all its rows stepped over is
  discarded unread, and `par explain --analyze --format json` counts it in
  `pagesPruned`. Under a row mask a page loads at window sizing, because no header
  states the rows of the mask.
- **Output columns pay for survivors only.** An output-only leaf reads the value
  slots covered by its surviving rows and steps its page decoder past the rest
  (`ValueDecode.WINDOWED_MASK`, survivor slots computed by `MaskedValues`).
  Decoded-value count tracks selectivity, not page size.
- **A column that both filters and outputs is read once.** It decodes as a filter
  column, and its surviving rows are gathered out of the window already in hand
  rather than through a second reader over the same pages.
- **`MATCHED` row groups never reach the scan.** Statistics already proved that
  their rows match, and there is nothing to evaluate. They take
  `BatchRowGroupReader` over the scan schema, and the batch pipeline narrows the result to the caller's
  projection - which is what keeps a predicate-only column out of the emitted
  shape.
- **Unfiltered reads are untouched.** No predicate means no scan:
  `BatchRowGroupReader` decodes the projection and nothing narrows.
- **Offset indexes are not required.** The scan walks pages as it meets them.
  Where the `COLUMN_INDEX` tier did produce a `RowMask`, the scan's readers take
  it and walk only the surviving rows; the two compose. Building that mask still
  needs flat scan columns and their page locations, from an offset index or
  learned by an earlier read (section 4). Learned pages serve the mask as
  declared ones do, and it is the mask, not the scan, that is flat-only.
- **Nested output columns are supported.** Repetition and definition levels travel
  with each window and are gathered alongside the values. A row whose values
  run past the end of its first page is followed across the boundary as a
  one-row window.
- **Both `read()` and `readBatches()` take it.** The predicate is applied exactly
  during decode, and no per-row filter runs afterwards at materialization.
- **The walk is proved.** A scan whose windows do not add up to the rows given
  to it by the plan throws `MalformedFileException` rather than silently
  dropping rows.

[`count()`](counting.md) pushes the same instinct to its limit: it materializes
no record, answering proven row groups from metadata and counting the rest
with a columnar popcount over the decoded predicate columns.

---

## 7. Key types

A map of the types named on this page. An arrow reads "uses" or "produces".

```mermaid
classDiagram
    class ParquetFileReader {
        +read(predicate, projection, options) Stream
        +readBatches(...) Stream
        +explain(...) ExplainPlan
    }
    class ReadPlanner {
        plan(predicate, scanProjection, observation) ReadPlan
        decodeUnplanned(survivors, scanSchema, options) ParallelDecodeCoordinator
    }
    class ReadPlan {
        survivors() List
        residual() List
        decode(survivors, DecodeShape) ParallelDecodeCoordinator
    }
    class DecodeShape
    class RowGroupChunks {
        +pageIndex(path)
        +pageLocations(path)
        +bloom(path)
        +stats(path)
    }
    class FilterPipeline
    class ExplainPlan
    class RowGroupSurvivor
    class RowMask
    class ParallelDecodeCoordinator
    class RowGroupPrefetcher
    class RowGroupFetcher
    class BatchRowGroupReader
    class MaskedScanRowGroupReader
    class BatchColumnReader
    class ValueDecode
    class MaskedValues
    class BatchPipeline
    class Materializer

    ParquetFileReader --> ReadPlanner : one per file
    ParquetFileReader --> DecodeShape : per query
    ReadPlanner --> RowGroupChunks : builds per call
    ReadPlanner --> FilterPipeline : runs
    FilterPipeline --> ExplainPlan : produces
    ReadPlanner --> ReadPlan : per query
    ReadPlan --> RowGroupSurvivor : survivors / residual
    RowGroupSurvivor --> RowGroupChunks : holds
    ReadPlan --> RowMask : page-skip mask
    ReadPlan --> ParallelDecodeCoordinator : decode
    ReadPlanner --> ParallelDecodeCoordinator : decodeUnplanned for unfiltered bounds
    ParallelDecodeCoordinator --> RowGroupPrefetcher : pulls fetched bytes
    RowGroupPrefetcher --> RowGroupFetcher : batch range reads
    ParallelDecodeCoordinator --> BatchRowGroupReader : unfiltered or MATCHED, full decode
    ParallelDecodeCoordinator --> MaskedScanRowGroupReader : filtered row group, one masked walk
    BatchRowGroupReader --> BatchColumnReader : one per leaf
    MaskedScanRowGroupReader --> BatchColumnReader : one per scanned leaf
    MaskedScanRowGroupReader --> MaskedValues : survivor value slots
    BatchColumnReader --> ValueDecode : eager, eager-on-demand or windowed-mask
    ParquetFileReader --> BatchPipeline : rows / batches
    BatchPipeline --> Materializer : record shape
```

---

## 8. Where to look

Paths are relative to the `io/tileverse/parquetry` package of `parquetry-core`.
The footer and schema files are in `parquetry-format`.

| Stage | Start in |
|------|----------|
| Entry, orchestration | `data/ParquetFileReader.java`, `data/ReadOptions.java` |
| Query plan + decode assembly | `data/ReadPlanner.java`, `data/ReadPlan.java`, `data/DecodeShape.java` |
| Footer + schema | `format/ParquetFormat.java`, `schema/SchemaBuilder.java` |
| Filter pipeline + tiers | `internal/filter/FilterPipeline.java`, `internal/filter/*Evaluator.java`, `filter/explain/ExplainPlan.java` |
| Per-call chunk view | `internal/read/RowGroupChunks.java` |
| Page index from page headers | `internal/filter/PageLearning.java`, `internal/read/HeaderPageIndexLearner.java`, `internal/read/page/PageHeaderScan.java`, `internal/filter/LearnedPages.java`, `internal/filter/PageIndex.java` |
| Which files learn and look up pages | `data/CachedPageIndexes.java` |
| Bounds kept by a learned entry | `internal/filter/LearnedBounds.java` |
| Metadata cache (footers + learned pages) | `data/FooterMetadataCache.java`, `data/ParquetCaches.java` |
| Survivors + page-skip mask | `internal/read/RowGroupSurvivor.java`, `internal/read/RowMask.java`, `internal/read/page/PageSelection.java` |
| Fetch | `internal/read/RowGroupFetcher.java`, `internal/read/RowGroupPrefetcher.java` |
| Dictionary page of a fetch | `internal/read/DictionaryFetch.java`, `internal/read/DictionarySource.java`, `internal/read/ColumnChunkSlicer.java`, `internal/read/DictionaryPage.java` |
| Parallel decode + budgets | `internal/read/ParallelDecodeCoordinator.java`, `internal/read/StreamingBatchSource.java`, `runtime/DecodeBudget.java`, `internal/read/BatchRowGroupReader.java` |
| Column / page decode | `internal/read/BatchColumnReader.java`, `internal/read/page/PageCursor.java`, `internal/read/page/PageDecoder.java` |
| Masked scan (filtered decode) | `internal/read/MaskedScanRowGroupReader.java`, `internal/read/MaskedValues.java`, `internal/read/ScanStages.java`, `internal/read/ValueDecode.java` |
| Materialize | `internal/read/BatchPipeline.java`, `materializer/Materializer.java` |
| Counting (no materialization) | `data/ParquetFileReader.java` (`count`), `internal/read/BatchPipeline.java` (`countMatching`), `columnar/VectorizedPredicateEvaluator.java` |

---

*Scope: the row and batch read paths. Flat and nested or repeated columns both
go through the masked scan. The column-index page-skip mask is the flat-only
part, and a read with a nested scan column runs the scan without one. Spatial
(bbox) filtering is covered in [Spatial filtering](spatial-filtering.md), and
counting without materialization in [Counting](counting.md).*
