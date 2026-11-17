# Tune memory

You run Parquetry in a Java application with a memory limit, such as a container, and want to check or change how much memory its reads may take. Parquetry sizes its memory budgets from the container limit at startup, and most applications need no setting. This guide shows the budgets, the settings changing them, and the JVM settings beside them.

It needs `parquetry-core`. [Memory](../concepts/memory.md) explains the budgets and why they follow the container limit.

## See the budgets

The default runtime holds the budgets shared by the readers of the process:

```java
import io.tileverse.parquetry.runtime.ParquetRuntime;

public class ShowBudgets {

    public static void main(String[] args) {
        ParquetRuntime runtime = ParquetRuntime.defaultRuntime();
        long fetchMiB = runtime.fetchBudget().capacity() >> 20;
        long decodeMiB = runtime.offHeapDecodeBudget().capacity() >> 20;
        System.out.println("fetch budget: " + fetchMiB + " MiB");
        System.out.println("off-heap decode budget: " + decodeMiB + " MiB");
    }
}
```

Run with `-Dparquetry.io.maxOffHeap=2g`, it prints:

```
fetch budget: 409 MiB
off-heap decode budget: 1024 MiB
```

The fetch budget bounds the native memory of the column data fetched by the reads, and the off-heap decode budget the native memory of decoded values. Both sit outside the Java heap. When the fetch budget is full, the fetch of the row group being read waits for room, then writes its buffers to a temporary file: the read slows down, and the process keeps its size.

## Change the memory figure

By default, the budgets derive from the memory left beside the heap: the container limit minus the maximum heap. The fetch budget takes a fifth of it, and the off-heap decode budget half. These settings replace the figures measured on the machine, as system properties or environment variables, with a size such as `512m` or `2g`:

| System property | Environment variable | Sets |
| :--- | :--- | :--- |
| `parquetry.io.maxOffHeap` | `PARQUETRY_IO_MAX_OFFHEAP` | The memory figure used to derive the budgets. It is not a cap: the budgets take their fractions of it. |
| `parquetry.io.maxSpill` | `PARQUETRY_IO_MAX_SPILL` | The disk figure used to derive the spill budget. Spill files may take half of it. By default, it is the free space of the spill directory. |
| `parquetry.io.spillDir` | `PARQUETRY_IO_SPILL_DIR` | The directory of the spill files, `java.io.tmpdir` by default. |

The system property wins over the environment variable. A spill directory on a memory-backed file system, such as a `tmpfs` volume, counts against the memory limit of the container: choose a directory on disk.

## Give readers a runtime of their own

`ParquetRuntime.builder()` sets the budgets explicitly, and `ParquetFileReader.open` takes the runtime:

```java
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.runtime.DecodeBudget;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import java.time.Duration;
import java.util.Optional;

ParquetRuntime runtime = ParquetRuntime.builder()
        .fetchBudget(FetchBudget.ofBytes(256L << 20))
        .offHeapDecodeBudget(DecodeBudget.ofBytes(512L << 20))
        .decodeBudget(DecodeBudget.ofMaxMemoryFraction(0.25))
        .mandatoryBufferWait(Duration.ofSeconds(1))
        .build();
ParquetFileReader reader = ParquetFileReader.open(source, runtime, Optional.empty());
```

The third argument supplies the decryption keys of an encrypted file. It stays empty, because Parquetry does not read encrypted files ([Format support](../reference/format-support.md#not-supported)).

The readers opened with one runtime share its budgets. A reader opened with its own runtime gets its own native budgets, and keeping their sum inside the container is up to you. Build one runtime per process, and share it. The datasets of [Multi-file datasets](datasets.md) read with the default runtime.

`decodeBudget` is a share of the Java heap: it bounds the batches decoded ahead of the consumer by a read. It is inside `-Xmx` and adds nothing to the native memory.

## Spill at once, or wait

Two settings of the runtime choose between waiting and spilling to disk:

- `mandatoryBufferWait` is the longest wait of a read for room in the fetch budget, 1 second by default. When the wait expires, the read takes a memory-mapped temporary file for its buffer. `Duration.ZERO` spills at once, without waiting.
- `spillEnabled(false)` keeps the batches decoded ahead out of spill files: a batch over the decode budget waits for room instead. It is on by default.

[Native memory and spill](../concepts/native-memory-and-spill.md) explains the wait and the spill.

## Size the caches

Parquetry keeps two kinds of data in the shared caches of tileverse, both on the Java heap:

- The parsed footers and the page indexes learned from page headers, in the metadata pool, sized by `io.tileverse.cache.metadata.max-size`. A budget of 0 keeps neither, and reads then learn no pages.
- The byte ranges read from remote storage, in the data pool, sized by `io.tileverse.cache.data.max-size`, when the byte-range cache is on ([Cloud storage](cloud-storage.md#what-parquetry-keeps-between-reads)). A GeoTools data store turns it off with its parameter `storage.caching.enabled=false`.

The tileverse guide [Caching layers](https://tileverse.io/storage/how-to/configure/#caching-layers) gives the defaults and the format of the sizes.

`ParquetCaches.clearAll()` clears both pools. `FooterMetadataCache.clear()` drops the footers and the learned page indexes alone.

## Allow native access

Start the JVM with `--enable-native-access=ALL-UNNAMED`. Parquetry allocates its buffers of 64 KiB or more as memory mappings on Linux and macOS, and returns their memory to the system as soon as it frees them. Without the flag, the JDK prints a warning at the first mapping. A JVM started with `--illegal-native-access=deny` keeps the buffers on malloc.

Mapped memory does not appear in the native memory tracking of the JVM (`jcmd <pid> VM.native_memory`). `SegmentPool.getDefault().stats()` reports the memory held by the pool. The system property `parquetry.io.mappedBackings=false` keeps the buffers on malloc.

## Size the container

The peak memory of the process is the maximum heap, plus the two native budgets, plus the decompressed pages of the row groups being decoded, plus the native memory of the JVM itself ([Sizing against a container limit](../concepts/memory.md#4-sizing-against-a-container-limit)). With `-XX:MaxRAMPercentage=50`, the heap takes half the container and the budgets take at most 35% of it. When you size the container:

- A heap set above half the container shrinks the budgets with the memory left beside it, and more fetches go to spill files. With a heap close to the container limit, the budgets still derive from an eighth of the container. The heap, the budgets, the decoded pages and the JVM itself may then need more memory than the container allows.
- The on-heap decode budget is inside `-Xmx`, and adds nothing to the sum.
- The heap scratch kept by Parquetry for tileverse readers, such as a PMTiles reader, is inside `-Xmx` too: at most 32 MiB between uses, released after 30 seconds unused. A row-group fetch uses none.
- The caches are inside `-Xmx`: their budgets are shares of the heap ([Caching layers](https://tileverse.io/storage/how-to/configure/#caching-layers) gives the defaults).

A read still needs the working set of its current row group on the heap. A row group too large for `-Xmx` fails the read with an `OutOfMemoryError`, because the budgets bound speculative work and native buffers, not the current row group. Wide projections over large geometry columns are the case to watch. Raise `-Xmx` or narrow the projection, rather than the budgets.

## JVM settings beside Parquetry's

These settings matter in a container, for the native memory of other users in the same JVM:

- `-XX:TrimNativeHeapInterval=<ms>` has the JVM return to the system the memory freed to malloc by other native users, such as zlib, the font engine or the network stack. It does not hold down the peak of a burst: under load, malloc refills within the interval.
- `MALLOC_ARENA_MAX` bounds the malloc arenas of glibc for the whole process, at a cost for other libraries allocating heavily in the same JVM.
- `-Djdk.nio.maxCachedBufferSize` caps the temporary direct buffer kept per thread by the JDK for channel reads into heap buffers.

## Temporary files of the writer

The writer encodes the columns of the current row group into temporary files, and appends the row group to the output when it is complete. `WriteOptions.builder().tempDir(path)` sets their directory, `java.io.tmpdir` by default. Each writer creates a private subdirectory there and deletes it when it closes. The directory must exist and be writable.

Like the spill directory, a temporary directory on a memory-backed file system counts against the memory limit of the container.

## Under the hood

- [Memory](../concepts/memory.md) explains the budgets, their defaults and the caches.
- [Native memory and spill](../concepts/native-memory-and-spill.md) explains the native buffers and the spill files.
