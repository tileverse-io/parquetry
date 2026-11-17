# Size a GeoServer pod

You deploy GeoServer or GeoServer Cloud in containers and want to choose the CPU and memory of a pod serving Parquetry layers. Parquetry sizes the memory of its reads from the memory limit of the pod, and no setting is needed per layer. This page gives two pod profiles and the arithmetic behind them.

## How Parquetry follows the pod

At startup, Parquetry reads the memory limit of the container and the maximum heap of the JVM. The difference is the memory left beside the heap. Parquetry takes its two native memory budgets from it:

- the fetch budget, a fifth of it, bounds the column data fetched by the reads;
- the off-heap decode budget, half of it, bounds the decoded values kept outside the heap.

The layers of the pod share these budgets. Adding a layer adds nothing to tune. When the fetch budget is full, a read waits for room, then writes its buffers to a temporary file: the read slows down, and the pod keeps its size. A read fails with an error only when the disk budget is full too, and the other requests keep running.

The caches of Parquetry, the file footers and the byte ranges read from remote storage, are on the Java heap, inside `-Xmx`.

Parquetry decodes on a shared pool of threads, one per CPU seen by the JVM. The JVM counts the CPU limit of the container.

[Memory](../concepts/memory.md) explains the budgets, and [Tune memory](../guides/tune-memory.md) shows their settings.

## Two profiles

| Profile | CPU | Memory |
| :--- | :--- | :--- |
| Small | 4 CPU | 4 GB |
| Recommended | 8 cores | 16 GB |

The two profiles differ in size only. Parquetry applies the same rules to both, and the next section gives their budgets.

## The budgets of the profiles

The table applies the rules above to both profiles, for a heap at half the container, as set for example by `-XX:MaxRAMPercentage=50`. The figures are approximate.

| | Small, 4 GB | Recommended, 16 GB |
| :--- | :--- | :--- |
| Maximum heap | 2 GB | 8 GB |
| Memory beside the heap | 2 GB | 8 GB |
| Fetch budget, a fifth | 400 MB | 1.6 GB |
| Off-heap decode budget, half | 1 GB | 4 GB |
| Left for the pages being decoded, the JVM and the page cache | 600 MB | 2.4 GB |

The two budgets take at most 35% of the container. [Sizing against a container limit](../concepts/memory.md#4-sizing-against-a-container-limit) gives the full formula.

## Settings to check

- **Mind the heap size.** The budgets take their share of the memory left beside the heap. A heap above half the container shrinks them, and more fetches go to spill files. With a heap close to the container limit, the budgets still derive from an eighth of the container. The heap, the budgets, the decoded pages and the JVM itself may then need more memory than the container allows.
- **Put the spill files on disk.** Spill files go to `java.io.tmpdir` by default, and `parquetry.io.spillDir` moves them. A directory on a memory-backed file system, such as a Kubernetes `emptyDir` volume with `medium: Memory`, counts against the memory limit of the pod ([Change the memory figure](../guides/tune-memory.md#change-the-memory-figure)).
- **Allow native access.** Start the JVM with `--enable-native-access=ALL-UNNAMED`. Parquetry allocates its large buffers as memory mappings, and returns their memory to the system as soon as it frees them. The flag keeps the JDK from warning about them ([Allow native access](../guides/tune-memory.md#allow-native-access)).
- **Size the caches inside the heap.** The tileverse cache settings size the footer and byte-range caches ([Size the caches](../guides/tune-memory.md#size-the-caches)). A store turns its byte-range cache off with `storage.caching.enabled=false`.
- **Override the memory figure only when needed.** `parquetry.io.maxOffHeap`, or the environment variable `PARQUETRY_IO_MAX_OFFHEAP`, replaces the memory beside the heap measured at startup.

## Under the hood

- [Memory](../concepts/memory.md) explains the budgets, their defaults and the caches.
- [Native memory and spill](../concepts/native-memory-and-spill.md) explains the wait and the spill files.
