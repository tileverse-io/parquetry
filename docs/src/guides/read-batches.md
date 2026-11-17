# Read columnar batches

You process data a column at a time, for example to sum the values of a column, and need no row objects. `readBatches` returns the matching rows of a file as batches of column vectors. This guide sums a column of the matching rows.

It needs `parquetry-core`. [Getting started](../getting-started.md) adds the dependency and downloads the `countries.parquet` file used below.

## Sum a column

The program adds up the population of the African countries of the file:

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class SumPopulation {

    private static final ColumnPath POPULATION = ColumnPath.of("pop_est");

    public static void main(String[] args) {
        Path path = Path.of("countries.parquet");
        Predicate filter = col("continent").eq("Africa");
        Projection columns = Projection.ofPhysical(List.of(POPULATION));

        try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecordBatch> batches = reader.readBatches(filter, columns, ReadOptions.DEFAULTS)) {
                double total = batches.mapToDouble(SumPopulation::sumPopulation).sum();
                System.out.println("Population of Africa: " + total);
            }
        }
    }

    private static double sumPopulation(ParquetRecordBatch batch) {
        try (batch) {
            DoubleVector population = (DoubleVector) batch.columns().get(POPULATION);
            double sum = 0;
            for (int row = 0; row < batch.rowCount(); row++) {
                if (population.isValid(row)) {
                    sum += population.getDouble(row);
                }
            }
            return sum;
        }
    }
}
```

It prints `Population of Africa: 5.8608716E7`.

`readBatches` takes the same arguments as `read`: a predicate, a projection and the read options. It applies the predicate exactly. A batch holds only the matching rows, and only the projected columns.

## Read the vectors

`batch.columns()` maps the projected column paths to their `ColumnVector`. The type of the vector follows the physical type of the column:

| Parquet type | Vector | Value accessor |
| :--- | :--- | :--- |
| `INT32` | `IntVector` | `getInt(row)` |
| `INT64` | `LongVector` | `getLong(row)` |
| `FLOAT` | `FloatVector` | `getFloat(row)` |
| `DOUBLE` | `DoubleVector` | `getDouble(row)` |
| `BOOLEAN` | `BooleanVector` | `getBoolean(row)` |
| `BYTE_ARRAY`, strings included | `BinaryVector` | `get(row)`, a `MemorySegment` |
| struct, list, map | `StructVector`, `ListVector`, `MapVector` | `batch.materialize(row)` |

A typed accessor throws on a null row: check `isNull(row)` or `isValid(row)` first, or use `get(row)`, which returns `null` for a null row. `batch.materialize(row)` returns the row as a `ParquetRecord`, with the accessors described in [Read records](read-records.md#read-the-values-of-a-row).

## Close each batch

Close a batch when you are done with it. Closing releases its off-heap buffers, and a vector backed by them is unreadable afterwards. Read or copy the needed values before closing the batch. Close the stream as well: closing it stops the read. [Tune memory](tune-memory.md) sizes the memory held by the reads.

## Options that matter

- **Batch size.** A batch holds at most the rows of one page by default. A page is a run of values of one column, stored and compressed as a unit. `ReadOptions.builder().batchSize(1024).build()` caps it at 1,024 rows.
- **Filter switches.** The pruning switches of [Filter rows](filter.md#turn-a-tier-off) apply to `readBatches` as they do to `read`.

## Where to go next

- [Arrow interop](arrow.md) exports the batches of a read as an Arrow IPC stream or through the Arrow C Data Interface.
- [Write files](write.md#write-batches) writes batches to a new file.

## Under the hood

- [The read path](../concepts/read-path.md#6-the-masked-scan) explains how a read decodes only the rows of a batch.
