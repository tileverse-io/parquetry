# Filter by location

You have a GeoParquet file and want the features in an area, such as the map window of a request. A read given a bounding box returns the rows whose geometry has a bounding box overlapping it, and skips the parts of the file whose recorded bounds lie outside it. An exact geometry test, such as a true intersection with a polygon, can run in the same read.

It needs `parquetry-core`, which includes JTS. [Getting started](../getting-started.md) adds the dependency and downloads the `countries.parquet` file used below.

## Filter by bounding box

The program prints the countries whose bounding box overlaps a box around North America:

```java
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class FindByBox {

    private static final ColumnPath NAME = ColumnPath.of("name");

    public static void main(String[] args) {
        Path path = Path.of("countries.parquet");
        Bbox northAmerica = Bbox.of2d(-130, 20, -60, 55);
        Predicate filter = Pred.col("geometry").bboxIntersects(northAmerica);
        Projection columns = Projection.ofPhysical(List.of(NAME));

        try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(filter, columns, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> System.out.println(row.getString(NAME)));
            }
        }
    }
}
```

It prints `Canada` and `United States of America`.

`Bbox.of2d(minX, minY, maxX, maxY)` builds the query box, in the coordinates of the file. Four relations compare the bounding boxes of the geometries with the query box:

| Builder | Keeps a row when the bounding box of its geometry ... |
| :--- | :--- |
| `bboxIntersects(box)` | overlaps the query box |
| `bboxContains(box)` | encloses the query box |
| `bboxCoveredBy(box)` | lies inside the query box |
| `bboxEquals(box)` | equals the query box |

A bounding box relation is a predicate like the others. It combines with attribute predicates through `and`, `or` and `negate`, and `count` takes it as well. The geometry column does not have to be in the projection.

A row with a null geometry matches neither a relation nor its negation. [Null geometries](../concepts/spatial-filtering.md#5-null-geometries) explains why.

## Apply an exact geometry test

A bounding box relation answers whether two shapes could interact. For the exact answer, push a geometry filter into the read. `JtsGeometryFilter` tests the geometries with JTS:

```java
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

GeometryFactory geometries = new GeometryFactory();
Point kansas = geometries.createPoint(new Coordinate(-98.5, 39.5));
JtsGeometryFilter containsKansas = JtsGeometryFilter.intersects(ColumnPath.of("geometry"), kansas);
Predicate filter = Predicate.geometryFilter(containsKansas);
```

With this filter, the program of the previous section prints `United States of America` alone. A Parquet file stores its rows in blocks called row groups, and the columns of a row group in smaller runs of values called pages. The reader prunes row groups and pages with a bounding box relation derived from the query, then runs the exact test on the remaining rows.

`JtsGeometryFilter` has a factory for each relation: `intersects`, `touches`, `crosses`, `overlaps`, `contains`, `covers`, `within`, `coveredBy`, `disjoint`, `equalsExact`, and `dwithin(column, query, distance)`. The query geometry must be in the coordinate reference system of the file, because the filter does not reproject. `disjoint` has no bounding box to prune with, and tests the rows of the whole file.

The [GeoTools data store](../geoserver/geotools-datastore.md#filters-pushed-into-the-read) pushes the geometry filters of GeoServer requests into the read this way.

The other way to apply the exact test is to filter by the bounding box of the query geometry in the read, and to test the returned rows in your own code, with your own geometry library.

## Make files that filter well

A spatial filter skips row groups and pages only when the file records where its geometries are:

- A file with `bbox` covering columns, four columns holding the bounding box of each geometry, prunes row groups, and pages when the file has a page index. Parquetry's writer adds both by default, as described in [Write a GeoParquet file](write.md#write-a-geoparquet-file).
- A GeoParquet 2.0 file with geometry statistics and without covering columns prunes row groups only.
- A file with neither prunes nothing. The read tests the geometries one by one, and the result is still exact.

Pruning pays off when nearby geometries sit in the same row groups and pages. Sort the rows by location, for example along a space-filling curve, before writing them. [Which path applies](../concepts/spatial-filtering.md#4-which-path-applies) details the three cases, and [Lay out files for selective queries](file-layout.md) shows how to write such a file.

## Where to go next

- [Read GeoParquet](geoparquet.md) reads the geometries as JTS objects and the extent of a filter.
- [Filter rows](filter.md) covers attribute predicates.

## Under the hood

- [Spatial filtering](../concepts/spatial-filtering.md) explains how a spatial predicate prunes and tests rows.
