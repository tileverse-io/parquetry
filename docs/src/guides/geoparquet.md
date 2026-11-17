# Read GeoParquet

You have a GeoParquet file and want its geometries as JTS objects, with the coordinate reference system and the extent of the data. This guide reads the geometries of a file, then its GeoParquet metadata: its geometry columns, their coordinate reference system and their extent.

It needs `parquetry-core`, which includes JTS. [Getting started](../getting-started.md) adds the dependency and downloads the `countries.parquet` file used below.

## Read the geometries

The program reads the names and the geometries of the countries, and decodes the geometry with JTS:

```java
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.geo.MemorySegmentWkbReader;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Geometry;

public class ReadGeometries {

    record Country(String name, Geometry geometry) {}

    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final MemorySegmentWkbReader WKB = new MemorySegmentWkbReader();

    public static void main(String[] args) {
        Path path = Path.of("countries.parquet");
        Projection columns = Projection.ofPhysical(List.of(NAME, GEOMETRY));

        try (ByteRangeSource source = ByteRangeSource.ofFile(path)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<Country> rows =
                    reader.read(Predicate.ALWAYS_TRUE, columns, ReadGeometries::toCountry, ReadOptions.DEFAULTS)) {
                rows.forEach(ReadGeometries::printCountry);
            }
        }
    }

    private static Country toCountry(ParquetSchema schema, ParquetRecord row) {
        String name = row.getString(NAME);
        Geometry geometry = row.readBinary(GEOMETRY, WKB::read);
        return new Country(name, geometry);
    }

    private static void printCountry(Country country) {
        String type = country.geometry().getGeometryType();
        System.out.println(country.name() + ": " + type);
    }
}
```

It prints `Fiji: MultiPolygon`, `Tanzania: Polygon`, and three more lines.

GeoParquet stores a geometry as WKB bytes, the Well-Known Binary encoding of geometries. `readBinary` hands the bytes of the value to `MemorySegmentWkbReader`, which decodes them without an intermediate copy. The WKB reader is thread-safe, and one instance can serve concurrent reads. `readBinary` returns `null` for a null geometry.

To read whole rows instead, pass a `JtsMaterializer` as the materializer: `new JtsMaterializer(reader.schema())`. It returns the rows as `Map<ColumnPath, Object>` instances, with a JTS `Geometry` for each geometry column. It sets the SRID of a geometry when the CRS of its column has an EPSG code, and leaves it at 0 otherwise. The other columns hold the values returned by `ParquetRecord.get`: a string column holds a `MemorySegment`, not a `String`. Such a segment is a view over the decoded batch, valid only while the stream hands you the row, as described in [Read the values of a row](read-records.md#read-the-values-of-a-row). Decode the needed strings before the stream moves on, or read them with `getString` in your own materializer, as in the sample above. The geometries are owned JTS objects, safe to keep.

## Find the geometry columns

A file declares its geometry columns in two ways. A GeoParquet 1.x file lists them in the `geo` document of its key/value metadata. A file with the native Parquet geometry types annotates the columns in its schema, and a GeoParquet 2.0 file does both. `GeometryColumns` reads both declarations:

```java
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;
import java.util.Set;

Set<ColumnPath> geometryColumns = GeometryColumns.resolve(reader.schema(), reader.keyValueMetadata());
```

`reader.schema()` annotates the geometry columns of a GeoParquet 1.x file with the `Geometry` logical type, as a 2.0 file does. The annotation includes the CRS of the column when the file declares one.

## Read the geo document

`GeoParquetMetadata.parse` reads the `geo` document into typed records:

```java
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

String geoJson = reader.keyValueMetadata().get("geo");
GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson);
GeoColumn primary = geo.columns().get(geo.primaryColumn());
System.out.println("Geometry types: " + primary.geometryTypes());
```

For `countries.parquet`, it prints `Geometry types: [Polygon, MultiPolygon]`. The record of the column also holds its encoding, its bounding box, and its `bbox` covering columns when the file has them.

The `crs()` of a column is a `ColumnCrs`, with three cases defined by the GeoParquet specification:

- `ColumnCrs.DEFAULTED`: the document names no CRS, and the column uses the default, OGC:CRS84.
- `ColumnCrs.UNKNOWN`: the document sets the CRS to null. The CRS is unknown, and no default applies.
- `ColumnCrs.Defined`: the document holds the CRS as a PROJJSON document. `projjson()` returns it parsed.

Treat an unknown CRS as unknown. Reading it as OGC:CRS84 would state degrees for coordinates that may be in metres.

## Get the extent

`bounds` returns the bounding box of the primary geometry column over the rows matching a predicate:

```java
import static io.tileverse.parquetry.filter.Pred.col;

import io.tileverse.parquetry.format.BoundingBox;
import java.util.Optional;

Optional<BoundingBox> extent = reader.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
Optional<BoundingBox> africa = reader.bounds(col("continent").eq("Africa"), ReadOptions.DEFAULTS);
```

With a filter, the result is the 2D bounding box of the matching geometries. Like `count`, `bounds` answers from statistics where it can, and elsewhere decodes only the geometry and the predicate columns. Without a filter, the box comes from the metadata of the file when the file records one, and no row group is read. A row group is a block of rows of the file. The result is empty when the file has no geometry column, or when no matching row has a geometry.

## Where to go next

- [Filter by location](spatial-filter.md) selects rows by bounding box and by geometry.
- [Write files](write.md#write-a-geoparquet-file) writes GeoParquet files.

## Under the hood

- [Spatial filtering](../concepts/spatial-filtering.md) explains how spatial predicates prune.
