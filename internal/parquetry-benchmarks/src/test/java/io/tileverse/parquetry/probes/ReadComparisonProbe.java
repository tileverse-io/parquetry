/*
 * Copyright (c) 2026 Multivers.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.parquetry.probes;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.geo.jts.JtsGeometryFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Read-path comparison of parquetry, parquet-java 1.17.0, and DuckDB over one local Parquet file, under attribute and
 * spatial filters, materializing every surviving row.
 *
 * <p>This is a characterization probe, not a JMH microbenchmark: it warms up, runs each engine a few times, and prints
 * a side-by-side table. It exists to answer "how does parquetry behave on the read path with filters, against the other
 * two, on real nested data" rather than to guard against regressions. Point it at a file that has nested columns and a
 * GeoParquet geometry (Overture buildings is the reference dataset; see the module README).
 *
 * <p>All three engines read the same local file, making the comparison apples to apples. parquet-java reads through
 * {@link LocalInputFile} with no Hadoop filesystem. DuckDB runs in-process over JDBC: the measured wall is the full
 * cost a consumer application pays - iterating every {@code ResultSet} row and calling {@code getObject} on every
 * requested column - which is exactly what a row-oriented consumer such as a GeoTools datastore over DuckDB would
 * incur. DuckDB's profiler additionally reports its internal query latency and peak buffer memory; that latency is the
 * engine scan over results DuckDB holds internally, shown for context only, because a JDBC consumer must still
 * materialise every row out of the {@code ResultSet} (the gap between the wall and that latency).
 *
 * <p>Peak heap is read from {@code MemoryPoolMXBean} after each run: it reflects heap occupancy including not-yet-
 * collected garbage at a high {@code -Xmx} rather than the live working set, and should be read as an upper bound. The
 * decisive memory signal is whether a scenario completes at a pod-sized heap (try {@code -Xmx2g}).
 *
 * <p>Each engine materialises the full row (every projected column, nested included) the way a streaming consumer does,
 * without retaining records: parquetry walks each row's values in place (descending struct cells and list-of-struct
 * elements), parquet-java reads each {@code Group}, and DuckDB fetches every column of every {@code ResultSet} row.
 * Row-count parity across the three engines per scenario doubles as a correctness check.
 *
 * <h2>Filter scenarios</h2>
 *
 * <ul>
 *   <li>{@code NO_FILTER}: full scan of the whole file.
 *   <li>{@code ATTRIBUTE}: equality on a low-cardinality dictionary column (default {@code subtype = 'commercial'}).
 *   <li>{@code SPATIAL}: exact geometry intersection with a query diamond. The diamond's bounding box strictly
 *       over-selects, making the exact test non-trivial. parquetry pushes the exact JTS filter into the reader (bbox
 *       pruning then exact gate, the GeoTools/GeoServer path); parquet-java pushes a numeric prefilter on the
 *       {@code bbox} covering columns and re-checks exactness app-side with the same JTS test; DuckDB uses its native
 *       {@code ST_Intersects}.
 *   <li>{@code ATTRIBUTE_AND_SPATIAL}: both filters combined.
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <p>This is a JUnit test controlled by the {@code parquetry.probe.file} system property: it is skipped unless that
 * property points at a Parquet file. Run it directly through the build, giving the test forks enough heap:
 *
 * <pre>{@code
 * ./mvnw -pl :parquetry-benchmarks -am test -Dtest=ReadComparisonProbe \
 *   -Dparquetry.probe.file=/path/to/buildings.parquet \
 *   -DextraArgLine="-Xmx4g"
 * }</pre>
 *
 * <p>Tunable via system properties:
 *
 * <ul>
 *   <li>{@code parquetry.probe.file}: the file path (required; the probe is skipped when unset).
 *   <li>{@code parquetry.probe.engines}: comma-separated engines to run (default {@code parquetry,parquet-java};
 *       {@code duckdb} has no columnar JDBC equivalent here and is ignored).
 *   <li>{@code parquetry.probe.subtype}: the attribute equality value (default {@code commercial}).
 *   <li>{@code parquetry.probe.cx} / {@code .cy} / {@code .r}: query diamond centre and half-diagonal in degrees
 *       (defaults centre San Salvador, {@code -89.2 13.7}, half-diagonal {@code 0.15}).
 *   <li>{@code parquetry.probe.warmup} / {@code .measure}: warmup and measured run counts (defaults {@code 1} /
 *       {@code 3}).
 *   <li>{@code parquetry.probe.concurrency}: number of reads to run at once (default {@code 1}). When greater than
 *       {@code 1} each engine/scenario pass instead launches that many reads simultaneously per wave (DuckDB is
 *       skipped) and the probe reports throughput, latency percentiles, and an OK/OOM/ERROR status, modelling a server
 *       pod that admits {@code 2 x cores} concurrent requests. Pair it with {@code -XX:ActiveProcessorCount=N} to
 *       simulate an N-core pod.
 * </ul>
 */
// S2699: characterization probe; it prints a comparison table and only asserts the run completes for every engine.
@SuppressWarnings("java:S2699")
@EnabledIfSystemProperty(named = "parquetry.probe.file", matches = ".+")
final class ReadComparisonProbe {

    private static final String GEOMETRY = "geometry";
    private static final String SUBTYPE = "subtype";
    private static final ColumnPath GEOMETRY_COL = ColumnPath.of(GEOMETRY);
    private static final ColumnPath SUBTYPE_COL = ColumnPath.of(SUBTYPE);

    private Path file;
    private String subtypeValue;
    private Geometry queryDiamond;
    private Bbox queryEnvelope;
    private int warmupRuns;
    private int measuredRuns;
    private int concurrency;

    /** A sink touched by every consumed value to keep the JIT from eliminating the materialization work. */
    private long sink;

    @Test
    void compareReadPaths() throws Exception {
        configure(Config.fromSystemProperties());
        run();
    }

    /**
     * Honors {@code parquetry.probe.engines} (comma-separated) to run a subset, e.g. just parquetry under a profiler.
     */
    private boolean engineEnabled(String engine) {
        String selected = System.getProperty("parquetry.probe.engines");
        if (selected == null || selected.isBlank()) {
            return true;
        }
        return Arrays.stream(selected.split(",")).map(String::trim).anyMatch(engine::equals);
    }

    /**
     * Honors {@code parquetry.probe.scenarios} (comma-separated names) to run a subset, e.g. just the selective filters
     * under a tight heap where the full scan would not fit.
     */
    private boolean scenarioEnabled(Scenario scenario) {
        String selected = System.getProperty("parquetry.probe.scenarios");
        if (selected == null || selected.isBlank()) {
            return true;
        }
        return Arrays.stream(selected.split(",")).map(String::trim).anyMatch(scenario.name()::equals);
    }

    private void configure(Config config) {
        this.file = config.file();
        this.subtypeValue = config.subtypeValue();
        this.queryDiamond = buildDiamond(config.cx(), config.cy(), config.r());
        this.queryEnvelope = Bbox.of2d(
                config.cx() - config.r(), config.cy() - config.r(), config.cx() + config.r(), config.cy() + config.r());
        this.warmupRuns = config.warmupRuns();
        this.measuredRuns = config.measuredRuns();
        this.concurrency = config.concurrency();
    }

    private void run() throws Exception {
        IO.println("Read-path comparison over %s (%.1f MiB)".formatted(file, Files.size(file) / (1024.0 * 1024.0)));
        IO.println("Attribute filter: %s = '%s'   Spatial filter: intersects %s%n"
                .formatted(SUBTYPE, subtypeValue, queryDiamond));

        if (concurrency > 1) {
            runConcurrent();
        } else {
            runSequential();
        }
        IO.println("%n(consumed checksum %d)".formatted(sink));
    }

    private void runSequential() {
        List<Row> rows = new ArrayList<>();
        for (Scenario scenario : Scenario.values()) {
            if (!scenarioEnabled(scenario)) {
                continue;
            }
            if (engineEnabled("parquetry")) {
                rows.add(measure("parquetry", scenario, () -> readParquetry(scenario)));
            }
            if (engineEnabled("parquet-java")) {
                rows.add(measure("parquet-java", scenario, () -> readParquetJava(scenario)));
            }
            if (engineEnabled("duckdb")) {
                rows.add(measureDuckDb(scenario));
            }
        }
        printTable(rows);
    }

    /**
     * Runs each engine/scenario pass at the configured concurrency. DuckDB is skipped: its JDBC arm shares one in-
     * process connection and its native engine parallelises internally, which would not measure the same thing as
     * independent concurrent reads. parquetry shares one open dataset across the concurrent reads (the long-lived
     * server model); parquet-java opens a reader per read (its per-query model).
     */
    private void runConcurrent() {
        IO.println(
                "Concurrency: %d reads in flight per engine/scenario pass (DuckDB skipped)%n".formatted(concurrency));
        List<ConcurrentRow> rows = new ArrayList<>();
        for (Scenario scenario : Scenario.values()) {
            if (!scenarioEnabled(scenario)) {
                continue;
            }
            if (engineEnabled("parquetry")) {
                rows.add(measureParquetryConcurrent(scenario));
            }
            if (engineEnabled("parquet-java")) {
                rows.add(measureParquetJavaConcurrent(scenario));
            }
        }
        printConcurrentTable(rows);
    }

    private ConcurrentRow measureParquetryConcurrent(Scenario scenario) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            ProbeMeasurement.Outcome outcome = ProbeMeasurement.runConcurrent(
                    concurrency, warmupRuns, measuredRuns, () -> readParquetry(dataset, scenario));
            return new ConcurrentRow("parquetry", scenario, outcome, null);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return ConcurrentRow.failed("parquetry", scenario, "interrupted");
        } catch (RuntimeException e) {
            return ConcurrentRow.failed("parquetry", scenario, e.getMessage());
        }
    }

    private ConcurrentRow measureParquetJavaConcurrent(Scenario scenario) {
        try {
            ProbeMeasurement.Outcome outcome = ProbeMeasurement.runConcurrent(
                    concurrency, warmupRuns, measuredRuns, () -> readParquetJava(scenario));
            return new ConcurrentRow("parquet-java", scenario, outcome, null);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return ConcurrentRow.failed("parquet-java", scenario, "interrupted");
        }
    }

    // --- parquetry arm ---

    private long readParquetry(Scenario scenario) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return readParquetry(ParquetDataset.open(source), scenario);
        }
    }

    private long readParquetry(ParquetDataset dataset, Scenario scenario) {
        Predicate predicate = parquetryPredicate(scenario);
        try (Stream<ParquetRecord> records = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            long count = 0L;
            for (ParquetRecord rec : (Iterable<ParquetRecord>) records::iterator) {
                consumeParquetry(rec);
                count++;
            }
            return count;
        }
    }

    private Predicate parquetryPredicate(Scenario scenario) {
        Predicate attribute = new Predicate.Eq(SUBTYPE_COL, new Value.StringVal(subtypeValue));
        Predicate spatial = Predicate.geometryFilter(JtsGeometryFilter.intersects(GEOMETRY_COL, queryDiamond));
        return switch (scenario) {
            case NO_FILTER -> Predicate.ALWAYS_TRUE;
            case ATTRIBUTE -> attribute;
            case SPATIAL -> spatial;
            case ATTRIBUTE_AND_SPATIAL -> new Predicate.And(List.of(attribute, spatial));
        };
    }

    /**
     * Reads every value of the row in place, mirroring a streaming consumer (e.g. a GeoTools datastore) that pulls each
     * attribute as it iterates and never retains the record. No {@code detach()}: that copy is only for callers that
     * hold a record past its batch, which a streaming reader does not do.
     */
    private void consumeParquetry(ParquetRecord rec) {
        walkRecord(rec, rec.schema().root());
    }

    /**
     * Reads each top-level field of {@code group} from {@code record}, descending nested values by their runtime kind.
     */
    private void walkRecord(ParquetRecord rec, SchemaNode.Group group) {
        for (SchemaNode child : group.children()) {
            touchValue(rec.get(ColumnPath.of(child.name())), child);
        }
    }

    /**
     * Forces the value to materialise fully. The kind comes from what {@code get()} returns - a {@link ParquetRecord}
     * for a struct cell, a {@link List} for a list, a {@link Map} for a map, otherwise a scalar - and {@code node} is
     * the matching schema node used only to descend struct and list-element records.
     */
    private void touchValue(Object value, SchemaNode node) {
        switch (value) {
            case null -> {
                // null cell, nothing to materialise
            }
            case ParquetRecord nested -> walkRecord(nested, (SchemaNode.Group) node);
            case List<?> list -> {
                SchemaNode element = listElement((SchemaNode.Group) node);
                for (Object item : list) {
                    touchValue(item, element);
                }
            }
            case Map<?, ?> map ->
                map.forEach((key, mapped) -> {
                    sink += scalarHash(key);
                    sink += scalarHash(mapped);
                });
            case MemorySegment segment -> sink += segment.byteSize();
            default -> sink += value.hashCode();
        }
    }

    private static long scalarHash(Object value) {
        if (value instanceof MemorySegment segment) {
            return segment.byteSize();
        }
        return value == null ? 0L : value.hashCode();
    }

    /** The element node of a 3-level LIST group ({@code list -> element}), falling back to a 2-level repeated child. */
    private static SchemaNode listElement(SchemaNode.Group listGroup) {
        SchemaNode middle = listGroup.children().get(0);
        if (middle instanceof SchemaNode.Group repeated) {
            return repeated.children().get(0);
        }
        return middle;
    }

    // --- parquet-java arm ---

    private long readParquetJava(Scenario scenario) throws IOException {
        FilterCompat.Filter filter = parquetJavaFilter(scenario);
        boolean exactGateNeeded = scenario == Scenario.SPATIAL || scenario == Scenario.ATTRIBUTE_AND_SPATIAL;
        JtsGeometryFilter exactGate = JtsGeometryFilter.intersects(GEOMETRY_COL, queryDiamond);
        try (ParquetReader<Group> reader = groupReader(filter)) {
            long count = 0L;
            Group group = reader.read();
            while (group != null) {
                if (!exactGateNeeded || passesExactGate(group, exactGate)) {
                    consumeGroup(group);
                    count++;
                }
                group = reader.read();
            }
            return count;
        }
    }

    private ParquetReader<Group> groupReader(FilterCompat.Filter filter) throws IOException {
        ParquetReader.Builder<Group> builder = new ParquetReader.Builder<>(new LocalInputFile(file)) {
            @Override
            protected ReadSupport<Group> getReadSupport() {
                return new GroupReadSupport();
            }
        };
        if (filter != FilterCompat.NOOP) {
            builder = builder.withFilter(filter);
        }
        return builder.build();
    }

    /**
     * parquet-java has no spatial concept: the attribute filter pushes an equality predicate, and the spatial filter
     * pushes a numeric intersection test on the {@code bbox} covering columns (row-group and page pruning), with the
     * exact geometry test applied app-side. {@code NO_FILTER} and the spatial-only numeric prefilter return
     * {@code NOOP} and the bbox predicate respectively.
     */
    private FilterCompat.Filter parquetJavaFilter(Scenario scenario) {
        FilterPredicate attribute = FilterApi.eq(FilterApi.binaryColumn(SUBTYPE), Binary.fromString(subtypeValue));
        FilterPredicate bboxPrefilter = bboxIntersectsPredicate();
        return switch (scenario) {
            case NO_FILTER -> FilterCompat.NOOP;
            case ATTRIBUTE -> FilterCompat.get(attribute);
            case SPATIAL -> FilterCompat.get(bboxPrefilter);
            case ATTRIBUTE_AND_SPATIAL -> FilterCompat.get(FilterApi.and(attribute, bboxPrefilter));
        };
    }

    /**
     * A row's bbox [xmin,xmax,ymin,ymax] overlaps the query envelope iff {@code xmin <= qMaxX and xmax >= qMinX and
     * ymin <= qMaxY and ymax >= qMinY}. This is the lowering parquetry's spatial covering rewrite does internally.
     */
    private FilterPredicate bboxIntersectsPredicate() {
        FilterPredicate xOverlap = FilterApi.and(
                FilterApi.ltEq(FilterApi.doubleColumn("bbox.xmin"), queryEnvelope.maxX()),
                FilterApi.gtEq(FilterApi.doubleColumn("bbox.xmax"), queryEnvelope.minX()));
        FilterPredicate yOverlap = FilterApi.and(
                FilterApi.ltEq(FilterApi.doubleColumn("bbox.ymin"), queryEnvelope.maxY()),
                FilterApi.gtEq(FilterApi.doubleColumn("bbox.ymax"), queryEnvelope.minY()));
        return FilterApi.and(xOverlap, yOverlap);
    }

    private boolean passesExactGate(Group group, JtsGeometryFilter exactGate) {
        Binary wkb = group.getBinary(GEOMETRY, 0);
        MemorySegment segment = MemorySegment.ofArray(wkb.getBytes()).asReadOnly();
        return exactGate.gate(segment).isPresent();
    }

    /** Recursively reads every value of {@code group}, materialising each primitive, and folds it into the sink. */
    private void consumeGroup(Group group) {
        GroupType type = group.getType();
        for (int field = 0; field < type.getFieldCount(); field++) {
            int repetitions = group.getFieldRepetitionCount(field);
            Type fieldType = type.getType(field);
            for (int i = 0; i < repetitions; i++) {
                if (fieldType.isPrimitive()) {
                    consumePrimitive(
                            group, field, i, fieldType.asPrimitiveType().getPrimitiveTypeName());
                } else {
                    consumeGroup(group.getGroup(field, i));
                }
            }
        }
    }

    private void consumePrimitive(Group group, int field, int index, PrimitiveTypeName kind) {
        switch (kind) {
            case BOOLEAN -> sink += group.getBoolean(field, index) ? 1 : 0;
            case INT32 -> sink += group.getInteger(field, index);
            case INT64 -> sink += group.getLong(field, index);
            case FLOAT -> sink += (long) group.getFloat(field, index);
            case DOUBLE -> sink += (long) group.getDouble(field, index);
            case INT96, BINARY, FIXED_LEN_BYTE_ARRAY ->
                sink += group.getBinary(field, index).length();
        }
    }

    // --- DuckDB arm ---

    private Row measureDuckDb(Scenario scenario) {
        boolean spatial = scenario == Scenario.SPATIAL || scenario == Scenario.ATTRIBUTE_AND_SPATIAL;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            if (spatial && !loadSpatial(connection)) {
                return Row.skipped("duckdb", scenario, "spatial extension unavailable");
            }
            return runDuckDb(connection, scenario);
        } catch (SQLException e) {
            return Row.skipped("duckdb", scenario, e.getMessage());
        }
    }

    private boolean loadSpatial(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSTALL spatial");
            statement.execute("LOAD spatial");
            return true;
        } catch (SQLException _) {
            return false;
        }
    }

    private Row runDuckDb(Connection connection, Scenario scenario) throws SQLException {
        String sql = duckDbQuery(scenario);
        Path profile;
        try {
            profile = Files.createTempFile("parquetry-probe-duckdb-", ".json");
        } catch (IOException _) {
            return Row.skipped("duckdb", scenario, "cannot create profile file");
        }
        enableProfiling(connection, profile);
        for (int i = 0; i < warmupRuns; i++) {
            executeAndConsume(connection, sql);
        }
        List<Long> wallNanos = new ArrayList<>(measuredRuns);
        long rows = 0L;
        long allocBytes = 0L;
        for (int i = 0; i < measuredRuns; i++) {
            long allocBefore = ProbeMeasurement.totalAllocatedBytes();
            long start = System.nanoTime();
            rows = executeAndConsume(connection, sql);
            wallNanos.add(System.nanoTime() - start);
            allocBytes = ProbeMeasurement.totalAllocatedBytes() - allocBefore;
        }
        DuckDbProfile parsed = DuckDbProfile.read(profile);
        return Row.duckDb(scenario, rows, ProbeMeasurement.medianMillis(wallNanos), allocBytes, parsed);
    }

    private void enableProfiling(Connection connection, Path profile) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA enable_profiling='json'");
            statement.execute("PRAGMA profiling_output='" + sqlLiteral(profile.toString()) + "'");
        }
    }

    private long executeAndConsume(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData meta = resultSet.getMetaData();
            int columns = meta.getColumnCount();
            long rows = 0L;
            while (resultSet.next()) {
                for (int column = 1; column <= columns; column++) {
                    Object value = resultSet.getObject(column);
                    if (value != null) {
                        sink += value.hashCode();
                    }
                }
                rows++;
            }
            return rows;
        }
    }

    private String duckDbQuery(Scenario scenario) {
        String source = "read_parquet('" + sqlLiteral(file.toAbsolutePath().toString()) + "')";
        String attribute = SUBTYPE + " = '" + sqlLiteral(subtypeValue) + "'";
        String spatial = "ST_Intersects(" + GEOMETRY + ", ST_GeomFromText('" + diamondWkt() + "'))";
        String where =
                switch (scenario) {
                    case NO_FILTER -> "";
                    case ATTRIBUTE -> " WHERE " + attribute;
                    case SPATIAL -> " WHERE " + spatial;
                    case ATTRIBUTE_AND_SPATIAL -> " WHERE " + attribute + " AND " + spatial;
                };
        return "SELECT * FROM " + source + where;
    }

    // --- timing + measurement ---

    private Row measure(String engine, Scenario scenario, ThrowingLongSupplier body) {
        try {
            for (int i = 0; i < warmupRuns; i++) {
                body.getAsLong();
            }
            List<Long> wallNanos = new ArrayList<>(measuredRuns);
            long rows = 0L;
            long peakHeapBytes = 0L;
            long allocBytes = 0L;
            for (int i = 0; i < measuredRuns; i++) {
                ProbeMeasurement.resetPeakHeap();
                long allocBefore = ProbeMeasurement.totalAllocatedBytes();
                long start = System.nanoTime();
                rows = body.getAsLong();
                wallNanos.add(System.nanoTime() - start);
                allocBytes = ProbeMeasurement.totalAllocatedBytes() - allocBefore;
                peakHeapBytes = Math.max(peakHeapBytes, ProbeMeasurement.peakHeapBytes());
            }
            return Row.jvm(engine, scenario, rows, ProbeMeasurement.medianMillis(wallNanos), peakHeapBytes, allocBytes);
        } catch (Exception e) {
            System.err.printf("%n[%s / %s] failed:%n", engine, scenario);
            e.printStackTrace();
            return Row.skipped(engine, scenario, e.getMessage());
        }
    }

    // --- query geometry ---

    private static Geometry buildDiamond(double cx, double cy, double r) {
        GeometryFactory factory = new GeometryFactory();
        Coordinate top = new Coordinate(cx, cy + r);
        Coordinate right = new Coordinate(cx + r, cy);
        Coordinate bottom = new Coordinate(cx, cy - r);
        Coordinate left = new Coordinate(cx - r, cy);
        Coordinate[] ring = {top, right, bottom, left, top};
        return factory.createPolygon(ring);
    }

    private String diamondWkt() {
        Coordinate[] ring = queryDiamond.getCoordinates();
        StringBuilder wkt = new StringBuilder("POLYGON((");
        for (int i = 0; i < ring.length; i++) {
            if (i > 0) {
                wkt.append(", ");
            }
            wkt.append(ring[i].x).append(' ').append(ring[i].y);
        }
        return wkt.append("))").toString();
    }

    private static String sqlLiteral(String raw) {
        return raw.replace("'", "''");
    }

    // --- output ---

    private static void printTable(List<Row> rows) {
        String header = String.format(
                "%-22s %-12s %12s %12s %12s %12s %12s %12s",
                "scenario", "engine", "rows", "wall(ms)", "alloc(MB)", "peakHeap(MB)", "duckScan(ms)", "duckMem(MB)");
        IO.println(header);
        IO.println("-".repeat(header.length()));
        for (Row row : rows) {
            IO.println(row.format());
        }
        IO.println();
        IO.println("wall(ms) is the consumer cost: every row materialised, every requested column read out");
        IO.println(
                "  (ResultSet.getObject per column for DuckDB, in-place value walk for parquetry, Group for parquet-java).");
        IO.println("alloc(MB) is heap allocated by all threads during the run (-Xmx-independent churn); for");
        IO.println("  DuckDB it is the JDBC consumer's per-row boxing, while duckMem is DuckDB's native buffer.");
        IO.println("peakHeap(MB) is heap occupancy incl. uncollected garbage at the run's -Xmx; an upper bound.");
        IO.println("duckScan(ms) is DuckDB's internal engine scan from its profiler - context only; a JDBC consumer");
        IO.println("  still pays the full wall to pull rows out, which is not what duckScan reflects.");
        IO.println("duckMem(MB) is DuckDB's self-reported peak buffer memory; its native footprint is off-heap.");
    }

    private void printConcurrentTable(List<ConcurrentRow> rows) {
        String header = String.format(
                "%-22s %-12s %8s %12s %10s %10s %10s %12s %12s %8s",
                "scenario",
                "engine",
                "rows",
                "req/s",
                "p50(ms)",
                "p95(ms)",
                "max(ms)",
                "peakHeap(MB)",
                "alloc(MB)",
                "status");
        IO.println(header);
        IO.println("-".repeat(header.length()));
        for (ConcurrentRow row : rows) {
            IO.println(row.format());
        }
        IO.println();
        IO.println("All passes ran %d reads at once, released together; metrics aggregate every read."
                .formatted(concurrency));
        IO.println("req/s is measured reads / summed wave wall; p50/p95/max are per-read latencies.");
        IO.println(
                "peakHeap(MB) is heap occupancy incl. uncollected garbage at the run's -Xmx; the decisive signal is");
        IO.println("  whether status stays OK (not OOM) at a pod-sized heap. alloc(MB) is total churn.");
    }

    // --- supporting types ---

    private enum Scenario {
        NO_FILTER,
        ATTRIBUTE,
        SPATIAL,
        ATTRIBUTE_AND_SPATIAL
    }

    @FunctionalInterface
    private interface ThrowingLongSupplier {
        long getAsLong() throws Exception;
    }

    /** One printed line: an engine's result for one scenario. */
    private record Row(
            String engine,
            Scenario scenario,
            long rows,
            double wallMillis,
            long peakHeapBytes,
            long allocBytes,
            DuckDbProfile duck,
            String skipReason) {

        static Row jvm(
                String engine, Scenario scenario, long rows, double wallMillis, long peakHeapBytes, long allocBytes) {
            return new Row(engine, scenario, rows, wallMillis, peakHeapBytes, allocBytes, null, null);
        }

        static Row duckDb(Scenario scenario, long rows, double wallMillis, long allocBytes, DuckDbProfile duck) {
            return new Row("duckdb", scenario, rows, wallMillis, 0L, allocBytes, duck, null);
        }

        static Row skipped(String engine, Scenario scenario, String reason) {
            return new Row(engine, scenario, 0L, 0.0, 0L, 0L, null, reason);
        }

        String format() {
            if (skipReason != null) {
                return String.format("%-22s %-12s   skipped: %s", scenario, engine, skipReason);
            }
            String alloc = allocBytes > 0 ? String.format("%.1f", allocBytes / (1024.0 * 1024.0)) : "-";
            String peakHeap = peakHeapBytes > 0 ? String.format("%.1f", peakHeapBytes / (1024.0 * 1024.0)) : "-";
            String duckScan = duck != null ? String.format("%.1f", duck.latencySeconds() * 1000.0) : "-";
            String duckMem = duck != null ? String.format("%.1f", duck.peakBufferBytes() / (1024.0 * 1024.0)) : "-";
            return String.format(
                    "%-22s %-12s %12d %12.1f %12s %12s %12s %12s",
                    scenario, engine, rows, wallMillis, alloc, peakHeap, duckScan, duckMem);
        }
    }

    /** One printed line of the concurrency table: an engine's aggregate result for one scenario. */
    private record ConcurrentRow(
            String engine, Scenario scenario, ProbeMeasurement.Outcome outcome, String skipReason) {

        static ConcurrentRow failed(String engine, Scenario scenario, String reason) {
            return new ConcurrentRow(engine, scenario, null, reason);
        }

        String format() {
            if (skipReason != null) {
                return String.format("%-22s %-12s   skipped: %s", scenario, engine, skipReason);
            }
            String peakHeap = String.format("%.1f", outcome.peakHeapBytes() / (1024.0 * 1024.0));
            String alloc = String.format("%.1f", outcome.allocBytes() / (1024.0 * 1024.0));
            return String.format(
                    "%-22s %-12s %8d %12.1f %10.1f %10.1f %10.1f %12s %12s %8s",
                    scenario,
                    engine,
                    outcome.rowsPerRead(),
                    outcome.throughputPerSecond(),
                    outcome.p50Millis(),
                    outcome.p95Millis(),
                    outcome.maxMillis(),
                    peakHeap,
                    alloc,
                    statusLabel());
        }

        /** OK, or the failure kind annotated with how many reads failed. */
        private String statusLabel() {
            if (outcome.status() == ProbeMeasurement.Status.OK) {
                return "OK";
            }
            return String.format("%s(%d failed)", outcome.status(), outcome.failed());
        }
    }

    /** The subset of DuckDB's profile JSON this probe reports. */
    private record DuckDbProfile(double latencySeconds, long peakBufferBytes) {

        static DuckDbProfile read(Path profile) {
            try {
                String json = Files.readString(profile);
                return new DuckDbProfile(
                        extractNumber(json, "latency"), (long) extractNumber(json, "system_peak_buffer_memory"));
            } catch (IOException _) {
                return new DuckDbProfile(0.0, 0L);
            }
        }

        /** Reads the numeric value of the first top-level {@code "key": <number>} occurrence. */
        private static double extractNumber(String json, String key) {
            String needle = "\"" + key + "\":";
            int at = json.indexOf(needle);
            if (at < 0) {
                return 0.0;
            }
            int start = at + needle.length();
            while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
                start++;
            }
            int end = start;
            while (end < json.length() && "0123456789+-.eE".indexOf(json.charAt(end)) >= 0) {
                end++;
            }
            String number = json.substring(start, end);
            return number.isEmpty() ? 0.0 : Double.parseDouble(number);
        }
    }

    /** Resolved probe inputs. */
    private record Config(
            Path file,
            String subtypeValue,
            double cx,
            double cy,
            double r,
            int warmupRuns,
            int measuredRuns,
            int concurrency) {

        static Config fromSystemProperties() {
            String path = System.getProperty("parquetry.probe.file");
            Path file = Path.of(path);
            if (!Files.isRegularFile(file)) {
                throw new IllegalArgumentException("parquetry.probe.file is not a regular file: " + path);
            }
            return new Config(
                    file,
                    System.getProperty("parquetry.probe.subtype", "commercial"),
                    doubleProperty("parquetry.probe.cx", -89.2),
                    doubleProperty("parquetry.probe.cy", 13.7),
                    doubleProperty("parquetry.probe.r", 0.15),
                    intProperty("parquetry.probe.warmup", 1),
                    intProperty("parquetry.probe.measure", 3),
                    intProperty("parquetry.probe.concurrency", 1));
        }

        private static double doubleProperty(String key, double fallback) {
            String value = System.getProperty(key);
            return value == null ? fallback : Double.parseDouble(value);
        }

        private static int intProperty(String key, int fallback) {
            String value = System.getProperty(key);
            return value == null ? fallback : Integer.parseInt(value);
        }
    }
}
