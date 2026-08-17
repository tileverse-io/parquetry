/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.stac;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Self-check for the item-table fixture: the file it writes must have the stac-geoparquet shape the reader tests rely
 * on - nested bbox struct, per-key asset structs with an alternate href sub-struct, a timestamp datetime, and the
 * item-table metadata under the {@code stac-geoparquet} footer key.
 */
class StacItemTableParquetTest {

    @Test
    void writesTheSpecShape(@TempDir Path dir) throws Exception {
        StacItemTableParquet.Asset data = StacItemTableParquet.Asset.data("parts/west.parquet");
        StacItemTableParquet.Asset extra = new StacItemTableParquet.Asset(
                "extra",
                "parts/west-extra.parquet",
                "application/vnd.apache.parquet",
                "Extra",
                List.of("collection-mirror"),
                "s3://bucket/west.parquet");
        StacItemTableParquet.Item west = new StacItemTableParquet.Item(
                "west", "building", 0.0, 0.0, 10.0, 10.0, "2026-07-22T00:00:00Z", List.of(data, extra));

        Path file = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(west),
                StacItemTableParquet.Options.defaults().withKv("{\"version\": \"1.0.0\"}"));

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            assertThat(source.keyValueMetadata()).containsEntry("stac-geoparquet", "{\"version\": \"1.0.0\"}");
            assertThat(source.schema().find(ColumnPath.of("assets", "extra", "alternate", "s3", "href")))
                    .isPresent();
            try (Stream<ParquetRecord> rows = source.read()) {
                ParquetRecord row = rows.findFirst().orElseThrow();
                assertThat(row.getString(ColumnPath.of("id"))).isEqualTo("west");
                assertThat(row.getDouble(ColumnPath.of("bbox", "xmax"))).isEqualTo(10.0);
                assertThat(row.getString(ColumnPath.of("assets", "data", "href")))
                        .isEqualTo("parts/west.parquet");
                assertThat(row.getString(ColumnPath.of("assets", "extra", "alternate", "s3", "href")))
                        .isEqualTo("s3://bucket/west.parquet");
                assertThat(row.getString(ColumnPath.of("collection"))).isEqualTo("building");
                assertThat(row.getLong(ColumnPath.of("datetime"))).isEqualTo(1_784_678_400_000_000L);
            }
        }
    }

    @Test
    void writesNullsForAbsentValuesAndDropsDeselectedColumns(@TempDir Path dir) throws Exception {
        StacItemTableParquet.Asset data = StacItemTableParquet.Asset.data("parts/west.parquet");
        StacItemTableParquet.Asset thumbnail = new StacItemTableParquet.Asset(
                "thumbnail", "parts/east.png", "image/png", "Thumb", List.of("thumbnail", "overview"), null);
        StacItemTableParquet.Item west = new StacItemTableParquet.Item(
                "west", "building", 0.0, 0.0, 10.0, 10.0, "2026-07-22T00:00:00Z", List.of(data));
        StacItemTableParquet.Item sparse =
                new StacItemTableParquet.Item(null, "building", null, null, null, null, null, List.of(thumbnail));

        Path file = StacItemTableParquet.write(
                dir.resolve("sparse.parquet"),
                List.of(west, sparse),
                StacItemTableParquet.Options.defaults().withoutCollectionColumn());

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            assertThat(source.schema().find(ColumnPath.of("collection"))).isEmpty();

            try (Stream<ParquetRecord> stream = source.read()) {
                Iterator<ParquetRecord> rows = stream.iterator();

                ParquetRecord first = rows.next();
                assertThat(first.getString(ColumnPath.of("assets", "data", "href")))
                        .isEqualTo("parts/west.parquet");
                assertThat(first.get(ColumnPath.of("assets", "thumbnail")))
                        .as("an asset key another item declares is null here")
                        .isNull();

                ParquetRecord second = rows.next();
                assertThat(second.isNull(ColumnPath.of("id"))).isTrue();
                assertThat(second.isNull(ColumnPath.of("datetime"))).isTrue();
                assertThat(second.get(ColumnPath.of("bbox"))).isNull();
                assertThat(second.get(ColumnPath.of("geometry"))).isNull();
                assertThat(second.get(ColumnPath.of("assets", "data"))).isNull();
                assertThat(second.getString(ColumnPath.of("assets", "thumbnail", "href")))
                        .isEqualTo("parts/east.png");
                assertThat(strings(second.get(ColumnPath.of("assets", "thumbnail", "roles"))))
                        .containsExactly("thumbnail", "overview");
                assertThat(second.get(ColumnPath.of("assets", "thumbnail", "alternate")))
                        .isNull();
            }
        }
    }

    @Test
    void writesTheBboxCornersWithTheRequestedPhysicalType(@TempDir Path dir) throws Exception {
        StacItemTableParquet.Item west =
                new StacItemTableParquet.Item("west", "building", 0.5, 0.0, 10.5, 10.0, null, List.of());

        Path file = StacItemTableParquet.write(
                dir.resolve("float-bbox.parquet"),
                List.of(west),
                StacItemTableParquet.Options.defaults().withBboxCornerKind(PrimitiveKind.FLOAT));

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            assertThat(source.schema().find(ColumnPath.of("bbox", "xmin")).orElseThrow())
                    .isInstanceOfSatisfying(
                            SchemaNode.Primitive.class,
                            corner -> assertThat(corner.kind()).isEqualTo(PrimitiveKind.FLOAT));
            assertThat(source.schema().find(ColumnPath.of("assets")))
                    .as("an item declaring no asset leaves the file without the column")
                    .isEmpty();
            try (Stream<ParquetRecord> rows = source.read()) {
                assertThat(rows.findFirst().orElseThrow().getFloat(ColumnPath.of("bbox", "xmax")))
                        .isEqualTo(10.5f);
            }
        }
    }

    @Test
    void rejectsAPartiallySpecifiedBbox() {
        List<StacItemTableParquet.Asset> noAssets = List.of();

        assertThatThrownBy(() -> new StacItemTableParquet.Item(
                        "west", "building", 0.0, 0.0, 10.0, null, "2026-07-22T00:00:00Z", noAssets))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("west");
    }

    private static List<String> strings(Object listValue) {
        List<String> values = new ArrayList<>();
        for (Object element : (List<?>) listValue) {
            MemorySegment segment = (MemorySegment) element;
            values.add(new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8));
        }
        return values;
    }
}
