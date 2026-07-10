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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.testkit.TestCorpus;

import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Proves the nested field-id guard over the only vendored nested corpus table, {@code v2_bbox_struct} (id#1 string,
 * bbox#2 struct of xmin#5/ymin#6/xmax#7/ymax#8 double). The data files hold nested ids that agree with the table
 * schema; an unevolved read passes. Evolving the metadata's nested ids to disagree with the byte-identical data file (a
 * rename and an id reassignment) makes the read fail loud.
 */
class IcebergNestedEvolutionIT {

    private static final String TABLE = "v2_bbox_struct";
    private static final long EXPECTED_TOTAL_ROWS = 10_000L;

    @TempDir
    Path tempDir;

    @Test
    void unevolvedNestedTableReadsWithoutFiring() {
        Path tableDir = extractTable();

        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertThatCode(() -> fullScanRowCount(dataset)).doesNotThrowAnyException();
            assertThat(fullScanRowCount(dataset)).isEqualTo(EXPECTED_TOTAL_ROWS);
        }
    }

    @Test
    void aNestedRenameFailsLoud() {
        // table renames bbox.xmin (id 5) to bbox.left (id 5); the file still calls id 5 "xmin".
        Path tableDir = evolveBbox("left", 5, "ymin", 6, "xmax", 7, "ymax", 8);

        assertThatThrownBy(() -> readAll(tableDir))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("bbox.xmin")
                .hasMessageContaining("bbox.left");
    }

    @Test
    void aNestedReassignmentFailsLoud() {
        // table keeps the name xmin but reassigns it to id 99; the file's bbox.xmin is id 5.
        Path tableDir = evolveBbox("xmin", 99, "ymin", 6, "xmax", 7, "ymax", 8);

        assertThatThrownBy(() -> readAll(tableDir))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("bbox.xmin")
                .hasMessageContaining("99");
    }

    @Test
    void aDisagreementThrowsOnEveryRead() {
        // a file that disagrees is never memoized as passing: a second read throws too.
        Path tableDir = evolveBbox("left", 5, "ymin", 6, "xmax", 7, "ymax", 8);

        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertThatThrownBy(() -> fullScanRowCount(dataset)).isInstanceOf(IcebergFormatException.class);
            assertThatThrownBy(() -> fullScanRowCount(dataset)).isInstanceOf(IcebergFormatException.class);
        }
    }

    private Path evolveBbox(String n1, int i1, String n2, int i2, String n3, int i3, String n4, int i4) {
        ObjectNode id = field(1, "id", "string");
        ObjectNode bbox = JsonMapper.shared().createObjectNode();
        bbox.put("id", 2);
        bbox.put("name", "bbox");
        bbox.put("required", false);
        ObjectNode struct = JsonMapper.shared().createObjectNode();
        struct.put("type", "struct");
        struct.set(
                "fields",
                JsonMapper.shared()
                        .createArrayNode()
                        .add(field(i1, n1, "double"))
                        .add(field(i2, n2, "double"))
                        .add(field(i3, n3, "double"))
                        .add(field(i4, n4, "double")));
        bbox.set("type", struct);
        return IcebergSchemaEvolution.evolveCurrentSchemaRaw(extractTable(), List.of(id, bbox));
    }

    private static ObjectNode field(int id, String name, String type) {
        ObjectNode node = JsonMapper.shared().createObjectNode();
        node.put("id", id);
        node.put("name", name);
        node.put("required", false);
        node.put("type", type);
        return node;
    }

    private Path extractTable() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }

    private void readAll(Path tableDir) {
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            fullScanRowCount(dataset);
        }
    }

    private static long fullScanRowCount(IcebergDataset dataset) {
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }
}
