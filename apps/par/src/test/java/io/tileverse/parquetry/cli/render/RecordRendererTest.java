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
package io.tileverse.parquetry.cli.render;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class RecordRendererTest {

    @Test
    void jsonlEmitsOneObjectPerRowWithStringDecoded(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        String out = renderAll(file, dir, RecordRenderer.Mode.JSONL);
        String[] lines = out.strip().split("\n");
        assertThat(lines).hasSize(4);
        assertThat(lines[0]).contains("\"id\":1", "\"name\":\"Rosario\"", "\"pop\":1300000", "\"capital\":false");
        assertThat(lines[3]).contains("\"id\":4");
        assertThat(lines[3]).doesNotContain("\"name\":\"");
    }

    @Test
    void csvEmitsHeaderThenRows(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        String out = renderAll(file, dir, RecordRenderer.Mode.CSV);
        String[] lines = out.strip().split("\n");
        assertThat(lines[0]).isEqualTo("id,name,pop,capital");
        assertThat(lines[1]).startsWith("1,Rosario,1300000,false");
    }

    @Test
    void jsonlRendersNestedMapAsObject(@TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("parquet-testing/data/nested_maps.snappy.parquet", dir);
        String out = renderAll(file, dir, RecordRenderer.Mode.JSONL);
        String firstLine = out.strip().split("\n")[0];
        JsonNode node = JsonMapper.shared().readTree(firstLine);
        assertThat(node.get("a").isObject())
                .as("string-keyed map renders as a JSON object")
                .isTrue();
        assertThat(firstLine).doesNotContain("a.key_value");
    }

    private static String renderAll(Path file, Path dir, RecordRenderer.Mode mode) throws Exception {
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(ByteRangeSources.from(reader));
            ParquetSchema schema = dataset.schema();
            StringWriter sw = new StringWriter();
            RecordRenderer renderer = new RecordRenderer(mode, schema, new PrintWriter(sw));
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                renderer.begin();
                rows.forEach(renderer::row);
                renderer.end();
            }
            return sw.toString();
        }
    }
}
