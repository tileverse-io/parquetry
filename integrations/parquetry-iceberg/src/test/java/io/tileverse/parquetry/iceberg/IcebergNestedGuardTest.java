/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class IcebergNestedGuardTest {

    // table: id#1 string, bbox#2 { xmin#5, ymin#6 } double
    private static final IcebergNestedSchema TABLE = tableWithBbox(5, 6);

    @Test
    void agreementPasses() {
        ParquetSchema file = fileWithBbox("xmin", 5, "ymin", 6);

        assertThatCode(() -> IcebergNestedGuard.validate(TABLE, file)).doesNotThrowAnyException();
    }

    @Test
    void renameThrowsNamingThePath() {
        // file still calls id 5 "xmin"; table renamed id 5 to "left"
        IcebergNestedSchema renamed = tableWithNames("left", 5, "ymin", 6);
        ParquetSchema file = fileWithBbox("xmin", 5, "ymin", 6);

        assertThatThrownBy(() -> IcebergNestedGuard.validate(renamed, file))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("bbox.xmin");
    }

    @Test
    void reassignmentThrowsNamingThePath() {
        // file calls the member at slot bbox.xmin id 99; table says id 5
        ParquetSchema file = fileWithBbox("xmin", 99, "ymin", 6);

        assertThatThrownBy(() -> IcebergNestedGuard.validate(TABLE, file))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("bbox.xmin");
    }

    @Test
    void aFileOnlyNestedMemberIsToleratedAsADrop() {
        // file has an extra member legacy#7 the table dropped
        ParquetSchema file = fileWithBboxPlus("xmin", 5, "ymin", 6, "legacy", 7);

        assertThatCode(() -> IcebergNestedGuard.validate(TABLE, file)).doesNotThrowAnyException();
    }

    @Test
    void aTableOnlyNestedMemberIsToleratedAsAnAdd() {
        // table adds zmin#9; file lacks it
        IcebergNestedSchema added = tableWithBboxAndZ(5, 6, 9);
        ParquetSchema file = fileWithBbox("xmin", 5, "ymin", 6);

        assertThatCode(() -> IcebergNestedGuard.validate(added, file)).doesNotThrowAnyException();
    }

    @Test
    void anIdlessNestedMemberIsSkipped() {
        // file member "xmin" has no field id (-1); cannot be checked
        ParquetSchema file = fileWithBbox("xmin", -1, "ymin", 6);

        assertThatCode(() -> IcebergNestedGuard.validate(TABLE, file)).doesNotThrowAnyException();
    }

    @Test
    void anEmptyTableSchemaIsANoOp() {
        ParquetSchema file = fileWithBbox("xmin", 99, "ymin", 6);

        assertThatCode(() -> IcebergNestedGuard.validate(IcebergNestedSchema.of(null), file))
                .doesNotThrowAnyException();
    }

    @Test
    void aTopLevelNestedColumnRenameThrows() {
        // table renamed the column id 2 from bbox to bounds; file still calls it bbox
        IcebergNestedSchema bounds = tableColumnNamed("bounds", 5, 6);
        ParquetSchema file = fileWithBbox("xmin", 5, "ymin", 6);

        assertThatThrownBy(() -> IcebergNestedGuard.validate(bounds, file))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("bounds");
    }

    @Test
    void aListWithMatchingElementIdPasses() {
        IcebergNestedSchema table = parseTable("""
                [ {"id": 3, "name": "tags", "required": false,
                   "type": {"type": "list", "element-id": 9, "element-required": false, "element": "string"}} ]
                """);
        ParquetSchema file = schema(List.of(listColumn("tags", 3, "element", 9)));

        assertThatCode(() -> IcebergNestedGuard.validate(table, file)).doesNotThrowAnyException();
    }

    @Test
    void aListElementReassignmentThrows() {
        IcebergNestedSchema table = parseTable("""
                [ {"id": 3, "name": "tags", "required": false,
                   "type": {"type": "list", "element-id": 9, "element-required": false, "element": "string"}} ]
                """);
        ParquetSchema file = schema(List.of(listColumn("tags", 3, "element", 99)));

        assertThatThrownBy(() -> IcebergNestedGuard.validate(table, file))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("tags.element");
    }

    @Test
    void aMapWithMatchingKeyValueIdsPasses() {
        IcebergNestedSchema table = parseTable("""
                [ {"id": 4, "name": "props", "required": false,
                   "type": {"type": "map", "key-id": 10, "key": "string",
                            "value-id": 11, "value-required": false, "value": "long"}} ]
                """);
        ParquetSchema file = schema(List.of(mapColumn("props", 4, "key", 10, "value", 11)));

        assertThatCode(() -> IcebergNestedGuard.validate(table, file)).doesNotThrowAnyException();
    }

    @Test
    void aMapValueReassignmentThrows() {
        IcebergNestedSchema table = parseTable("""
                [ {"id": 4, "name": "props", "required": false,
                   "type": {"type": "map", "key-id": 10, "key": "string",
                            "value-id": 11, "value-required": false, "value": "long"}} ]
                """);
        ParquetSchema file = schema(List.of(mapColumn("props", 4, "key", 10, "value", 99)));

        assertThatThrownBy(() -> IcebergNestedGuard.validate(table, file))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("props.value");
    }

    // --- table builders (parse JSON so the fixture matches IcebergNestedSchema.of exactly) ---

    private static IcebergNestedSchema tableWithBbox(int xminId, int yminId) {
        return tableWithNames("xmin", xminId, "ymin", yminId);
    }

    private static IcebergNestedSchema tableWithNames(String xName, int xId, String yName, int yId) {
        return parseTable(("""
                [ {"id": 1, "name": "id", "required": false, "type": "string"},
                  {"id": 2, "name": "bbox", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"id": %d, "name": "%s", "required": false, "type": "double"},
                     {"id": %d, "name": "%s", "required": false, "type": "double"}
                   ]}} ]
                """).formatted(xId, xName, yId, yName));
    }

    private static IcebergNestedSchema tableWithBboxAndZ(int xId, int yId, int zId) {
        return parseTable(("""
                [ {"id": 2, "name": "bbox", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"id": %d, "name": "xmin", "required": false, "type": "double"},
                     {"id": %d, "name": "ymin", "required": false, "type": "double"},
                     {"id": %d, "name": "zmin", "required": false, "type": "double"}
                   ]}} ]
                """).formatted(xId, yId, zId));
    }

    private static IcebergNestedSchema tableColumnNamed(String columnName, int xId, int yId) {
        return parseTable(("""
                [ {"id": 2, "name": "%s", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"id": %d, "name": "xmin", "required": false, "type": "double"},
                     {"id": %d, "name": "ymin", "required": false, "type": "double"}
                   ]}} ]
                """).formatted(columnName, xId, yId));
    }

    private static IcebergNestedSchema parseTable(String fieldsJson) {
        JsonNode fields = JsonMapper.shared().readTree(fieldsJson);
        return IcebergNestedSchema.of(fields);
    }

    // --- file builders (SchemaNode tree with a top-level bbox group id 2) ---

    private static ParquetSchema fileWithBbox(String xName, int xId, String yName, int yId) {
        SchemaNode.Group bbox = group("bbox", 2, List.of(leaf(xName, xId), leaf(yName, yId)));
        return schema(List.of(leaf("id", 1), bbox));
    }

    private static ParquetSchema fileWithBboxPlus(
            String xName, int xId, String yName, int yId, String extraName, int extraId) {
        SchemaNode.Group bbox = group("bbox", 2, List.of(leaf(xName, xId), leaf(yName, yId), leaf(extraName, extraId)));
        return schema(List.of(leaf("id", 1), bbox));
    }

    private static ParquetSchema schema(List<SchemaNode> children) {
        SchemaNode.Group root = new SchemaNode.Group("table", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Group group(String name, int fieldId, List<SchemaNode> children) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, children, Optional.empty(), fieldId);
    }

    private static SchemaNode.Primitive leaf(String name, int fieldId) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), fieldId);
    }

    // Standard Parquet LIST: <name> (LIST) { repeated group list { <element> } }
    private static SchemaNode.Group listColumn(String name, int listId, String elementName, int elementId) {
        SchemaNode.Group repeated = group("list", -1, List.of(leaf(elementName, elementId)));
        return group(name, listId, List.of(repeated));
    }

    // Standard Parquet MAP: <name> (MAP) { repeated group key_value { <key>; <value> } }
    private static SchemaNode.Group mapColumn(
            String name, int mapId, String keyName, int keyId, String valueName, int valueId) {
        SchemaNode.Group keyValue = group("key_value", -1, List.of(leaf(keyName, keyId), leaf(valueName, valueId)));
        return group(name, mapId, List.of(keyValue));
    }
}
