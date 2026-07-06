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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.VectorArrays;

/**
 * Unit tests for struct scope support in {@link ParquetRecordBatchBuilder}.
 *
 * <p>Schema under test: {@code optional group s { optional int32 f; }} unless stated otherwise.
 */
class StructBuilderTest {

    /** Row 0: struct present, field present (f=7). Row 1: struct null. Verifies size, null flag, and child value. */
    @Test
    void optionalStructWithOptionalField() {
        ParquetSchema schema = optionalStructWithField("f", Repetition.OPTIONAL);
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        builder.beginStruct("s").setInt(ColumnPath.of("s", "f"), 7).endStruct().endRow();
        builder.setNull(ColumnPath.of("s")).endRow();

        ParquetRecordBatch batch = builder.build();

        assertThat(batch.rowCount()).isEqualTo(2);
        ColumnVector sCol = batch.columns().get(ColumnPath.of("s"));
        assertThat(sCol).isInstanceOf(StructVector.class);
        StructVector sv = (StructVector) sCol;
        assertThat(sv.size()).isEqualTo(2);
        assertThat(sv.validity().isNull(1)).isTrue();

        ColumnVector fCol = sv.children().get(ColumnPath.of("f"));
        assertThat(fCol).isInstanceOf(IntVector.class);
        IntVector iv = (IntVector) fCol;
        assertThat(VectorArrays.toArray(iv)[0]).isEqualTo(7);
    }

    /** An OPTIONAL struct that is never explicitly set or nulled for a row defaults to null at endRow. */
    @Test
    void unsetOptionalStructDefaultsToNull() {
        ParquetSchema schema = optionalStructWithField("f", Repetition.OPTIONAL);
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        // No beginStruct/endStruct or setNull - the struct is unset.
        builder.endRow();

        ParquetRecordBatch batch = builder.build();
        ColumnVector sCol = batch.columns().get(ColumnPath.of("s"));
        assertThat(sCol).isInstanceOf(StructVector.class);
        StructVector sv = (StructVector) sCol;
        assertThat(sv.validity().isNull(0)).isTrue();
    }

    /** A REQUIRED field inside a present struct that is not set must throw at endRow. */
    @Test
    void requiredFieldInsideStructMissingThrows() {
        SchemaNode.Primitive rField = new SchemaNode.Primitive(
                "r", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group sGroup = new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(rField), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(sGroup), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.beginStruct("s");
        // Do NOT set the required field r.
        builder.endStruct();

        assertThatThrownBy(builder::endRow)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("r");
    }

    /** Nested struct (struct inside struct): verifies correct child-vector wiring at two levels. */
    @Test
    @SuppressWarnings("java:S125") // schema description comment, not commented-out code
    void nestedStructInsideStruct() {
        // Schema: optional group outer { optional group inner { optional int32 x; } }
        SchemaNode.Primitive xField = new SchemaNode.Primitive(
                "x", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group inner =
                new SchemaNode.Group("inner", Repetition.OPTIONAL, List.of(xField), Optional.empty(), -1);
        SchemaNode.Group outer =
                new SchemaNode.Group("outer", Repetition.OPTIONAL, List.of(inner), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(outer), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.beginStruct("outer")
                .beginStruct(ColumnPath.of("outer", "inner"))
                .setInt(ColumnPath.of("outer", "inner", "x"), 42)
                .endStruct()
                .endStruct()
                .endRow();
        // Row 1: outer present, inner null.
        builder.beginStruct("outer")
                .setNull(ColumnPath.of("outer", "inner"))
                .endStruct()
                .endRow();

        ParquetRecordBatch batch = builder.build();
        assertThat(batch.rowCount()).isEqualTo(2);

        StructVector outerVec = (StructVector) batch.columns().get(ColumnPath.of("outer"));
        assertThat(outerVec.validity().isNull(0)).isFalse();
        assertThat(outerVec.validity().isNull(1)).isFalse();

        StructVector innerVec0 = (StructVector) outerVec.children().get(ColumnPath.of("inner"));
        assertThat(innerVec0.validity().isNull(0)).isFalse();
        assertThat(innerVec0.validity().isNull(1)).isTrue();

        IntVector xVec = (IntVector) innerVec0.children().get(ColumnPath.of("x"));
        assertThat(VectorArrays.toArray(xVec)[0]).isEqualTo(42);
    }

    /** The batch can mix a flat primitive column and a struct column in the same schema. */
    @Test
    void flatAndStructColumnsCoexist() {
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive fLeaf = new SchemaNode.Primitive(
                "f", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group sGroup = new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(fLeaf), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(idLeaf, sGroup), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setInt(ColumnPath.of("id"), 1)
                .beginStruct("s")
                .setInt(ColumnPath.of("s", "f"), 10)
                .endStruct()
                .endRow();
        builder.setNull(ColumnPath.of("id")).setNull(ColumnPath.of("s")).endRow();

        ParquetRecordBatch batch = builder.build();
        assertThat(batch.rowCount()).isEqualTo(2);

        // Row 0: id=1, s.f=10.
        ParquetRecord r0 = batch.materialize(0);
        assertThat(r0.getInt(0)).isEqualTo(1);

        StructVector sv = (StructVector) batch.columns().get(ColumnPath.of("s"));
        assertThat(sv.validity().isNull(0)).isFalse();
        assertThat(VectorArrays.toArray(((IntVector) sv.children().get(ColumnPath.of("f"))))[0])
                .isEqualTo(10);

        // Row 1: both null.
        assertThat(batch.columns().get(ColumnPath.of("id")).validity().isNull(1))
                .isTrue();
        assertThat(sv.validity().isNull(1)).isTrue();
    }

    /**
     * Authoring a struct schema through the index-based setters (as {@link WriteFixtures#appendRow} does) must produce
     * the correct per-row child values. Before the fix, the struct accumulator did not know children had been authored
     * implicitly and overwrote every child value with null in {@code endRow()}, producing all-default vectors.
     */
    @Test
    @SuppressWarnings("java:S125") // schema description comment, not commented-out code
    void indexSettersRouteIntoStructChildWithDistinctPerRowValues() {
        // Schema: required group bbox { required float xmin; required float xmax; }
        ParquetSchema schema = requiredBboxSchema();

        ColumnPath xminPath = ColumnPath.of("bbox", "xmin");
        ColumnPath xmaxPath = ColumnPath.of("bbox", "xmax");

        List<Map<ColumnPath, Object>> rows = List.of(
                Map.of(xminPath, 0.0f, xmaxPath, 1.0f),
                Map.of(xminPath, 10.0f, xmaxPath, 11.0f),
                Map.of(xminPath, 20.0f, xmaxPath, 21.0f));

        ParquetRecordBatch batch = WriteFixtures.batch(schema, rows);

        assertThat(batch.rowCount()).isEqualTo(3);
        ColumnVector bboxCol = batch.columns().get(ColumnPath.of("bbox"));
        assertThat(bboxCol).isInstanceOf(StructVector.class);
        StructVector bboxVec = (StructVector) bboxCol;

        FloatVector xminVec = (FloatVector) bboxVec.children().get(ColumnPath.of("xmin"));
        assertThat(VectorArrays.toArray(xminVec))
                .as("xmin must hold the distinct per-row values authored via index setters")
                .containsExactly(0.0f, 10.0f, 20.0f);

        FloatVector xmaxVec = (FloatVector) bboxVec.children().get(ColumnPath.of("xmax"));
        assertThat(VectorArrays.toArray(xmaxVec))
                .as("xmax must hold the distinct per-row values authored via index setters")
                .containsExactly(1.0f, 11.0f, 21.0f);
    }

    /**
     * FIX 1: a REQUIRED child inside an ABSENT optional struct must not throw at endRow. The struct being absent means
     * the whole subtree is null, which is valid regardless of child repetition.
     *
     * <p>Schema: {@code optional group person { required binary name (STRING); optional int32 age; }}
     *
     * <p>Three rows: (0) person present with name set, (1) person null - never touched, (2) person present with name
     * set. Row 1 must not throw even though "name" is required and was never set.
     */
    @Test
    void requiredChildOfAbsentOptionalStructDoesNotThrow() {
        SchemaNode.Primitive nameField = new SchemaNode.Primitive(
                "name", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive ageField = new SchemaNode.Primitive(
                "age", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group personGroup =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(nameField, ageField), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(personGroup), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        // Row 0: person present, name set.
        builder.beginStruct("person")
                .setString(ColumnPath.of("person", "name"), "Alice")
                .endStruct()
                .endRow();

        // Row 1: person absent - do not touch person at all. Must not throw despite required "name" not being set.
        builder.endRow();

        // Row 2: person present, name set again.
        builder.beginStruct("person")
                .setString(ColumnPath.of("person", "name"), "Bob")
                .endStruct()
                .endRow();

        ParquetRecordBatch batch = builder.build();
        assertThat(batch.rowCount()).isEqualTo(3);
        StructVector personVec = (StructVector) batch.columns().get(ColumnPath.of("person"));
        assertThat(personVec.validity().isNull(0)).isFalse();
        assertThat(personVec.validity().isNull(1)).isTrue();
        assertThat(personVec.validity().isNull(2)).isFalse();
    }

    /**
     * FIX 3: setNull on a struct child path must mark the enclosing struct present (the struct exists but the child is
     * null), while setNull on the struct path itself must leave the struct absent.
     *
     * <p>Schema: {@code optional group s { optional int32 f; }}
     *
     * <p>Row A: only {@code setNull(s.f)} - struct must be present, f must be null. Row B: only {@code setNull(s)} -
     * struct must be null.
     */
    @Test
    void setNullOnChildMarksStructPresent() {
        ParquetSchema schema = optionalStructWithField("f", Repetition.OPTIONAL);
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        // Row A: null child, struct is present.
        builder.setNull(ColumnPath.of("s", "f")).endRow();

        // Row B: null struct itself.
        builder.setNull(ColumnPath.of("s")).endRow();

        ParquetRecordBatch batch = builder.build();
        StructVector sv = (StructVector) batch.columns().get(ColumnPath.of("s"));

        // Row A: struct present.
        assertThat(sv.validity().isNull(0))
                .as("row A: struct must be present when child was nulled")
                .isFalse();
        // Child f must be null for row A.
        IntVector fVec = (IntVector) sv.children().get(ColumnPath.of("f"));
        assertThat(fVec.isNull(0)).as("row A: child f must be null").isTrue();

        // Row B: struct absent.
        assertThat(sv.validity().isNull(1)).as("row B: struct must be null").isTrue();
    }

    // --- helpers ---

    private static ParquetSchema requiredBboxSchema() {
        SchemaNode.Group bbox = new SchemaNode.Group(
                "bbox",
                Repetition.REQUIRED,
                List.of(
                        new SchemaNode.Primitive(
                                "xmin",
                                Repetition.REQUIRED,
                                PrimitiveKind.FLOAT,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1),
                        new SchemaNode.Primitive(
                                "xmax",
                                Repetition.REQUIRED,
                                PrimitiveKind.FLOAT,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1)),
                Optional.empty(),
                -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(bbox), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema optionalStructWithField(String fieldName, Repetition fieldRep) {
        SchemaNode.Primitive field = new SchemaNode.Primitive(
                fieldName, fieldRep, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group sGroup = new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(field), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(sGroup), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
