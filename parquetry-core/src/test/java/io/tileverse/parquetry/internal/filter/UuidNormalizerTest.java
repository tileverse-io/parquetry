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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class UuidNormalizerTest {

    private static ParquetSchema schemaWithFlba(String name, int width, Optional<LogicalType> logical) {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(width), logical, -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    @Test
    void uuidValAcceptedOnFlba16() {
        ParquetSchema schema = schemaWithFlba("id", 16, Optional.of(new LogicalType.UuidType()));
        Predicate pred = Pred.col(ColumnPath.of("id")).eq(new UUID(1L, 2L));
        assertThat(PredicateNormalizer.normalizeAndValidate(pred, schema)).isNotNull();
    }

    @Test
    void uuidValRejectedOnWrongWidth() {
        ParquetSchema schema = schemaWithFlba("id", 12, Optional.empty());
        Predicate pred = Pred.col(ColumnPath.of("id")).eq(new UUID(1L, 2L));
        assertThatThrownBy(() -> PredicateNormalizer.normalizeAndValidate(pred, schema))
                .isInstanceOf(ParquetSchemaException.class);
    }

    @Test
    void uuidValRejectedOnWrongWidthInPath() {
        ParquetSchema schema = schemaWithFlba("id", 12, Optional.empty());
        Predicate pred = Pred.col(ColumnPath.of("id")).inUuids(new UUID(1L, 2L), new UUID(3L, 4L));
        assertThatThrownBy(() -> PredicateNormalizer.normalizeAndValidate(pred, schema))
                .isInstanceOf(ParquetSchemaException.class);
    }

    @Test
    void uuidValAcceptedOnFlba16InPath() {
        ParquetSchema schema = schemaWithFlba("id", 16, Optional.of(new LogicalType.UuidType()));
        Predicate pred = Pred.col(ColumnPath.of("id")).inUuids(new UUID(1L, 2L), new UUID(3L, 4L));
        assertThat(PredicateNormalizer.normalizeAndValidate(pred, schema)).isNotNull();
    }
}
