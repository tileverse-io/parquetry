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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ProjectionPlanTest {

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");

    @Test
    void scanProjectionUnionsPredicateColumns() {
        ProjectionPlan plan = ProjectionPlan.resolve(
                schema(), Projection.ofPhysical(List.of(A)), new Predicate.Eq(B, new Value.IntVal(1)));

        Projection.Of scan = (Projection.Of) plan.scanProjection();
        assertThat(scan.physicalColumns()).containsExactlyInAnyOrder(A, B);
        assertThat(plan.physicalOutputSchema().leafColumns()).containsExactly(A);
        assertThat(plan.needsShaping()).isFalse();
    }

    @Test
    void plainPassthroughNeedsNoShaping() {
        ProjectionPlan plan =
                ProjectionPlan.resolve(schema(), Projection.ofPhysical(List.of(A)), Predicate.ALWAYS_TRUE);

        assertThat(plan.needsShaping()).isFalse();
        assertThat(plan.producedSchema().leafColumns())
                .isEqualTo(plan.physicalOutputSchema().leafColumns());
    }

    @Test
    void allConstantProduceSetDrivesRowsFromOneCheapLeaf() {
        Projection onlyConstant =
                Projection.of(seq(new Projection.Column.Constant(ColumnPath.of("k"), new Value.IntVal(1))));

        ProjectionPlan plan = ProjectionPlan.resolve(schema(), onlyConstant, Predicate.ALWAYS_TRUE);

        assertThat(plan.needsShaping()).isTrue();
        assertThat(plan.physicalOutputSchema().leafColumns()).hasSize(1);
        assertThat(plan.physicalOutputSchema().leafColumns()).containsAnyElementsOf(schema().leafColumns());
        assertThat(plan.producedSchema().leafColumns()).containsExactly(ColumnPath.of("k"));
    }

    private static java.util.SequencedSet<Projection.Column> seq(Projection.Column... columns) {
        return new java.util.LinkedHashSet<>(List.of(columns));
    }

    private static ParquetSchema schema() {
        SchemaNode.Primitive a = new SchemaNode.Primitive(
                "a", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Primitive b = new SchemaNode.Primitive(
                "b", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 2);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(a, b), Optional.empty(), 0);
        return new ParquetSchema(root);
    }
}
