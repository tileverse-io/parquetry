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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ElementMeta;
import io.tileverse.parquetry.columnar.LeafOrdinals;
import io.tileverse.parquetry.columnar.ListMeta;
import io.tileverse.parquetry.columnar.ListMeta.FieldMeta;
import io.tileverse.parquetry.columnar.ListMeta.StructMeta;
import io.tileverse.parquetry.columnar.MapMeta;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.GroupPlan;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.ListPlan;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The level-assembly plan resolves one set of present leaves into the metadata every batch of a row group reads.
 *
 * <p>The load-bearing property is that the leaf ordinals stored in the metadata tree and the leaf ordering the batch's
 * level arrays are packed into come from the same source: a scalar's ordinal must name that scalar's own leaf in the
 * plan's ordering, at every depth of the tree.
 */
class LevelAssemblyPlanTest {

    private static final ColumnPath A = ColumnPath.of("xs", "list", "element", "a");
    private static final ColumnPath B = ColumnPath.of("xs", "list", "element", "b");
    private static final ColumnPath MAP_KEY = ColumnPath.of("xs", "list", "element", "m", "key_value", "key");
    private static final ColumnPath MAP_VALUE = ColumnPath.of("xs", "list", "element", "m", "key_value", "value");

    @Test
    void everyScalarOrdinalNamesItsOwnLeafInThePlansOrdering() {
        ListPlan plan = listPlanFor(Set.of(A, B, MAP_KEY, MAP_VALUE));

        assertOrdinalsResolve(plan.meta(), plan.leafOrdinals());
    }

    @Test
    void theOrderingCoversExactlyThePresentDescendantLeaves() {
        ListPlan plan = listPlanFor(Set.of(A, B, MAP_KEY, MAP_VALUE));
        LeafOrdinals ordinals = plan.leafOrdinals();

        assertThat(pathsOf(ordinals)).containsExactlyInAnyOrder(A, B, MAP_KEY, MAP_VALUE);
        for (int ordinal = 0; ordinal < ordinals.leafCount(); ordinal++) {
            assertThat(ordinals.ordinalOf(ordinals.pathAt(ordinal))).isEqualTo(ordinal);
        }
    }

    @Test
    void aLeafTheProjectionDroppedIsAbsentFromTheOrderingAndTheMetadata() {
        ListPlan plan = listPlanFor(Set.of(A, MAP_KEY, MAP_VALUE));

        assertThat(pathsOf(plan.leafOrdinals())).containsExactlyInAnyOrder(A, MAP_KEY, MAP_VALUE);
        assertThat(fieldNamed(structOf(plan.meta()), "b").element()).isInstanceOf(ElementMeta.Absent.class);
        assertOrdinalsResolve(plan.meta(), plan.leafOrdinals());
    }

    @Test
    void thePlanHidesEveryDescendantLeafOfItsListGroup() {
        ListPlan plan = listPlanFor(Set.of(A, B, MAP_KEY, MAP_VALUE));

        assertThat(plan.hiddenLeaves()).containsExactlyInAnyOrder(A, B, MAP_KEY, MAP_VALUE);
        assertThat(plan.path()).isEqualTo(ColumnPath.of("xs"));
    }

    @Test
    void oneLeafSetResolvesToOnePlan() {
        LevelAssemblyPlans plans = new LevelAssemblyPlans(listOfStructWithMapSchema());

        LevelAssemblyPlan first = plans.forLeaves(Set.of(A, B, MAP_KEY, MAP_VALUE));
        LevelAssemblyPlan again = plans.forLeaves(Set.of(A, B, MAP_KEY, MAP_VALUE));

        assertThat(again).isSameAs(first);
    }

    @Test
    void anUnseenLeafSetResolvesToItsOwnPlan() {
        LevelAssemblyPlans plans = new LevelAssemblyPlans(listOfStructWithMapSchema());

        LevelAssemblyPlan all = plans.forLeaves(Set.of(A, B, MAP_KEY, MAP_VALUE));
        LevelAssemblyPlan withoutB = plans.forLeaves(Set.of(A, MAP_KEY, MAP_VALUE));

        assertThat(withoutB).isNotSameAs(all);
        assertThat(withoutB.repeatedLeaves()).containsExactlyInAnyOrder(A, MAP_KEY, MAP_VALUE);
        assertThat(all.repeatedLeaves()).containsExactlyInAnyOrder(A, B, MAP_KEY, MAP_VALUE);
    }

    // --- ordinal round-trip over the metadata tree ---

    private static void assertOrdinalsResolve(ListMeta list, LeafOrdinals ordinals) {
        assertElementOrdinalsResolve(list.element(), ordinals);
    }

    private static void assertElementOrdinalsResolve(ElementMeta element, LeafOrdinals ordinals) {
        switch (element) {
            case ElementMeta.Scalar(int _, ColumnPath leaf, int leafOrdinal) ->
                assertThat(ordinals.pathAt(leafOrdinal))
                        .as("ordinal %d of the plan ordering", leafOrdinal)
                        .isEqualTo(leaf);
            case ElementMeta.NestedList(ListMeta inner) -> assertOrdinalsResolve(inner, ordinals);
            case ElementMeta.Struct(StructMeta struct) -> assertStructOrdinalsResolve(struct, ordinals);
            case ElementMeta.MapEntry(MapMeta map) -> assertStructOrdinalsResolve(map.entry(), ordinals);
            case ElementMeta.VariantLeaves _, ElementMeta.Absent _ -> {
                // no ordinal to resolve
            }
        }
    }

    private static void assertStructOrdinalsResolve(StructMeta struct, LeafOrdinals ordinals) {
        for (FieldMeta field : struct.fields()) {
            assertElementOrdinalsResolve(field.element(), ordinals);
        }
    }

    private static StructMeta structOf(ListMeta list) {
        return ((ElementMeta.Struct) list.element()).struct();
    }

    private static FieldMeta fieldNamed(StructMeta struct, String name) {
        return struct.fields().stream()
                .filter(field -> field.name().equals(name))
                .findFirst()
                .orElseThrow();
    }

    private static List<ColumnPath> pathsOf(LeafOrdinals ordinals) {
        List<ColumnPath> paths = new ArrayList<>(ordinals.leafCount());
        for (int ordinal = 0; ordinal < ordinals.leafCount(); ordinal++) {
            paths.add(ordinals.pathAt(ordinal));
        }
        return paths;
    }

    // --- schema under test ---

    private static ListPlan listPlanFor(Set<ColumnPath> presentLeaves) {
        LevelAssemblyPlan plan = LevelAssemblyPlan.of(listOfStructWithMapSchema(), presentLeaves);
        GroupPlan group = plan.topLevelGroups().get(0);
        return (ListPlan) group;
    }

    /**
     * {@code optional group xs (LIST) { repeated group list { optional group element { optional double a; optional
     * binary b (STRING); optional group m (MAP) { repeated group key_value { required binary key (STRING); optional
     * int32 value } } } } }}
     */
    private static ParquetSchema listOfStructWithMapSchema() {
        SchemaNode.Primitive a = primitive("a", Repetition.OPTIONAL, PrimitiveKind.DOUBLE);
        SchemaNode.Primitive b = stringPrimitive("b", Repetition.OPTIONAL);
        SchemaNode.Group map = mapStringInt("m");
        SchemaNode.Group element = group("element", Repetition.OPTIONAL, List.of(a, b, map), Optional.empty());
        SchemaNode.Group list = group("list", Repetition.REPEATED, List.of(element), Optional.empty());
        SchemaNode.Group xs = group("xs", Repetition.OPTIONAL, List.of(list), Optional.of(new LogicalType.ListType()));
        return new ParquetSchema(group("root", Repetition.REQUIRED, List.of(xs), Optional.empty()));
    }

    private static SchemaNode.Group mapStringInt(String name) {
        SchemaNode.Primitive key = stringPrimitive("key", Repetition.REQUIRED);
        SchemaNode.Primitive value = primitive("value", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group keyValue = group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty());
        return group(name, Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()));
    }

    private static SchemaNode.Group group(
            String name, Repetition repetition, List<SchemaNode> children, Optional<LogicalType> logicalType) {
        return new SchemaNode.Group(name, repetition, children, logicalType, -1);
    }

    private static SchemaNode.Primitive primitive(String name, Repetition repetition, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, repetition, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive stringPrimitive(String name, Repetition repetition) {
        return new SchemaNode.Primitive(
                name,
                repetition,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }
}
