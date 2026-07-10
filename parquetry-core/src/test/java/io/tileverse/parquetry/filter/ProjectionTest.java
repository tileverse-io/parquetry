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
package io.tileverse.parquetry.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

class ProjectionTest {

    private static SequencedSet<Projection.Column> set(Projection.Column... columns) {
        return new LinkedHashSet<>(List.of(columns));
    }

    @Test
    void ofPreservesOrderAndRejectsEmpty() {
        Projection.Column a = new Projection.Column.Physical(ColumnPath.of("a"), ColumnPath.of("a"));
        Projection.Column k = new Projection.Column.Constant(ColumnPath.of("k"), new Value.IntVal(1));
        Projection.Of of = (Projection.Of) Projection.of(set(a, k));
        assertThat(of.columns()).containsExactly(a, k);
        assertThatThrownBy(() -> Projection.of(new LinkedHashSet<Projection.Column>()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ofRejectsDuplicateName() {
        Projection.Column first = new Projection.Column.Physical(ColumnPath.of("x"), ColumnPath.of("a"));
        Projection.Column second = new Projection.Column.Physical(ColumnPath.of("x"), ColumnPath.of("b"));
        assertThatThrownBy(() -> Projection.of(set(first, second))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void physicalColumnsDerivesPhysicalSourcesAndIgnoresInjected() {
        Projection.Of of = (Projection.Of) Projection.of(set(
                new Projection.Column.Physical(ColumnPath.of("a"), ColumnPath.of("src_a")),
                new Projection.Column.Promoted(ColumnPath.of("b"), ColumnPath.of("src_b"), PrimitiveKind.INT64),
                new Projection.Column.Constant(ColumnPath.of("k"), new Value.IntVal(1)),
                new Projection.Column.Null(ColumnPath.of("n"), new Value.IntVal(0))));
        assertThat(of.physicalColumns()).containsExactlyInAnyOrder(ColumnPath.of("src_a"), ColumnPath.of("src_b"));
    }

    @Test
    void needsShapingIsFalseForPlainPassthroughAndTrueOtherwise() {
        Projection.Of passthrough = (Projection.Of) Projection.of(set(
                new Projection.Column.Physical(ColumnPath.of("a"), ColumnPath.of("a")),
                new Projection.Column.Physical(ColumnPath.of("b"), ColumnPath.of("b"))));
        assertThat(passthrough.needsShaping()).isFalse();

        Projection.Of renamed = (Projection.Of)
                Projection.of(set(new Projection.Column.Physical(ColumnPath.of("a"), ColumnPath.of("src"))));
        assertThat(renamed.needsShaping()).isTrue();

        Projection.Of constant = (Projection.Of)
                Projection.of(set(new Projection.Column.Constant(ColumnPath.of("k"), new Value.IntVal(1))));
        assertThat(constant.needsShaping()).isTrue();

        Projection.Of rowPosition =
                (Projection.Of) Projection.of(set(new Projection.Column.RowPosition(ColumnPath.of("_pos"), 0L)));
        assertThat(rowPosition.needsShaping()).isTrue();
    }

    @Test
    void physicalColumnsIgnoresASynthesizedRowPosition() {
        Projection.Of of = (Projection.Of) Projection.of(set(
                new Projection.Column.Physical(ColumnPath.of("a"), ColumnPath.of("src_a")),
                new Projection.Column.RowPosition(ColumnPath.of("_pos"), 0L)));
        assertThat(of.physicalColumns()).containsExactly(ColumnPath.of("src_a"));
    }

    @Test
    void ofPhysicalBuildsPassthroughColumnsInOrder() {
        Projection.Of of = (Projection.Of) Projection.ofPhysical(List.of(ColumnPath.of("a"), ColumnPath.of("b")));
        assertThat(of.columns())
                .containsExactly(
                        new Projection.Column.Physical(ColumnPath.of("a"), ColumnPath.of("a")),
                        new Projection.Column.Physical(ColumnPath.of("b"), ColumnPath.of("b")));
        assertThat(of.needsShaping()).isFalse();
    }
}
