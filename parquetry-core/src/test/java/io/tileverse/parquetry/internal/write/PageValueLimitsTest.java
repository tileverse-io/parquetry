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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pins the page value limit each leaf of one write receives: an explicit per-path entry first, then the covering limit
 * for a covering leaf, then the limit shared by all columns.
 */
class PageValueLimitsTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath XMIN = ColumnPath.of("bbox", "xmin");
    private static final ColumnPath YMIN = ColumnPath.of("bbox", "ymin");
    private static final ColumnPath XMAX = ColumnPath.of("bbox", "xmax");
    private static final ColumnPath YMAX = ColumnPath.of("bbox", "ymax");
    private static final List<ColumnPath> COVERING_LEAVES = List.of(XMIN, YMIN, XMAX, YMAX);

    @Test
    void aLeafWithoutAnEntryTakesTheSharedLimit() {
        WriteOptions options = WriteOptions.builder().pageValueLimit(8_192).build();

        PageValueLimits limits = PageValueLimits.of(options);

        assertThat(limits.forLeaf(GEOMETRY)).isEqualTo(8_192);
    }

    @Test
    void anExplicitEntryOverridesTheSharedLimit() {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(8_192)
                .pageValueLimit("id", 1_024)
                .build();

        PageValueLimits limits = PageValueLimits.of(options);

        assertThat(limits.forLeaf(ColumnPath.of("id"))).isEqualTo(1_024);
        assertThat(limits.forLeaf(GEOMETRY)).isEqualTo(8_192);
    }

    @Test
    void theCoveringLimitAppliesToEveryCoveringLeaf() {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(8_192)
                .coveringPageValueLimit(512)
                .build();

        PageValueLimits limits = PageValueLimits.of(options, COVERING_LEAVES);

        assertThat(limits.forLeaf(XMIN)).isEqualTo(512);
        assertThat(limits.forLeaf(YMIN)).isEqualTo(512);
        assertThat(limits.forLeaf(XMAX)).isEqualTo(512);
        assertThat(limits.forLeaf(YMAX)).isEqualTo(512);
        assertThat(limits.forLeaf(GEOMETRY))
                .as("the covering limit leaves the other columns on the shared limit")
                .isEqualTo(8_192);
    }

    @Test
    void anExplicitEntryOnACoveringLeafBeatsTheCoveringLimit() {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(8_192)
                .coveringPageValueLimit(512)
                .pageValueLimit("bbox.xmin", 256)
                .build();

        PageValueLimits limits = PageValueLimits.of(options, COVERING_LEAVES);

        assertThat(limits.forLeaf(XMIN)).isEqualTo(256);
        assertThat(limits.forLeaf(YMIN)).isEqualTo(512);
    }

    @Test
    void coveringLeavesTakeTheSharedLimitWhenNoCoveringLimitIsSet() {
        WriteOptions options = WriteOptions.builder().pageValueLimit(8_192).build();

        PageValueLimits limits = PageValueLimits.of(options, COVERING_LEAVES);

        assertThat(limits.forLeaf(XMIN)).isEqualTo(8_192);
    }

    @Test
    void aCoveringLimitWithoutCoveringLeavesChangesNothing() {
        WriteOptions options = WriteOptions.builder()
                .pageValueLimit(8_192)
                .coveringPageValueLimit(512)
                .build();

        PageValueLimits limits = PageValueLimits.of(options);

        assertThat(limits.forLeaf(XMIN)).isEqualTo(8_192);
        assertThat(limits.forLeaf(GEOMETRY)).isEqualTo(8_192);
    }
}
