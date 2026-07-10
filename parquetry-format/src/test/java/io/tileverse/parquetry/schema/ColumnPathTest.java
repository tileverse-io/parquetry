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
package io.tileverse.parquetry.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ColumnPathTest {

    @Nested
    class Construction {

        @Test
        void ofSegmentsJoinsOnDot() {
            ColumnPath path = ColumnPath.of("bbox", "xmin");

            assertThat(path.dot()).isEqualTo("bbox.xmin");
        }

        @Test
        void ofSingleSegmentHasNoDot() {
            assertThat(ColumnPath.of("geometry").dot()).isEqualTo("geometry");
        }

        @Test
        void ofListJoinsOnDot() {
            ColumnPath path = ColumnPath.of(List.of("a", "b", "c"));

            assertThat(path.dot()).isEqualTo("a.b.c");
        }

        @Test
        void parseAcceptsAlreadyDottedForm() {
            assertThat(ColumnPath.parse("a.b.c").numParts()).isEqualTo(3);
        }

        @Test
        void rejectsDotInsideSegment() {
            assertThatIllegalArgumentException().isThrownBy(() -> ColumnPath.of("bbox.xmin"));
        }

        @Test
        void rejectsBlankSegment() {
            assertThatIllegalArgumentException().isThrownBy(() -> ColumnPath.of("a", " "));
        }

        @Test
        void rejectsEmptySegmentList() {
            assertThatIllegalArgumentException().isThrownBy(() -> ColumnPath.of(List.of()));
        }

        @Test
        void parseRejectsLeadingTrailingOrDoubleDots() {
            assertThatIllegalArgumentException().isThrownBy(() -> ColumnPath.parse(".a"));
            assertThatIllegalArgumentException().isThrownBy(() -> ColumnPath.parse("a."));
            assertThatIllegalArgumentException().isThrownBy(() -> ColumnPath.parse("a..b"));
        }

        @Test
        void acceptsPlainSegments() {
            assertThatNoException().isThrownBy(() -> ColumnPath.of("a", "b"));
        }
    }

    @Nested
    class Navigation {

        private final ColumnPath path = ColumnPath.of("bbox", "xmin");

        @Test
        void numPartsCountsSegments() {
            assertThat(path.numParts()).isEqualTo(2);
            assertThat(ColumnPath.of("geometry").numParts()).isEqualTo(1);
        }

        @Test
        void partReturnsSegmentByIndex() {
            assertThat(path.part(0)).isEqualTo("bbox");
            assertThat(path.part(1)).isEqualTo("xmin");
        }

        @Test
        void partRejectsOutOfRangeIndex() {
            assertThat(catchThrowable(() -> path.part(2))).isInstanceOf(IndexOutOfBoundsException.class);
        }

        @Test
        void nameReturnsLeafSegment() {
            assertThat(path.name()).isEqualTo("xmin");
            assertThat(ColumnPath.of("geometry").name()).isEqualTo("geometry");
        }

        private static Throwable catchThrowable(Runnable runnable) {
            try {
                runnable.run();
                return null;
            } catch (Throwable t) {
                return t;
            }
        }
    }

    @Nested
    class Equality {

        @Test
        void equalWhenSameSegments() {
            assertThat(ColumnPath.of("bbox", "xmin")).isEqualTo(ColumnPath.of("bbox", "xmin"));
        }

        @Test
        void hashCodeMatchesEqualPaths() {
            assertThat(ColumnPath.of("bbox", "xmin")).hasSameHashCodeAs(new ColumnPath("bbox.xmin"));
        }

        @Test
        void notEqualWhenSegmentsDiffer() {
            assertThat(ColumnPath.of("bbox", "xmax")).isNotEqualTo(ColumnPath.of("bbox", "xmin"));
        }
    }
}
