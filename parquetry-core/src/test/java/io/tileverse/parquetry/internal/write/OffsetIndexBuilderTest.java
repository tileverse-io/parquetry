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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.codec.ParquetFormatDeserializer;

class OffsetIndexBuilderTest {

    @Test
    void singlePageRoundTripsThroughThrift() {
        OffsetIndexBuilder builder = new OffsetIndexBuilder();
        builder.appendPage(1024L, 256, 0L);

        OffsetIndex original = builder.finishChunk();
        OffsetIndex parsed = roundTrip(original);

        assertThat(parsed.pageLocations()).containsExactly(new PageLocation(1024L, 256, 0L));
    }

    @Test
    void multiplePagesPreserveOrderAndOffsets() {
        OffsetIndexBuilder builder = new OffsetIndexBuilder();
        builder.appendPage(4L, 100, 0L);
        builder.appendPage(104L, 200, 1024L);
        builder.appendPage(304L, 150, 3072L);

        OffsetIndex parsed = roundTrip(builder.finishChunk());

        assertThat(parsed.pageLocations())
                .containsExactly(
                        new PageLocation(4L, 100, 0L),
                        new PageLocation(104L, 200, 1024L),
                        new PageLocation(304L, 150, 3072L));
    }

    @Test
    void finishChunkClearsBuilderForReuse() {
        OffsetIndexBuilder builder = new OffsetIndexBuilder();
        builder.appendPage(0L, 64, 0L);
        builder.appendPage(64L, 64, 100L);
        OffsetIndex first = builder.finishChunk();

        builder.appendPage(128L, 64, 200L);
        OffsetIndex second = builder.finishChunk();

        assertThat(first.pageLocations()).hasSize(2);
        assertThat(second.pageLocations()).containsExactly(new PageLocation(128L, 64, 200L));
    }

    @Test
    void resetClearsAccumulatedPages() {
        OffsetIndexBuilder builder = new OffsetIndexBuilder();
        builder.appendPage(0L, 64, 0L);
        builder.reset();
        builder.appendPage(64L, 64, 100L);

        OffsetIndex index = builder.finishChunk();

        assertThat(index.pageLocations()).containsExactly(new PageLocation(64L, 64, 100L));
    }

    @Test
    void emptyBuilderFinishProducesEmptyIndex() {
        OffsetIndexBuilder builder = new OffsetIndexBuilder();

        OffsetIndex index = builder.finishChunk();

        assertThat(index.pageLocations()).isEmpty();
    }

    private static OffsetIndex roundTrip(OffsetIndex original) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeOffsetIndex(out, original);
        return ParquetFormatDeserializer.readOffsetIndex(new ByteArrayInputStream(out.toByteArray()));
    }
}
