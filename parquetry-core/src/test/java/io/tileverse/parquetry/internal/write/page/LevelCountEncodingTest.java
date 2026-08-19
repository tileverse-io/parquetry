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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.write.ColumnContext;
import io.tileverse.parquetry.schema.PrimitiveKind;

class LevelCountEncodingTest {

    @Test
    void encodesOnlyTheCellCountEvenWhenTheLevelArrayIsOversized() throws Exception {
        // The level backing is reused across pages: after a larger page, its tail holds stale entries past the
        // current page's cell count. Those stale entries are non-zero here on purpose -- a zero-filled tail would be
        // indistinguishable from the encoder's own zero padding and would not exercise the count boundary.
        int[] exact = {1, 0, 1};
        int[] oversized = {1, 0, 1, 1, 1, 1, 1, 1}; // length 8, meaningful count 3, stale non-zero tail
        assertThat(encodePage(exact, 3)).isEqualTo(encodePage(oversized, 3));
    }

    // Emits a V2, UNCOMPRESSED, definition-only page whose definition-level block is directly comparable across two
    // calls that differ only in the backing array's length. The value bytes stay identical between the two calls.
    // Any byte difference in the emitted page therefore isolates the level block encoding the wrong count.
    private static byte[] encodePage(int[] definitionLevels, int payloadValueCount) throws Exception {
        ColumnContext column =
                new ColumnContext(0, 1, PrimitiveKind.INT32, ParquetVersion.V2_0, Compression.uncompressed());
        PageWriter writer = new PageWriter(column);

        MemorySegment encodedValues = MemorySegment.ofArray(new byte[] {1, 0, 0, 0, 2, 0, 0, 0});
        PreEncodedPageJob job = new PreEncodedPageJob(
                encodedValues, Encoding.PLAIN, payloadValueCount, 1, payloadValueCount, null, definitionLevels, null);

        GrowableByteSink out = new GrowableByteSink(64);
        writer.writeDataPageV2PreEncoded(job, out);
        return out.toByteArray();
    }
}
