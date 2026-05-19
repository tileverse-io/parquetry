/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.PlainInt32Decoder;
import io.tileverse.parquetry.schema.ColumnPath;

class BasicColumnReaderTest {

    // --- required column (maxRep=0, maxDef=0) ---

    @Test
    void requiredColumnPassesValuesDirectly() {
        // maxRep=0, maxDef=0: no level streams consulted; yield values in order.
        PlainInt32Decoder values = new PlainInt32Decoder();
        ByteBuffer page = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(10);
        page.putInt(20);
        page.putInt(30);
        page.flip();
        values.load(page, 3);

        // LevelDecoder(0) with bitWidth=0: next() always returns 0 without reading bytes.
        // We never call load() on them -- that verifies the reader bypasses them entirely.
        BasicColumnReader reader = new BasicColumnReader(
                ColumnPath.of("id"),
                /* maxRep */ 0,
                /* maxDef */ 0,
                new LevelDecoder(0),
                new LevelDecoder(0),
                values,
                /* totalRows */ 3);

        assertThat(reader.columnPath()).isEqualTo(ColumnPath.of("id"));
        assertThat(reader.maxRepetitionLevel()).isZero();
        assertThat(reader.maxDefinitionLevel()).isZero();

        for (int expected : new int[] {10, 20, 30}) {
            assertThat(reader.hasNext()).isTrue();
            assertThat(reader.currentRepetitionLevel()).isZero();
            assertThat(reader.currentDefinitionLevel()).isZero();
            assertThat(reader.currentValue()).isEqualTo(expected);
            // Calling currentValue() again before consume() is stable (idempotent positioning).
            assertThat(reader.currentValue()).isEqualTo(expected);
            reader.consume();
        }
        assertThat(reader.hasNext()).isFalse();
    }

    @Test
    void requiredColumnThrowsWhenExhausted() {
        PlainInt32Decoder values = new PlainInt32Decoder();
        ByteBuffer page = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(42);
        page.flip();
        values.load(page, 1);

        BasicColumnReader reader =
                new BasicColumnReader(ColumnPath.of("x"), 0, 0, new LevelDecoder(0), new LevelDecoder(0), values, 1);

        reader.consume();
        assertThat(reader.hasNext()).isFalse();
        assertThatThrownBy(reader::currentValue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("x");
    }

    // --- optional column (maxRep=0, maxDef=1) ---

    @Test
    void optionalColumnYieldsNullForBelowMaxDef() {
        // maxRep=0, maxDef=1; three rows: value(10), null, value(20)
        // Def levels: [1, 0, 1] -- bit-packed at bitWidth=1.
        // Only 2 values decoded because the middle row has def=0.
        PlainInt32Decoder values = new PlainInt32Decoder();
        ByteBuffer valuePage = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        valuePage.putInt(10);
        valuePage.putInt(20);
        valuePage.flip();
        values.load(valuePage, 2);

        LevelDecoder defDec = new LevelDecoder(1);
        // Pad to 8 values for a single bit-packed group: [1, 0, 1, 0, 0, 0, 0, 0]
        defDec.load(encodeBitPacked(new int[] {1, 0, 1, 0, 0, 0, 0, 0}, 1));

        BasicColumnReader reader = new BasicColumnReader(
                ColumnPath.of("opt"),
                /* maxRep */ 0,
                /* maxDef */ 1,
                new LevelDecoder(0),
                defDec,
                values,
                /* totalRows */ 3);

        // Row 1: def=1 -> non-null value 10
        assertThat(reader.hasNext()).isTrue();
        assertThat(reader.currentDefinitionLevel()).isEqualTo(1);
        assertThat(reader.currentRepetitionLevel()).isZero();
        assertThat(reader.currentValue()).isEqualTo(10);
        reader.consume();

        // Row 2: def=0 -> null
        assertThat(reader.hasNext()).isTrue();
        assertThat(reader.currentDefinitionLevel()).isZero();
        assertThat(reader.currentValue()).isNull();
        reader.consume();

        // Row 3: def=1 -> non-null value 20
        assertThat(reader.hasNext()).isTrue();
        assertThat(reader.currentDefinitionLevel()).isEqualTo(1);
        assertThat(reader.currentValue()).isEqualTo(20);
        reader.consume();

        assertThat(reader.hasNext()).isFalse();
    }

    // --- repeated column (maxRep=1, maxDef=1) ---

    @Test
    void repeatedColumnTracksRepetitionLevels() {
        // maxRep=1, maxDef=1; three rows.
        // Rep levels: [0, 1, 0] -- new row, repeated element, new row.
        // Def levels: [1, 1, 1] -- all non-null.
        // Values: [5, 6, 7].
        PlainInt32Decoder values = new PlainInt32Decoder();
        ByteBuffer valuePage = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        valuePage.putInt(5);
        valuePage.putInt(6);
        valuePage.putInt(7);
        valuePage.flip();
        values.load(valuePage, 3);

        LevelDecoder repDec = new LevelDecoder(1);
        // Pad to 8: [0, 1, 0, 0, 0, 0, 0, 0]
        repDec.load(encodeBitPacked(new int[] {0, 1, 0, 0, 0, 0, 0, 0}, 1));

        LevelDecoder defDec = new LevelDecoder(1);
        // Pad to 8: [1, 1, 1, 0, 0, 0, 0, 0]
        defDec.load(encodeBitPacked(new int[] {1, 1, 1, 0, 0, 0, 0, 0}, 1));

        BasicColumnReader reader = new BasicColumnReader(
                ColumnPath.of("tags"), /* maxRep */ 1, /* maxDef */ 1, repDec, defDec, values, /* totalRows */ 3);

        // Row 1: rep=0 (new top-level row), def=1, value=5
        assertThat(reader.hasNext()).isTrue();
        assertThat(reader.currentRepetitionLevel()).isZero();
        assertThat(reader.currentDefinitionLevel()).isEqualTo(1);
        assertThat(reader.currentValue()).isEqualTo(5);
        reader.consume();

        // Row 2: rep=1 (repeated element), def=1, value=6
        assertThat(reader.hasNext()).isTrue();
        assertThat(reader.currentRepetitionLevel()).isEqualTo(1);
        assertThat(reader.currentDefinitionLevel()).isEqualTo(1);
        assertThat(reader.currentValue()).isEqualTo(6);
        reader.consume();

        // Row 3: rep=0 (new top-level row), def=1, value=7
        assertThat(reader.hasNext()).isTrue();
        assertThat(reader.currentRepetitionLevel()).isZero();
        assertThat(reader.currentDefinitionLevel()).isEqualTo(1);
        assertThat(reader.currentValue()).isEqualTo(7);
        reader.consume();

        assertThat(reader.hasNext()).isFalse();
    }

    // --- consume() without prior level/value access ---

    @Test
    void consumeWithoutPriorAccessAdvancesRow() {
        // Consuming without reading levels/values should still advance the row counter.
        PlainInt32Decoder values = new PlainInt32Decoder();
        ByteBuffer page = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(100);
        page.putInt(200);
        page.flip();
        values.load(page, 2);

        BasicColumnReader reader =
                new BasicColumnReader(ColumnPath.of("v"), 0, 0, new LevelDecoder(0), new LevelDecoder(0), values, 2);

        // Skip first row entirely without reading it.
        reader.consume();
        assertThat(reader.hasNext()).isTrue();
        assertThat(reader.currentValue()).isEqualTo(200);
        reader.consume();
        assertThat(reader.hasNext()).isFalse();
    }

    // --- helper ---

    /**
     * Encodes the given values into one bit-packed group of 8 values at {@code bitWidth}. The group header is {@code (1
     * << 1) | 1} (one group, bit-packed flag). Values are packed LSB-first per the Parquet RLE-bit-packed hybrid spec.
     */
    private static ByteBuffer encodeBitPacked(int[] vals, int bitWidth) {
        if (vals.length != 8) {
            throw new IllegalArgumentException("Must supply exactly 8 values for one bit-packed group");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Header: groups=1, bit-packed flag (low bit = 1) -> varint = (1 << 1) | 1 = 3
        writeVarint(out, (1L << 1) | 1L);
        // Pack values LSB-first.
        long buf = 0L;
        int bits = 0;
        for (int v : vals) {
            buf |= ((long) v) << bits;
            bits += bitWidth;
            while (bits >= 8) {
                out.write((int) (buf & 0xff));
                buf >>>= 8;
                bits -= 8;
            }
        }
        if (bits > 0) {
            out.write((int) (buf & 0xff));
        }
        return ByteBuffer.wrap(out.toByteArray());
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7fL) != 0L) {
            out.write((int) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }
}
