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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.util.List;

import org.junit.jupiter.api.Test;

class AvroSchemaRefTest {

    @Test
    void readsTargetAfterBinding() {
        AvroSchema.Ref ref = new AvroSchema.Ref("ns.Inner");
        ref.bind(AvroSchema.LONG);

        assertThat(ref.fullName()).isEqualTo("ns.Inner");
        assertThat(ref.target()).isSameAs(AvroSchema.LONG);
        assertThat(ref.type()).isEqualTo(AvroSchema.Type.LONG);
        assertThat(ref.logicalType()).isEmpty();
    }

    @Test
    void rejectsReadingUnboundRef() {
        AvroSchema.Ref ref = new AvroSchema.Ref("ns.Inner");

        assertThatThrownBy(ref::target).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(ref::type).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsBindingTwice() {
        AvroSchema.Ref ref = new AvroSchema.Ref("ns.Inner");
        ref.bind(AvroSchema.LONG);

        assertThatThrownBy(() -> ref.bind(AvroSchema.LONG)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void decoderFollowsBoundRef() {
        AvroSchema.Record target =
                new AvroSchema.Record("Inner", null, List.of(), List.of(AvroSchema.Field.of("id", 0, AvroSchema.LONG)));
        AvroSchema.Ref ref = new AvroSchema.Ref("Inner");
        ref.bind(target);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        varLong(out, 7);
        AvroBinaryDecoder in =
                new AvroBinaryDecoder(MemorySegment.ofArray(out.toByteArray()).asReadOnly());

        AvroRecord decoded = (AvroRecord) new AvroDatumDecoder().decode(ref, in);
        assertThat(decoded.get("id")).isEqualTo(7L);
    }

    private static void varLong(ByteArrayOutputStream out, long signed) {
        long n = (signed << 1) ^ (signed >> 63);
        while ((n & ~0x7FL) != 0) {
            out.write((int) ((n & 0x7F) | 0x80));
            n >>>= 7;
        }
        out.write((int) n);
    }
}
