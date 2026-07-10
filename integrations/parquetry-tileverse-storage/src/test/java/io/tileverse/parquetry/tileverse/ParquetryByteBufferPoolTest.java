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
package io.tileverse.parquetry.tileverse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.ByteBuffer;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;

class ParquetryByteBufferPoolTest {

    private final ParquetryByteBufferPool pool = new ParquetryByteBufferPool();

    @Test
    void borrowDirectReturnsDirectBufferSizedToRequest() {
        int size = 4096;
        try (PooledByteBuffer pooled = pool.borrowDirect(size)) {
            ByteBuffer buffer = pooled.buffer();
            assertThat(buffer.capacity()).as("direct buffer capacity").isEqualTo(size);
            assertThat(buffer.isDirect()).as("buffer is direct").isTrue();
        }
    }

    @Test
    void borrowDirectBufferRoundTripsAByte() {
        try (PooledByteBuffer pooled = pool.borrowDirect(16)) {
            ByteBuffer buffer = pooled.buffer();
            buffer.put(0, (byte) 0x7f);
            assertThat(buffer.get(0)).as("round-tripped byte").isEqualTo((byte) 0x7f);
        }
    }

    @Test
    void closeIsIdempotent() {
        PooledByteBuffer pooled = pool.borrowDirect(64);
        pooled.close();
        assertThatCode(pooled::close).as("second close must not throw").doesNotThrowAnyException();
    }

    @Test
    void borrowHeapReturnsArrayBackedBufferSizedToRequest() {
        int size = 2048;
        try (PooledByteBuffer pooled = pool.borrowHeap(size)) {
            ByteBuffer buffer = pooled.buffer();
            assertThat(buffer.hasArray()).as("heap buffer is array-backed").isTrue();
            assertThat(buffer.capacity()).as("heap buffer capacity").isEqualTo(size);
        }
    }

    @Test
    void manySequentialDirectBorrowsAllReturnUsableBuffers() {
        int size = 1 << 20;
        for (int i = 0; i < 256; i++) {
            try (PooledByteBuffer pooled = pool.borrowDirect(size)) {
                ByteBuffer buffer = pooled.buffer();
                assertThat(buffer.capacity()).as("borrow %d capacity", i).isEqualTo(size);
                buffer.put(0, (byte) i);
                assertThat(buffer.get(0)).as("borrow %d byte", i).isEqualTo((byte) i);
            }
        }
    }

    @Test
    void serviceLoaderDiscoversTheProvider() {
        ServiceLoader<ByteBufferPool> loader = ServiceLoader.load(ByteBufferPool.class);
        boolean found = loader.stream().anyMatch(provider -> provider.type() == ParquetryByteBufferPool.class);
        assertThat(found)
                .as("ServiceLoader must discover ParquetryByteBufferPool via META-INF/services")
                .isTrue();
    }

    @Test
    void getDefaultResolvesToThisProvider() {
        assertThat(ByteBufferPool.getDefault())
                .as("getDefault() must win as the registered provider that vends parquetry's shared pool to tileverse")
                .isInstanceOf(ParquetryByteBufferPool.class);
    }
}
