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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AvroFormatExceptionTest {

    @Test
    void isUnchecked() {
        assertThat(new AvroFormatException("boom")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void keepsMessageAndCause() {
        Throwable cause = new IllegalStateException("root");
        AvroFormatException exception = new AvroFormatException("boom", cause);
        assertThat(exception).hasMessage("boom").hasCause(cause);
    }
}
