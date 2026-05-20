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
package io.tileverse.parquetry.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import tools.jackson.databind.json.JsonMapper;

/**
 * Pins Jackson 3's behavior on missing / explicit-{@code null} record-component values. Records that are
 * Jackson-deserialized must null-tolerate {@link List} components in their canonical constructor; {@link Optional}
 * components do not need that treatment under the current Jackson 3.x default mapper.
 *
 * <p>If a future Jackson version changes either of these, this test fails and tells us we need to revisit the
 * null-tolerance pattern across the format records.
 */
class JacksonOptionalProbeTest {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Probe(Optional<String> name, Optional<Integer> count, List<String> tags) {}

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Test
    void missingOptionalsLandAsEmpty() {
        Probe p = MAPPER.readValue("{}", Probe.class);
        assertThat(p.name())
                .as("missing Optional<String> should deserialize to Optional.empty()")
                .isEmpty();
        assertThat(p.count())
                .as("missing Optional<Integer> should deserialize to Optional.empty()")
                .isEmpty();
    }

    @Test
    void explicitNullOptionalsLandAsEmpty() {
        Probe p = MAPPER.readValue("{\"name\":null,\"count\":null,\"tags\":null}", Probe.class);
        assertThat(p.name())
                .as("explicit JSON null on Optional<String> should deserialize to Optional.empty()")
                .isEmpty();
        assertThat(p.count())
                .as("explicit JSON null on Optional<Integer> should deserialize to Optional.empty()")
                .isEmpty();
    }

    @Test
    void missingOrNullListsLandAsRawNull() {
        Probe missing = MAPPER.readValue("{}", Probe.class);
        assertThat(missing.tags())
                .as("missing List<X> arrives as raw null - records must null-tolerate to land as empty")
                .isNull();

        Probe explicitNull = MAPPER.readValue("{\"tags\":null}", Probe.class);
        assertThat(explicitNull.tags())
                .as("explicit JSON null on List<X> also arrives as raw null - same null-tolerance requirement")
                .isNull();
    }

    @Test
    void presentValuesArePreserved() {
        Probe p = MAPPER.readValue("{\"name\":\"x\",\"count\":5,\"tags\":[\"a\",\"b\"]}", Probe.class);
        assertThat(p.name()).contains("x");
        assertThat(p.count()).contains(5);
        assertThat(p.tags()).containsExactly("a", "b");
    }
}
