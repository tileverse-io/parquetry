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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.util.Optional;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.UnsupportedFeatureException;

/**
 * Reads the encoding fields of page headers. A page header names the encodings needed to decode its page: an encoding
 * unknown to parquetry makes the page undecodable.
 */
final class PageHeaderEncodings {

    private static final long UNKNOWN_OFFSET = -1L;

    private PageHeaderEncodings() {}

    /**
     * Reads one encoding field.
     *
     * @param field the Thrift field being read, named in the failure message
     * @throws UnsupportedFeatureException when parquetry does not know the encoding's wire code
     */
    static Encoding read(CompactProtocolReader r, String field) throws IOException {
        int code = r.readI32();
        Optional<Encoding> encoding = Encoding.fromCode(code);
        return encoding.orElseThrow(() -> new UnsupportedFeatureException(
                "Unsupported encoding: wire code " + code + " is unknown to parquetry", UNKNOWN_OFFSET, field));
    }
}
