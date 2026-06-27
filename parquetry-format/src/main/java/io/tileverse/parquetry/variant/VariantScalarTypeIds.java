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
package io.tileverse.parquetry.variant;

/** The Variant spec scalar primitive type ids shared by the encoder, decoder, and shredded-Variant classifier. */
public final class VariantScalarTypeIds {

    public static final int TYPE_BOOLEAN_TRUE = 1;
    public static final int TYPE_BOOLEAN_FALSE = 2;
    public static final int TYPE_INT8 = 3;
    public static final int TYPE_INT16 = 4;
    public static final int TYPE_INT32 = 5;
    public static final int TYPE_INT64 = 6;
    public static final int TYPE_DOUBLE = 7;
    public static final int TYPE_DECIMAL4 = 8;
    public static final int TYPE_DECIMAL8 = 9;
    public static final int TYPE_DECIMAL16 = 10;
    public static final int TYPE_DATE = 11;
    public static final int TYPE_TIMESTAMP_TZ_MICROS = 12;
    public static final int TYPE_TIMESTAMP_NTZ_MICROS = 13;
    public static final int TYPE_FLOAT = 14;
    public static final int TYPE_BINARY = 15;
    public static final int TYPE_STRING = 16;
    public static final int TYPE_TIME = 17;
    public static final int TYPE_TIMESTAMP_TZ_NANOS = 18;
    public static final int TYPE_TIMESTAMP_NTZ_NANOS = 19;
    public static final int TYPE_UUID = 20;

    private VariantScalarTypeIds() {}
}
