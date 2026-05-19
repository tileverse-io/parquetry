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
package io.tileverse.parquetry.materializer;

import io.tileverse.parquetry.assembly.RowAccessor;
import io.tileverse.parquetry.record.DefaultParquetRecord;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.Schema;

/**
 * Built-in {@link Materializer} that wraps each row in a {@link DefaultParquetRecord}.
 *
 * <p>Stateless and shared as a singleton via {@link Materializer#defaultRecord()}; callers should reach for it through
 * that factory rather than constructing this class directly.
 */
// Stateless dispatcher exposed through Materializer.defaultRecord(); singleton is the intended shape.
@SuppressWarnings("java:S6548")
final class DefaultMaterializer implements Materializer<ParquetRecord> {

    static final DefaultMaterializer INSTANCE = new DefaultMaterializer();

    private DefaultMaterializer() {}

    @Override
    public ParquetRecord materialize(Schema projectedSchema, RowAccessor row) {
        return new DefaultParquetRecord(projectedSchema, row);
    }
}
