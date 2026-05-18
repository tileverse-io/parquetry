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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.enums.BoundaryOrder;

public record ColumnIndex(
        List<Boolean> nullPages,
        List<ByteBuffer> minValues,
        List<ByteBuffer> maxValues,
        BoundaryOrder boundaryOrder,
        Optional<List<Long>> nullCounts,
        Optional<List<Long>> repetitionLevelHistograms,
        Optional<List<Long>> definitionLevelHistograms) {

    public ColumnIndex {
        nullPages = List.copyOf(nullPages);
        minValues = minValues.stream().map(ByteBuffer::asReadOnlyBuffer).toList();
        maxValues = maxValues.stream().map(ByteBuffer::asReadOnlyBuffer).toList();
        nullCounts = nullCounts.map(List::copyOf);
        repetitionLevelHistograms = repetitionLevelHistograms.map(List::copyOf);
        definitionLevelHistograms = definitionLevelHistograms.map(List::copyOf);
    }
}
