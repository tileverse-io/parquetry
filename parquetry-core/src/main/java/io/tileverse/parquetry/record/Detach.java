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
package io.tileverse.parquetry.record;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a lazy batch-backed value into a self-contained copy that owns its data.
 *
 * <p>Values returned from the read path are views over a columnar batch: binary leaves are read-only
 * {@link MemorySegment} slices over per-page heap buffers, list and map cells hold such values, and struct cells are
 * {@link ParquetRecord} sub-records. The views stay valid only while the batch is open. A consumer that retains a value
 * past the batch (a cache, a buffered or sorted feature collection) pins the batch's backing buffers. Detaching copies
 * the data out into owned, immutable structures that are safe to retain.
 *
 * <p>Detaching is idempotent: detaching an already-detached value yields an equivalent owned value.
 */
final class Detach {

    private Detach() {}

    /**
     * Returns an owned copy of {@code value} that no longer references the batch's backing buffers.
     *
     * <ul>
     *   <li>{@code null} stays {@code null}.
     *   <li>A {@link MemorySegment} becomes a fresh read-only heap segment holding a copy of the bytes.
     *   <li>A {@link List} becomes an unmodifiable list of detached elements, preserving null elements.
     *   <li>A {@link Map} becomes an insertion-ordered unmodifiable map of detached keys to detached values, tolerating
     *       null values.
     *   <li>A {@link ParquetRecord} is detached via {@link ParquetRecord#detach()}.
     *   <li>Everything else (boxed primitives, {@link String}) is already immutable and returned as-is.
     * </ul>
     */
    static Object detach(Object value) {
        return switch (value) {
            case null -> null;
            case MemorySegment segment -> detachSegment(segment);
            case List<?> list -> detachList(list);
            case Map<?, ?> map -> detachMap(map);
            case ParquetRecord nested -> nested.detach();
            default -> value;
        };
    }

    private static MemorySegment detachSegment(MemorySegment segment) {
        return MemorySegment.ofArray(segment.toArray(JAVA_BYTE)).asReadOnly();
    }

    private static List<Object> detachList(List<?> list) {
        List<Object> copy = new ArrayList<>(list.size());
        for (Object element : list) {
            copy.add(detach(element));
        }
        return Collections.unmodifiableList(copy);
    }

    private static Map<Object, Object> detachMap(Map<?, ?> map) {
        Map<Object, Object> copy = LinkedHashMap.newLinkedHashMap(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            copy.put(detach(entry.getKey()), detach(entry.getValue()));
        }
        return Collections.unmodifiableMap(copy);
    }
}
