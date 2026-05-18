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
package io.tileverse.parquetry.assembly;

import java.util.Objects;

import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.PageDecoder;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Ties a {@link PageDecoder} with rep/def {@link LevelDecoder}s into a row-by-row view.
 *
 * <p>Positioning is lazy: the first call to any {@code current*()} method on a fresh (or just
 * consumed) row pulls the next set of levels and a value from the underlying decoders and caches
 * them until {@link #consume()} is called. This lets the Dremel record assembler read each field
 * multiple times without re-reading from the byte streams.
 *
 * <p>For REQUIRED columns (maxRepetitionLevel == 0 and maxDefinitionLevel == 0) the level decoders
 * are bypassed entirely -- every row has rep=0, def=0, and a non-null value.
 */
public final class BasicColumnReader implements ColumnReader {

    private final ColumnPath columnPath;
    private final int maxRepetitionLevel;
    private final int maxDefinitionLevel;
    private final LevelDecoder repetitionDecoder;
    private final LevelDecoder definitionDecoder;
    private final PageDecoder<?> valueDecoder;
    private final int totalRows;

    private int rowsRead;
    private int currentRepLevel;
    private int currentDefLevel;
    private Object currentValue;
    private boolean positioned;

    public BasicColumnReader(
            ColumnPath columnPath,
            int maxRepetitionLevel,
            int maxDefinitionLevel,
            LevelDecoder repetitionDecoder,
            LevelDecoder definitionDecoder,
            PageDecoder<?> valueDecoder,
            int totalRows) {
        this.columnPath = Objects.requireNonNull(columnPath, "columnPath");
        this.maxRepetitionLevel = maxRepetitionLevel;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.repetitionDecoder = Objects.requireNonNull(repetitionDecoder, "repetitionDecoder");
        this.definitionDecoder = Objects.requireNonNull(definitionDecoder, "definitionDecoder");
        this.valueDecoder = Objects.requireNonNull(valueDecoder, "valueDecoder");
        this.totalRows = totalRows;
        this.rowsRead = 0;
        this.positioned = false;
    }

    @Override
    public boolean hasNext() {
        return rowsRead < totalRows;
    }

    @Override
    public int currentRepetitionLevel() {
        positionIfNeeded();
        return currentRepLevel;
    }

    @Override
    public int currentDefinitionLevel() {
        positionIfNeeded();
        return currentDefLevel;
    }

    @Override
    public Object currentValue() {
        positionIfNeeded();
        return currentValue;
    }

    @Override
    public void consume() {
        // Ensure the current row is positioned before we skip over it, so that
        // the underlying decoders are advanced for rows that are consumed without
        // any currentXxx() call.
        positionIfNeeded();
        positioned = false;
        rowsRead++;
    }

    @Override
    public ColumnPath columnPath() {
        return columnPath;
    }

    @Override
    public int maxRepetitionLevel() {
        return maxRepetitionLevel;
    }

    @Override
    public int maxDefinitionLevel() {
        return maxDefinitionLevel;
    }

    // --- internal ---

    private void positionIfNeeded() {
        if (!positioned) {
            positionNext();
        }
    }

    private void positionNext() {
        if (rowsRead >= totalRows) {
            throw new IllegalStateException("No more rows to read in column " + columnPath.dot());
        }
        currentRepLevel = readRepetitionLevel();
        currentDefLevel = readDefinitionLevel();
        currentValue = (currentDefLevel == maxDefinitionLevel) ? valueDecoder.next() : null;
        positioned = true;
    }

    /**
     * Returns 0 for REQUIRED (maxRep==0) columns without consulting the level decoder; otherwise
     * reads the next rep level from the stream.
     */
    private int readRepetitionLevel() {
        return (maxRepetitionLevel == 0) ? 0 : repetitionDecoder.next();
    }

    /**
     * Returns maxDef for REQUIRED (maxDef==0) columns without consulting the level decoder;
     * otherwise reads the next def level from the stream.
     */
    private int readDefinitionLevel() {
        return (maxDefinitionLevel == 0) ? maxDefinitionLevel : definitionDecoder.next();
    }
}
