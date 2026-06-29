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
package io.tileverse.parquetry.data;

import java.util.function.Consumer;

import io.tileverse.parquetry.internal.write.ColumnAccumulator;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Tracks an open list or map scope. A list scope routes element setters into the list's element accumulator; a map
 * scope routes key and value setters into the map's two child accumulators. Both wrap their child setters in a relative
 * path that starts at the repeated wrapper node ({@code element} for a list, {@code key_value} for a map), mirroring
 * how the read path names those columns.
 */
final class ContainerScope {

    static final String ADD_ELEMENT = "addElement";
    static final String END_ELEMENT = "endElement";
    static final String PUT_ENTRY = "putEntry";
    static final String END_ENTRY = "endEntry";

    private final ColumnPath path;
    private final ColumnAccumulator.ListAccumulator listAccumulator;
    private final ColumnAccumulator.MapAccumulator mapAccumulator;
    private final String elementNodeName;
    private final String entryNodeName;
    private final String keyNodeName;
    private final String valueNodeName;
    private boolean entryStarted;

    private ContainerScope(
            ColumnPath path,
            ColumnAccumulator.ListAccumulator listAccumulator,
            ColumnAccumulator.MapAccumulator mapAccumulator,
            String elementNodeName,
            String entryNodeName,
            String keyNodeName,
            String valueNodeName) {
        this.path = path;
        this.listAccumulator = listAccumulator;
        this.mapAccumulator = mapAccumulator;
        this.elementNodeName = elementNodeName;
        this.entryNodeName = entryNodeName;
        this.keyNodeName = keyNodeName;
        this.valueNodeName = valueNodeName;
    }

    static ContainerScope list(
            ColumnPath path, ColumnAccumulator.ListAccumulator listAccumulator, String elementNodeName) {
        return new ContainerScope(path, listAccumulator, null, elementNodeName, null, null, null);
    }

    static ContainerScope map(
            ColumnPath path,
            ColumnAccumulator.MapAccumulator mapAccumulator,
            String entryNodeName,
            String keyNodeName,
            String valueNodeName) {
        return new ContainerScope(path, null, mapAccumulator, null, entryNodeName, keyNodeName, valueNodeName);
    }

    ColumnPath path() {
        return path;
    }

    boolean elementStarted() {
        return entryStarted;
    }

    boolean isList() {
        return listAccumulator != null;
    }

    boolean isMap() {
        return mapAccumulator != null;
    }

    ColumnAccumulator elementAccumulator() {
        return listAccumulator.element();
    }

    void openElement() {
        entryStarted = true;
    }

    void closeElement() {
        requireStarted(END_ELEMENT, ADD_ELEMENT);
        listAccumulator.endElement();
        entryStarted = false;
    }

    void commitElement() {
        listAccumulator.endElement();
    }

    void openEntry() {
        entryStarted = true;
    }

    void closeEntry() {
        requireStarted(END_ENTRY, PUT_ENTRY);
        mapAccumulator.endEntry();
        entryStarted = false;
    }

    /**
     * Stages a value on the leaf addressed by {@code path} within the active element or entry. The path starts at the
     * repeated wrapper node, then navigates struct children of the element (lists) or selects the key or value
     * accumulator (maps).
     */
    void setRelative(ColumnPath path, Consumer<ColumnAccumulator> stage) {
        if (isList()) {
            setListElement(path, stage);
        } else {
            setMapEntry(path, stage);
        }
    }

    private void setListElement(ColumnPath path, Consumer<ColumnAccumulator> stage) {
        requireStarted("element setter", ADD_ELEMENT);
        requireFirstPart(path, elementNodeName);
        ColumnAccumulator target = navigate(listAccumulator.element(), path, 1);
        stage.accept(target);
    }

    private void setMapEntry(ColumnPath path, Consumer<ColumnAccumulator> stage) {
        requireStarted("entry setter", PUT_ENTRY);
        requireFirstPart(path, entryNodeName);
        ColumnAccumulator child = selectEntryChild(path.part(1));
        ColumnAccumulator target = navigate(child, path, 2);
        stage.accept(target);
    }

    private ColumnAccumulator selectEntryChild(String childName) {
        if (childName.equals(keyNodeName)) {
            return mapAccumulator.key();
        }
        if (childName.equals(valueNodeName)) {
            return mapAccumulator.value();
        }
        throw new ParquetWriteException("Map entry has no field named " + childName);
    }

    /**
     * Walks struct children of {@code start} from {@code path} part {@code fromPart} to the addressed leaf, marking
     * each struct present along the way.
     */
    private ColumnAccumulator navigate(ColumnAccumulator start, ColumnPath path, int fromPart) {
        ColumnAccumulator current = start;
        for (int i = fromPart; i < path.numParts(); i++) {
            if (!(current instanceof ColumnAccumulator.StructAccumulator structAcc)) {
                throw new ParquetWriteException("Column " + path.part(i - 1) + " is not a struct");
            }
            structAcc.markPresent();
            current = structAcc.child(path.part(i));
        }
        return current;
    }

    private void requireStarted(String verb, String opener) {
        if (!entryStarted) {
            throw new ParquetWriteException(verb + " called without a matching " + opener);
        }
    }

    private void requireFirstPart(ColumnPath path, String expected) {
        if (path.numParts() == 0 || !path.part(0).equals(expected)) {
            throw new ParquetWriteException(
                    "Relative path " + path.dot() + " does not start with the expected node '" + expected + "'");
        }
    }
}
