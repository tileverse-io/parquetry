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
package io.tileverse.parquetry.data;

import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.internal.write.ColumnAccumulator;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.UuidConverter;
import io.tileverse.parquetry.variant.Variant;

/**
 * Accumulates authored rows into one columnar buffer per leaf column and freezes them into an immutable
 * {@link ParquetRecordBatch}.
 *
 * <p>Cells are staged column by column through the typed {@code set*} setters (addressed by leaf index or by
 * {@link ColumnPath}), and each authored row is closed with {@link #endRow()}. A column left unset for a row defaults
 * to null when the leaf is {@link Repetition#OPTIONAL}, while an unset {@link Repetition#REQUIRED} leaf fails the row.
 * {@link #build()} turns the buffers into a heap-backed batch.
 *
 * <p>Struct columns are handled through the {@link #beginStruct(ColumnPath)} / {@link #endStruct()} scope protocol.
 * Open a struct scope before setting its child fields; {@link #endStruct()} marks the struct present for the row. Call
 * {@link #setNull(ColumnPath)} on the struct path to null the entire struct (children are automatically nulled). A
 * {@link Repetition#REPEATED} leaf or group is rejected at construction.
 *
 * <p>Two terminal modes exist. A standalone builder ({@link #forSchema}) accumulates every authored row and produces
 * one heap-backed batch through {@link #build()}; the caller decides when to stop. A builder bound to a
 * {@link ParquetFileWriter} ({@link ParquetFileWriter#appender()}) auto-flushes a batch to its writer once it reaches
 * {@code flushThresholdRows} rows or its accumulated cells reach {@code flushThresholdBytes}, and on an explicit
 * {@link #flush()}, keeping heap bounded for unbounded row-at-a-time producers. The byte threshold bounds the transient
 * authoring heap and the per-batch row-group overshoot independently of the per-cell size. The two terminals are
 * mutually exclusive: {@link #build()} rejects a writer-bound builder, and {@link #flush()} rejects a standalone
 * builder.
 */
public final class ParquetRecordBatchBuilder implements AutoCloseable {

    static final int DEFAULT_BATCH_ROWS = 8192;
    static final long DEFAULT_BATCH_BYTES = 16L * 1024 * 1024;
    // Check for interrupt every 1024 rows: a cheap bitmask cadence that bounds cancellation latency without a per-row
    // branch cost.
    private static final int INTERRUPT_CHECK_ROW_MASK = 1023;

    private final ParquetSchema schema;
    private final List<ColumnPath> leaves;
    private final Map<ColumnPath, Integer> indexByPath;
    private final ColumnAccumulator[] accumulators;
    private final boolean[] required;
    private final boolean[] setThisRow;

    /**
     * Top-level accumulators keyed by the top-level column name. For flat primitive columns this is the primitive
     * accumulator itself; for struct groups this is the {@link ColumnAccumulator.StructAccumulator} that owns the child
     * accumulators for all descendant leaves.
     */
    private final Map<String, ColumnAccumulator> topLevel;

    /**
     * Whether a top-level column is a REQUIRED list, map, or Variant container. Such a column is authored through a
     * scope verb (beginList / beginMap / setVariant) rather than an index setter; the builder's leaf-indexed required
     * check never reaches it, and endRow checks these explicitly, rejecting a row that left one unset.
     */
    private final boolean[] topLevelRequiredContainer;

    /** Names of the top-level columns in schema order, for endRow iteration. */
    private final List<String> topLevelNames;

    /**
     * For each leaf index, the chain of enclosing {@link ColumnAccumulator.StructAccumulator} instances from outermost
     * to innermost. Null for flat (non-struct-nested) leaves. When an index-based setter authors a value for a struct
     * child leaf, every accumulator in this chain must be marked present for the row; otherwise
     * {@link ColumnAccumulator.StructAccumulator#endRow()} overwrites the authored value with null.
     */
    private final ColumnAccumulator.StructAccumulator[][] leafEnclosingStructs;

    /**
     * Active struct scopes, innermost on top. Each entry is the StructAccumulator currently being populated.
     * beginStruct pushes; endStruct pops and marks the struct present.
     */
    private final Deque<StructScope> structScopeStack = new ArrayDeque<>();

    /**
     * Active list and map scopes, innermost on top. A list scope routes element setters into the list's element
     * accumulator; a map scope routes key/value setters into the map's key and value accumulators. While a scope is
     * open, path-based setters address the active element by its relative path rather than the top-level leaf arrays.
     */
    private final Deque<ContainerScope> containerScopeStack = new ArrayDeque<>();

    private final ParquetFileWriter boundWriter;
    private final int flushThresholdRows;
    private final long flushThresholdBytes;
    private int rows;

    // Counts authored value bytes only (not validity, offsets, or accumulator overhead); a soft lower bound used purely
    // to trigger an auto-flush.
    private long approxBatchBytes;

    public static ParquetRecordBatchBuilder forSchema(ParquetSchema schema) {
        return new ParquetRecordBatchBuilder(schema, null, 0, DEFAULT_BATCH_BYTES);
    }

    static ParquetRecordBatchBuilder boundTo(
            ParquetFileWriter writer, ParquetSchema schema, int flushThresholdRows, long flushThresholdBytes) {
        return new ParquetRecordBatchBuilder(schema, writer, flushThresholdRows, flushThresholdBytes);
    }

    private ParquetRecordBatchBuilder(
            ParquetSchema schema, ParquetFileWriter boundWriter, int flushThresholdRows, long flushThresholdBytes) {
        this.schema = schema;
        this.boundWriter = boundWriter;
        this.flushThresholdRows = flushThresholdRows;
        this.flushThresholdBytes = flushThresholdBytes;
        BatchBuilderLayout layout = new BatchBuilderLayout(schema);
        this.leaves = layout.leaves();
        this.indexByPath = layout.indexByPath();
        this.accumulators = layout.accumulators();
        this.required = layout.required();
        this.setThisRow = new boolean[accumulators.length];
        this.leafEnclosingStructs = layout.leafEnclosingStructs();
        this.topLevel = layout.topLevel();
        this.topLevelRequiredContainer = layout.topLevelRequiredContainer();
        this.topLevelNames = layout.topLevelNames();
    }

    // --- struct scope protocol ---

    /**
     * Opens a struct scope for the struct group at {@code path}. Subsequent {@code setXxx} calls route to the child
     * accumulators of this struct. Close the scope with {@link #endStruct()}.
     */
    public ParquetRecordBatchBuilder beginStruct(ColumnPath path) {
        ColumnAccumulator.StructAccumulator structAcc = resolveStructAccumulator(path);
        structScopeStack.push(new StructScope(path, structAcc));
        return this;
    }

    /** Opens a struct scope by top-level column name. Equivalent to {@code beginStruct(ColumnPath.of(name))}. */
    public ParquetRecordBatchBuilder beginStruct(String name) {
        return beginStruct(ColumnPath.of(name));
    }

    /**
     * Closes the innermost open struct scope and marks the struct row as present. The child accumulators' staged values
     * are committed on the enclosing {@link #endRow()}.
     */
    public ParquetRecordBatchBuilder endStruct() {
        if (structScopeStack.isEmpty()) {
            throw new ParquetWriteException("endStruct called without a matching beginStruct");
        }
        StructScope scope = structScopeStack.pop();
        scope.structAcc.markPresent();
        return this;
    }

    private ColumnAccumulator.StructAccumulator resolveStructAccumulator(ColumnPath path) {
        // Navigate from the top-level accumulator down to the struct at path.
        ColumnAccumulator acc = topLevel.get(path.part(0));
        if (acc == null) {
            throw new ParquetWriteException("No such column: " + path.dot());
        }
        for (int i = 1; i < path.numParts(); i++) {
            if (!(acc instanceof ColumnAccumulator.StructAccumulator structAcc)) {
                throw new ParquetWriteException("Column " + path.part(i - 1) + " is not a struct");
            }
            acc = structAcc.child(path.part(i));
        }
        if (!(acc instanceof ColumnAccumulator.StructAccumulator structAcc)) {
            throw new ParquetWriteException("Column " + path.dot() + " is not a struct group");
        }
        return structAcc;
    }

    // --- list scope protocol ---

    /**
     * Opens a list scope for the list column at {@code path}, marking the list present for the row. Author elements
     * with {@link #addInt}/{@link #addLong}/{@code add*} (scalar elements) or
     * {@link #addElement()}/{@link #endElement()} (struct or nested elements). A list with no element authored before
     * {@link #endList()} is an empty (present) list.
     */
    public ParquetRecordBatchBuilder beginList(ColumnPath path) {
        ContainerScope enclosing = containerScopeStack.peek();
        if (enclosing != null) {
            return beginNestedList(enclosing, path);
        }
        ColumnAccumulator.ListAccumulator listAcc = resolveListAccumulator(path);
        listAcc.markPresent();
        String elementNodeName = listElementNodeName(path);
        containerScopeStack.push(ContainerScope.list(path, listAcc, elementNodeName));
        return this;
    }

    /**
     * Opens a list nested inside the active element or entry through structs, addressed by its relative path from the
     * repeated wrapper node ({@code element} / {@code key_value}). The enclosing element stays open and is still closed
     * by its own {@link #endElement()} / {@link #endEntry()}. A container that is ITSELF the current element or entry
     * value has no relative name and is opened with {@link #addList()} instead.
     */
    private ParquetRecordBatchBuilder beginNestedList(ContainerScope enclosing, ColumnPath path) {
        requireOpenElementForNestedBegin(enclosing, "list", path);
        ColumnAccumulator.ListAccumulator inner = requireInnerList(enclosing.relativeTarget(path, "beginList"), path);
        inner.markPresent();
        ColumnPath absolutePath = relativeToAbsolute(enclosing, path);
        containerScopeStack.push(ContainerScope.list(absolutePath, inner, listElementNodeName(absolutePath)));
        return this;
    }

    /** The name of a list's element node ({@code element} in the standard three-level encoding). */
    private String listElementNodeName(ColumnPath path) {
        SchemaNode.Group listGroup = requireGroup(path, "list");
        SchemaNode repeated = listGroup.children().get(0);
        if (repeated instanceof SchemaNode.Primitive) {
            return repeated.name();
        }
        SchemaNode.Group repeatedGroup = (SchemaNode.Group) repeated;
        if (repeatedGroup.repetition() != Repetition.REPEATED) {
            return repeated.name();
        }
        return repeatedGroup.children().get(0).name();
    }

    /** Opens a list scope by top-level column name. Equivalent to {@code beginList(ColumnPath.of(name))}. */
    public ParquetRecordBatchBuilder beginList(String name) {
        return beginList(ColumnPath.of(name));
    }

    /**
     * Opens an element scope inside the active list. Subsequent path-based setters address the element by its relative
     * path under the list's element node. Close the element with {@link #endElement()}.
     */
    public ParquetRecordBatchBuilder addElement() {
        ContainerScope scope = requireListScope(ContainerScope.ADD_ELEMENT);
        scope.openElement();
        return this;
    }

    /** Closes the innermost element scope and commits the element into the list. */
    public ParquetRecordBatchBuilder endElement() {
        ContainerScope scope = requireListScope(ContainerScope.END_ELEMENT);
        scope.closeElement();
        return this;
    }

    /**
     * Opens a list that is directly the current element of the active list scope, or the current entry's value of the
     * active map scope. In a list scope this starts a fresh element whose content is the inner list, and closing the
     * inner scope with {@link #endList()} commits that element ({@link #addElement()}/{@link #endElement()} are not
     * involved). In a map scope it requires an open entry and stages the inner list as the entry's value; the entry is
     * committed by {@link #endEntry()} as usual.
     */
    public ParquetRecordBatchBuilder addList() {
        ContainerScope parent = requireOpenContainerScope("addList");
        if (parent.isList()) {
            requireNoExplicitElement(parent, "addList");
            ColumnPath innerPath = listElementContainerPath(parent.path());
            ColumnAccumulator.ListAccumulator inner = requireInnerList(parent.elementAccumulator(), innerPath);
            inner.markPresent();
            containerScopeStack.push(ContainerScope.listAsElement(innerPath, inner, listElementNodeName(innerPath)));
            return this;
        }
        requireOpenEntry(parent, "addList");
        ColumnPath innerPath = mapValueContainerPath(parent.path());
        ColumnAccumulator.ListAccumulator inner = requireInnerList(parent.mapValueAccumulator(), innerPath);
        inner.markPresent();
        containerScopeStack.push(ContainerScope.list(innerPath, inner, listElementNodeName(innerPath)));
        return this;
    }

    /**
     * Opens a map that is directly the current element of the active list scope, or the current entry's value of the
     * active map scope; the exact counterpart of {@link #addList()} for map-typed elements and values.
     */
    public ParquetRecordBatchBuilder addMap() {
        ContainerScope parent = requireOpenContainerScope("addMap");
        if (parent.isList()) {
            requireNoExplicitElement(parent, "addMap");
            ColumnPath innerPath = listElementContainerPath(parent.path());
            ColumnAccumulator.MapAccumulator inner = requireInnerMap(parent.elementAccumulator(), innerPath);
            inner.markPresent();
            containerScopeStack.push(innerMapScope(innerPath, inner, true));
            return this;
        }
        requireOpenEntry(parent, "addMap");
        ColumnPath innerPath = mapValueContainerPath(parent.path());
        ColumnAccumulator.MapAccumulator inner = requireInnerMap(parent.mapValueAccumulator(), innerPath);
        inner.markPresent();
        containerScopeStack.push(innerMapScope(innerPath, inner, false));
        return this;
    }

    private ContainerScope innerMapScope(
            ColumnPath innerPath, ColumnAccumulator.MapAccumulator inner, boolean asElement) {
        SchemaNode.Group keyValue = mapKeyValueNode(innerPath);
        String keyName = keyValue.children().get(0).name();
        String valueName = keyValue.children().get(1).name();
        if (asElement) {
            return ContainerScope.mapAsElement(innerPath, inner, keyValue.name(), keyName, valueName);
        }
        return ContainerScope.map(innerPath, inner, keyValue.name(), keyName, valueName);
    }

    /** The schema path of a list's element node: the single child of the list's repeated child group. */
    private ColumnPath listElementContainerPath(ColumnPath listPath) {
        SchemaNode.Group listGroup = requireGroup(listPath, "list");
        SchemaNode repeated = listGroup.children().get(0);
        if (!(repeated instanceof SchemaNode.Group repeatedGroup)) {
            throw new ParquetWriteException(
                    "List " + listPath.dot() + " has a primitive element; author it with the scalar add* verbs");
        }
        SchemaNode element = repeatedGroup.children().get(0);
        return childPath(listPath, repeatedGroup.name(), element.name());
    }

    /** The schema path of a map's value node under its {@code key_value} group. */
    private ColumnPath mapValueContainerPath(ColumnPath mapPath) {
        SchemaNode.Group keyValue = mapKeyValueNode(mapPath);
        SchemaNode value = keyValue.children().get(1);
        return childPath(mapPath, keyValue.name(), value.name());
    }

    private static ColumnPath childPath(ColumnPath base, String... names) {
        List<String> parts = new ArrayList<>(base.numParts() + names.length);
        for (int i = 0; i < base.numParts(); i++) {
            parts.add(base.part(i));
        }
        parts.addAll(Arrays.asList(names));
        return ColumnPath.of(parts);
    }

    private ContainerScope requireOpenContainerScope(String verb) {
        ContainerScope scope = containerScopeStack.peek();
        if (scope == null) {
            throw new ParquetWriteException(verb + " called without an open list or map scope");
        }
        return scope;
    }

    private void requireNoExplicitElement(ContainerScope scope, String verb) {
        if (scope.elementStarted()) {
            throw new IllegalStateException(
                    verb + " must not be mixed with addElement()/endElement() within one element");
        }
    }

    private void requireOpenEntry(ContainerScope scope, String verb) {
        if (!scope.elementStarted()) {
            throw new ParquetWriteException(verb + " requires an open map entry; call putEntry() first");
        }
    }

    private ColumnAccumulator.ListAccumulator requireInnerList(ColumnAccumulator acc, ColumnPath innerPath) {
        if (!(acc instanceof ColumnAccumulator.ListAccumulator inner)) {
            throw new ParquetWriteException("Column " + innerPath.dot() + " is not a list group");
        }
        return inner;
    }

    private ColumnAccumulator.MapAccumulator requireInnerMap(ColumnAccumulator acc, ColumnPath innerPath) {
        if (!(acc instanceof ColumnAccumulator.MapAccumulator inner)) {
            throw new ParquetWriteException("Column " + innerPath.dot() + " is not a map group");
        }
        return inner;
    }

    /** Appends one int element to the active list. Shorthand for an {@link #addElement()} of a scalar element. */
    public ParquetRecordBatchBuilder addInt(int value) {
        return addScalarElement(accumulator -> accumulator.setInt(value), Integer.BYTES);
    }

    /** Appends one long element to the active list. */
    public ParquetRecordBatchBuilder addLong(long value) {
        return addScalarElement(accumulator -> accumulator.setLong(value), Long.BYTES);
    }

    /** Appends one double element to the active list. */
    public ParquetRecordBatchBuilder addDouble(double value) {
        return addScalarElement(accumulator -> accumulator.setDouble(value), Double.BYTES);
    }

    /** Appends one float element to the active list. */
    public ParquetRecordBatchBuilder addFloat(float value) {
        return addScalarElement(accumulator -> accumulator.setFloat(value), Float.BYTES);
    }

    /** Appends one boolean element to the active list. */
    public ParquetRecordBatchBuilder addBoolean(boolean value) {
        return addScalarElement(accumulator -> accumulator.setBoolean(value), 1);
    }

    /** Appends one binary element to the active list. */
    public ParquetRecordBatchBuilder addBinary(MemorySegment value) {
        return addScalarElement(accumulator -> accumulator.setBinary(value), value.byteSize());
    }

    /** Appends one UTF-8 string element to the active list. */
    public ParquetRecordBatchBuilder addString(String value) {
        return addBinary(
                MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)).asReadOnly());
    }

    /** Appends one null element to the active list. */
    public ParquetRecordBatchBuilder addNull() {
        return addScalarElement(ColumnAccumulator::setNull, 0);
    }

    /**
     * Stages one scalar element on the active list's element accumulator. {@code valueBytes} is the per-value byte
     * estimate this element adds to the auto-flush threshold, mirroring the index-based setters so a batch of a few
     * rows holding large lists trips the byte-bound flush rather than growing unbounded on heap.
     */
    private ParquetRecordBatchBuilder addScalarElement(Consumer<ColumnAccumulator> stage, long valueBytes) {
        ContainerScope scope = requireScalarElementScope();
        stage.accept(scope.elementAccumulator());
        scope.commitElement();
        approxBatchBytes += valueBytes;
        return this;
    }

    /**
     * Requires an open list scope with no explicit element scope started. A scalar {@code add*} verb commits its own
     * element; mixing it with {@link #addElement()}/{@link #endElement()} within one element would double-commit.
     */
    private ContainerScope requireScalarElementScope() {
        ContainerScope scope = requireListScope(ContainerScope.ADD_ELEMENT);
        if (scope.elementStarted()) {
            throw new IllegalStateException(
                    "scalar element verbs (addInt/addLong/...) must not be mixed with addElement()/endElement() "
                            + "within one element");
        }
        return scope;
    }

    /**
     * Closes the innermost open list scope. A list opened with {@link #addList()} commits itself as the enclosing
     * list's element; a top-level or entry-value list is committed by the enclosing {@link #endRow()} /
     * {@link #endEntry()}.
     */
    public ParquetRecordBatchBuilder endList() {
        ContainerScope scope = requireListScope("endList");
        containerScopeStack.pop();
        commitParentElementIfNeeded(scope);
        return this;
    }

    /** Commits the enclosing list's current element when the closed scope was opened as that element. */
    private void commitParentElementIfNeeded(ContainerScope closed) {
        if (closed.commitsParentElement()) {
            containerScopeStack.peek().commitElement();
        }
    }

    private ColumnAccumulator.ListAccumulator resolveListAccumulator(ColumnPath path) {
        ColumnAccumulator acc = resolveContainerAccumulator(path);
        if (!(acc instanceof ColumnAccumulator.ListAccumulator listAcc)) {
            throw new ParquetWriteException("Column " + path.dot() + " is not a list group");
        }
        return listAcc;
    }

    private ContainerScope requireListScope(String verb) {
        ContainerScope scope = containerScopeStack.peek();
        if (scope == null || !scope.isList()) {
            throw new ParquetWriteException(verb + " called without an open list scope");
        }
        return scope;
    }

    // --- map scope protocol ---

    /**
     * Opens a map scope for the map column at {@code path}, marking the map present for the row. Author entries with
     * {@link #putEntry()}/{@link #endEntry()}, setting the entry's key and value by their relative paths under the
     * map's {@code key_value} node. A map with no entry authored before {@link #endMap()} is an empty (present) map.
     */
    public ParquetRecordBatchBuilder beginMap(ColumnPath path) {
        ContainerScope enclosing = containerScopeStack.peek();
        if (enclosing != null) {
            return beginNestedMap(enclosing, path);
        }
        ColumnAccumulator.MapAccumulator mapAcc = resolveMapAccumulator(path);
        mapAcc.markPresent();
        SchemaNode.Group keyValue = mapKeyValueNode(path);
        String keyName = keyValue.children().get(0).name();
        String valueName = keyValue.children().get(1).name();
        containerScopeStack.push(ContainerScope.map(path, mapAcc, keyValue.name(), keyName, valueName));
        return this;
    }

    /** The {@link #beginNestedList} counterpart for maps nested inside the active element or entry through structs. */
    private ParquetRecordBatchBuilder beginNestedMap(ContainerScope enclosing, ColumnPath path) {
        requireOpenElementForNestedBegin(enclosing, "map", path);
        ColumnAccumulator.MapAccumulator inner = requireInnerMap(enclosing.relativeTarget(path, "beginMap"), path);
        inner.markPresent();
        ColumnPath absolutePath = relativeToAbsolute(enclosing, path);
        containerScopeStack.push(innerMapScope(absolutePath, inner, false));
        return this;
    }

    private void requireOpenElementForNestedBegin(ContainerScope enclosing, String kind, ColumnPath path) {
        if (!enclosing.elementStarted()) {
            throw new ParquetWriteException("Cannot open " + kind + " " + path.dot()
                    + " by path inside the open container scope "
                    + enclosing.path().dot()
                    + " without an open element or entry; use addList()/addMap() for a container that is itself the"
                    + " current element or entry value, or open the element first (addElement()/putEntry())");
        }
    }

    /**
     * The absolute schema path of a scope-relative container path. A list-relative path anchors at the element node and
     * gains the repeated child segment; a map-relative path already starts at the {@code key_value} node.
     */
    private ColumnPath relativeToAbsolute(ContainerScope enclosing, ColumnPath relative) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < enclosing.path().numParts(); i++) {
            parts.add(enclosing.path().part(i));
        }
        if (enclosing.isList()) {
            SchemaNode.Group listGroup = requireGroup(enclosing.path(), "list");
            parts.add(listGroup.children().get(0).name());
        }
        for (int i = 0; i < relative.numParts(); i++) {
            parts.add(relative.part(i));
        }
        return ColumnPath.of(parts);
    }

    /** The {@code key_value} node of a map. */
    private SchemaNode.Group mapKeyValueNode(ColumnPath path) {
        SchemaNode.Group mapGroup = requireGroup(path, "map");
        return (SchemaNode.Group) mapGroup.children().get(0);
    }

    private SchemaNode.Group requireGroup(ColumnPath path, String kind) {
        SchemaNode node =
                schema.find(path).orElseThrow(() -> new ParquetWriteException("No such column: " + path.dot()));
        if (!(node instanceof SchemaNode.Group group)) {
            throw new ParquetWriteException("Column " + path.dot() + " is not a " + kind + " group");
        }
        return group;
    }

    /** Opens a map scope by top-level column name. Equivalent to {@code beginMap(ColumnPath.of(name))}. */
    public ParquetRecordBatchBuilder beginMap(String name) {
        return beginMap(ColumnPath.of(name));
    }

    /**
     * Opens an entry scope inside the active map. Set the entry's key and value through the path-based setters
     * addressed by their relative paths under {@code key_value}. Close the entry with {@link #endEntry()}.
     */
    public ParquetRecordBatchBuilder putEntry() {
        ContainerScope scope = requireMapScope(ContainerScope.PUT_ENTRY);
        scope.openEntry();
        return this;
    }

    /** Closes the innermost entry scope and commits the entry into the map. */
    public ParquetRecordBatchBuilder endEntry() {
        ContainerScope scope = requireMapScope(ContainerScope.END_ENTRY);
        scope.closeEntry();
        return this;
    }

    /**
     * Closes the innermost open map scope. A map opened with {@link #addMap()} commits itself as the enclosing list's
     * element; a top-level or entry-value map is committed by the enclosing {@link #endRow()} / {@link #endEntry()}.
     */
    public ParquetRecordBatchBuilder endMap() {
        ContainerScope scope = requireMapScope("endMap");
        containerScopeStack.pop();
        commitParentElementIfNeeded(scope);
        return this;
    }

    private ColumnAccumulator.MapAccumulator resolveMapAccumulator(ColumnPath path) {
        ColumnAccumulator acc = resolveContainerAccumulator(path);
        if (!(acc instanceof ColumnAccumulator.MapAccumulator mapAcc)) {
            throw new ParquetWriteException("Column " + path.dot() + " is not a map group");
        }
        return mapAcc;
    }

    private ContainerScope requireMapScope(String verb) {
        ContainerScope scope = containerScopeStack.peek();
        if (scope == null || !scope.isMap()) {
            throw new ParquetWriteException(verb + " called without an open map scope");
        }
        return scope;
    }

    /**
     * Resolves the list or map accumulator addressed by {@code path}, relative to the innermost open struct scope. When
     * a struct scope is open, {@code path} must extend that scope's path; the tail navigates into the struct's child
     * accumulators, and every struct on the way (including the scope's own struct) is marked present so an enclosing
     * struct is not nulled at {@link #endRow()}. When no struct scope is open, {@code path} addresses a top-level
     * column.
     */
    private ColumnAccumulator resolveContainerAccumulator(ColumnPath path) {
        StructScope scope = structScopeStack.peek();
        if (scope == null) {
            return resolveTopLevelAccumulator(path);
        }
        return resolveContainerInStructScope(path, scope);
    }

    private ColumnAccumulator resolveTopLevelAccumulator(ColumnPath path) {
        ColumnAccumulator acc = topLevel.get(path.part(0));
        if (acc == null) {
            throw new ParquetWriteException("No such column: " + path.dot());
        }
        return acc;
    }

    /**
     * Navigates from an open struct scope to the container accumulator at {@code path}. The path must start with the
     * scope's struct path; the remaining parts walk the struct's children. The scope's struct and every nested struct
     * on the way are marked present.
     */
    private ColumnAccumulator resolveContainerInStructScope(ColumnPath path, StructScope scope) {
        int prefixLength = scope.path.numParts();
        requireDescendantOfScope(path, scope);
        scope.structAcc.markPresent();
        ColumnAccumulator current = scope.structAcc;
        for (int i = prefixLength; i < path.numParts(); i++) {
            if (!(current instanceof ColumnAccumulator.StructAccumulator structAcc)) {
                throw new ParquetWriteException("Column " + path.part(i - 1) + " is not a struct");
            }
            structAcc.markPresent();
            current = structAcc.child(path.part(i));
        }
        return current;
    }

    private void requireDescendantOfScope(ColumnPath path, StructScope scope) {
        ColumnPath scopePath = scope.path;
        if (path.numParts() <= scopePath.numParts()) {
            throw new ParquetWriteException(
                    "Path " + path.dot() + " is not nested inside the open struct scope " + scopePath.dot());
        }
        for (int i = 0; i < scopePath.numParts(); i++) {
            if (!path.part(i).equals(scopePath.part(i))) {
                throw new ParquetWriteException(
                        "Path " + path.dot() + " is not nested inside the open struct scope " + scopePath.dot());
            }
        }
    }

    // --- primitive setters by leaf index ---

    public ParquetRecordBatchBuilder setBoolean(int col, boolean value) {
        accumulators[col].setBoolean(value);
        markEnclosingStructsPresent(col);
        setThisRow[col] = true;
        approxBatchBytes += 1;
        return this;
    }

    public ParquetRecordBatchBuilder setInt(int col, int value) {
        accumulators[col].setInt(value);
        markEnclosingStructsPresent(col);
        setThisRow[col] = true;
        approxBatchBytes += 4;
        return this;
    }

    public ParquetRecordBatchBuilder setLong(int col, long value) {
        accumulators[col].setLong(value);
        markEnclosingStructsPresent(col);
        setThisRow[col] = true;
        approxBatchBytes += 8;
        return this;
    }

    public ParquetRecordBatchBuilder setFloat(int col, float value) {
        accumulators[col].setFloat(value);
        markEnclosingStructsPresent(col);
        setThisRow[col] = true;
        approxBatchBytes += 4;
        return this;
    }

    public ParquetRecordBatchBuilder setDouble(int col, double value) {
        accumulators[col].setDouble(value);
        markEnclosingStructsPresent(col);
        setThisRow[col] = true;
        approxBatchBytes += 8;
        return this;
    }

    public ParquetRecordBatchBuilder setBinary(int col, MemorySegment value) {
        accumulators[col].setBinary(value);
        markEnclosingStructsPresent(col);
        setThisRow[col] = true;
        approxBatchBytes += value.byteSize();
        return this;
    }

    public ParquetRecordBatchBuilder setString(int col, String value) {
        return setBinary(col, MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)));
    }

    public ParquetRecordBatchBuilder setUuid(int col, UUID value) {
        return setBinary(col, UuidConverter.toReadOnlySegment(value));
    }

    public ParquetRecordBatchBuilder setNull(int col) {
        if (required[col]) {
            throw new ParquetWriteException("Required column " + leaves.get(col).dot() + " cannot be set to null");
        }
        accumulators[col].setNull();
        markEnclosingStructsPresent(col);
        setThisRow[col] = true;
        return this;
    }

    /**
     * Marks every struct accumulator enclosing the leaf at {@code col} as present for the current row. Called by every
     * non-null index-based setter; a struct whose children are authored via index setters (without an explicit
     * {@link #beginStruct}/{@link #endStruct} scope) is then not incorrectly treated as absent at {@link #endRow()}.
     */
    private void markEnclosingStructsPresent(int col) {
        ColumnAccumulator.StructAccumulator[] chain = leafEnclosingStructs[col];
        if (chain == null) {
            return;
        }
        for (ColumnAccumulator.StructAccumulator struct : chain) {
            struct.markPresent();
        }
    }

    // --- primitive setters by ColumnPath ---

    public ParquetRecordBatchBuilder setBoolean(ColumnPath path, boolean value) {
        if (inContainerScope()) {
            return setInActiveElement(path, accumulator -> accumulator.setBoolean(value), 1);
        }
        return setBoolean(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setInt(ColumnPath path, int value) {
        if (inContainerScope()) {
            return setInActiveElement(path, accumulator -> accumulator.setInt(value), Integer.BYTES);
        }
        return setInt(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setLong(ColumnPath path, long value) {
        if (inContainerScope()) {
            return setInActiveElement(path, accumulator -> accumulator.setLong(value), Long.BYTES);
        }
        return setLong(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setFloat(ColumnPath path, float value) {
        if (inContainerScope()) {
            return setInActiveElement(path, accumulator -> accumulator.setFloat(value), Float.BYTES);
        }
        return setFloat(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setDouble(ColumnPath path, double value) {
        if (inContainerScope()) {
            return setInActiveElement(path, accumulator -> accumulator.setDouble(value), Double.BYTES);
        }
        return setDouble(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setBinary(ColumnPath path, MemorySegment value) {
        if (inContainerScope()) {
            return setInActiveElement(path, accumulator -> accumulator.setBinary(value), value.byteSize());
        }
        return setBinary(indexOf(path), value);
    }

    public ParquetRecordBatchBuilder setString(ColumnPath path, String value) {
        return setBinary(
                path,
                MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)).asReadOnly());
    }

    public ParquetRecordBatchBuilder setUuid(ColumnPath path, UUID value) {
        return setBinary(path, UuidConverter.toReadOnlySegment(value));
    }

    // --- Variant verb ---

    /**
     * Sets the unshredded Variant column at {@code path} to {@code variant} for the current row. The Variant's value
     * buffer and its metadata dictionary are staged into the column's two binary leaves. A null Variant column is
     * authored through {@link #setNull(ColumnPath)} on the same path.
     */
    public ParquetRecordBatchBuilder setVariant(ColumnPath path, Variant variant) {
        ColumnAccumulator acc = resolveVariantAccumulator(path);
        MemorySegment metadata = variant.metadata().rawSegment();
        MemorySegment value = variant.value();
        stageVariant(acc, variant);
        approxBatchBytes += metadata.byteSize() + value.byteSize();
        return this;
    }

    /** Sets the Variant column by top-level name. Equivalent to {@code setVariant(ColumnPath.of(name), variant)}. */
    public ParquetRecordBatchBuilder setVariant(String topLevelName, Variant variant) {
        return setVariant(ColumnPath.of(topLevelName), variant);
    }

    private static void stageVariant(ColumnAccumulator acc, Variant variant) {
        switch (acc) {
            case ColumnAccumulator.VariantAccumulator unshredded ->
                unshredded.setVariant(variant.metadata().rawSegment(), variant.value());
            case ColumnAccumulator.ShreddedVariantAccumulator shredded -> shredded.setVariant(variant);
            default -> throw new IllegalStateException("not a Variant accumulator: " + acc.getClass());
        }
    }

    private ColumnAccumulator resolveVariantAccumulator(ColumnPath path) {
        ColumnAccumulator acc = resolveContainerAccumulator(path);
        if (!isVariantAccumulator(acc)) {
            throw new ParquetWriteException("Column " + path.dot() + " is not a Variant group");
        }
        return acc;
    }

    private static boolean isVariantAccumulator(ColumnAccumulator acc) {
        return acc instanceof ColumnAccumulator.VariantAccumulator
                || acc instanceof ColumnAccumulator.ShreddedVariantAccumulator;
    }

    private boolean inContainerScope() {
        return !containerScopeStack.isEmpty();
    }

    /**
     * Stages a value on the leaf accumulator addressed by {@code path} within the active element or entry. Marking
     * every struct enclosing that leaf present keeps the element's struct from being nulled when it is committed.
     * {@code valueBytes} mirrors the index-based setter's per-value estimate into the auto-flush threshold so an
     * element or entry value counts toward the byte-bound flush just like a top-level cell.
     */
    private ParquetRecordBatchBuilder setInActiveElement(
            ColumnPath path, Consumer<ColumnAccumulator> stage, long valueBytes) {
        ContainerScope scope = containerScopeStack.peek();
        scope.setRelative(path, stage);
        approxBatchBytes += valueBytes;
        return this;
    }

    /**
     * Sets null for the leaf or struct group at {@code path}. When {@code path} addresses a struct group, nulls the
     * entire struct (all its descendant leaves are null for this row). When it addresses a leaf, nulls just that leaf.
     * Inside an open list or map scope, the path addresses a leaf within the active element or entry.
     */
    public ParquetRecordBatchBuilder setNull(ColumnPath path) {
        if (inContainerScope()) {
            return setInActiveElement(path, ColumnAccumulator::setNull, 0);
        }
        ColumnAccumulator acc = topLevel.get(path.part(0));
        if (acc == null) {
            throw new ParquetWriteException("No such column: " + path.dot());
        }
        if (acc instanceof ColumnAccumulator.ListAccumulator || acc instanceof ColumnAccumulator.MapAccumulator) {
            // A null container: setNull clears its presence; its own endRow writes the empty offset span for this row.
            acc.setNull();
            return this;
        }
        if (isVariantAccumulator(acc)) {
            if (path.numParts() > 1) {
                throw new ParquetWriteException("Variant children are not addressable; null the whole Variant column '"
                        + path.part(0) + "' instead of '" + path.dot() + "'");
            }
            // A null Variant: setNull clears its presence; its own endRow nulls the metadata, value, and typed
            // children.
            acc.setNull();
            return this;
        }
        // Navigate to the target node.
        for (int i = 1; i < path.numParts(); i++) {
            if (!(acc instanceof ColumnAccumulator.StructAccumulator structAcc)) {
                throw new ParquetWriteException(
                        "Column " + path.dot() + " not found: " + path.part(i - 1) + " is not a struct");
            }
            acc = structAcc.child(path.part(i));
        }
        if (acc instanceof ColumnAccumulator.StructAccumulator structAcc) {
            // Null the whole struct: the StructAccumulator.setNull marks the struct absent; endRow propagates nulls to
            // children.
            structAcc.setNull();
            // Mark all descendant leaves as set for this row to keep endRow from double-nulling them.
            markStructLeavesSet(path);
        } else if (acc instanceof ColumnAccumulator.ListAccumulator
                || acc instanceof ColumnAccumulator.MapAccumulator) {
            // A null container nested inside a struct clears its own presence. The enclosing struct stays present
            // because a null nested list still belongs to a present struct row.
            acc.setNull();
            markEnclosingStructsPresentForContainer(path);
        } else {
            // Leaf null via the index-based path.
            setNull(indexOf(path));
        }
        return this;
    }

    /**
     * Marks the structs enclosing the container at {@code path} present for the row. A struct-nested list or map that
     * is nulled still belongs to a present struct; the enclosing struct must not be nulled at {@link #endRow()}.
     */
    private void markEnclosingStructsPresentForContainer(ColumnPath path) {
        ColumnAccumulator acc = topLevel.get(path.part(0));
        for (int i = 1; i < path.numParts(); i++) {
            if (!(acc instanceof ColumnAccumulator.StructAccumulator structAcc)) {
                return;
            }
            structAcc.markPresent();
            acc = structAcc.child(path.part(i));
        }
    }

    /**
     * Marks all leaf indices descended from the struct at {@code structPath} as set for this row. This prevents endRow
     * from filling the same leaves a second time when the whole struct is nulled.
     */
    private void markStructLeavesSet(ColumnPath structPath) {
        String prefix = structPath.dot() + ".";
        for (int i = 0; i < leaves.size(); i++) {
            String leafDot = leaves.get(i).dot();
            if (leafDot.startsWith(prefix) || leafDot.equals(structPath.dot())) {
                setThisRow[i] = true;
            }
        }
    }

    /**
     * Closes the current row. An unset REQUIRED column throws {@link ParquetWriteException}; after such a programming
     * error the builder is left partially advanced and must be discarded.
     */
    public void endRow() {
        if (!structScopeStack.isEmpty()) {
            throw new ParquetWriteException("endRow called with an open struct scope: "
                    + structScopeStack.peek().path.dot());
        }
        if (!containerScopeStack.isEmpty()) {
            throw new ParquetWriteException("endRow called with an open list or map scope: "
                    + containerScopeStack.peek().path().dot());
        }
        for (int i = 0; i < accumulators.length; i++) {
            // List and map element leaves have no flat accumulator slot; their container drives them in its own endRow.
            if (accumulators[i] != null && !setThisRow[i]) {
                fillUnsetColumn(i);
            }
        }
        rejectUnsetRequiredContainers();
        // Drive endRow on top-level accumulators. Struct accumulators call endRow on their children internally.
        for (String name : topLevelNames) {
            topLevel.get(name).endRow();
        }
        // Reset per-row tracking.
        for (int i = 0; i < setThisRow.length; i++) {
            setThisRow[i] = false;
        }
        rows++;
        if (boundWriter == null) {
            return;
        }
        if ((rows & INTERRUPT_CHECK_ROW_MASK) == 0) {
            boundWriter.checkInterrupt();
        }
        if (rows >= flushThresholdRows || approxBatchBytes >= flushThresholdBytes) {
            flush();
        }
    }

    /**
     * Rejects a row that left a REQUIRED list, map, or Variant column unset. These columns are authored through scope
     * verbs rather than index setters, hence the leaf-level required check does not reach them; a present empty list or
     * map is authored with beginList / beginMap and endList / endMap, which is distinct from never authoring the
     * column.
     */
    private void rejectUnsetRequiredContainers() {
        for (int t = 0; t < topLevelNames.size(); t++) {
            if (!topLevelRequiredContainer[t]) {
                continue;
            }
            String name = topLevelNames.get(t);
            if (!topLevel.get(name).isPendingPresent()) {
                throw new ParquetWriteException("Required column '" + name + "' was not set");
            }
        }
    }

    public int rowCount() {
        return rows;
    }

    /**
     * The running estimate of authored value bytes in the pending batch. Counts value payloads only (not validity,
     * offsets, or accumulator overhead); a soft lower bound. External producers batching through a standalone builder
     * poll this after each {@link #endRow()} to freeze a batch on a byte budget the way a writer-bound appender does.
     */
    public long approxBatchBytes() {
        return approxBatchBytes;
    }

    /**
     * Freezes the accumulated rows into one immutable heap-backed batch. Valid only for a standalone builder; a
     * writer-bound appender must use {@link #flush()} instead, which would otherwise bypass the writer.
     */
    public ParquetRecordBatch build() {
        if (boundWriter != null) {
            throw new ParquetWriteException("build() is for a standalone batch; a writer-bound appender uses flush()");
        }
        return freezeBatch();
    }

    /**
     * Writes the pending rows as one batch to the bound writer and resets the accumulators for the next batch. A no-op
     * when no rows are pending. Valid only for a writer-bound appender; a standalone builder has no writer to flush to
     * and must use {@link #build()}.
     */
    public void flush() {
        if (boundWriter == null) {
            throw new ParquetWriteException(
                    "flush() requires an appender bound to a writer; use build() for a standalone batch");
        }
        drainTo(boundWriter::writeBatch);
    }

    /**
     * Drains the pending rows into the bound writer's close-time append path, which skips the open check that
     * {@link #flush()} relies on. The writer calls this while finalizing, after it has marked itself closed.
     */
    void flushOnClose() {
        drainTo(boundWriter::writeClosingBatch);
    }

    private void drainTo(Consumer<ParquetRecordBatch> batchSink) {
        if (rows == 0) {
            return;
        }
        ParquetRecordBatch batch = freezeBatch();
        try {
            batchSink.accept(batch);
        } catch (UncheckedIOException e) {
            throw new ParquetWriteException("Appender flush failed", e.getCause());
        }
        resetAccumulators();
    }

    /**
     * Flushes a writer-bound appender's pending rows. A standalone builder (created via {@link #forSchema}) has no
     * writer to flush to and close() is a no-op; call {@link #build()} to obtain its batch.
     */
    @Override
    public void close() {
        if (boundWriter != null) {
            flush();
        }
    }

    private ParquetRecordBatch freezeBatch() {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        for (String name : topLevelNames) {
            columns.put(ColumnPath.of(name), topLevel.get(name).freeze());
        }
        return DefaultParquetRecordBatch.ofHeap(schema, columns, rows);
    }

    private void resetAccumulators() {
        for (ColumnAccumulator acc : topLevel.values()) {
            acc.clear();
        }
        rows = 0;
        approxBatchBytes = 0;
    }

    private void fillUnsetColumn(int col) {
        if (required[col] && enclosingStructsAllPresent(col)) {
            throw new ParquetWriteException(
                    "Required column " + leaves.get(col).dot() + " was not set for row " + rows);
        }
        // Make the unset-optional-as-null intent explicit, independent of accumulator row-reset internals.
        accumulators[col].setNull();
    }

    /**
     * Returns true when every enclosing struct in the leaf's chain is present for the current row, meaning the leaf is
     * reachable and a REQUIRED leaf with no authored value is a genuine authoring error. Returns true for flat
     * (non-struct-nested) leaves (they have no enclosing struct to be absent).
     */
    private boolean enclosingStructsAllPresent(int col) {
        ColumnAccumulator.StructAccumulator[] chain = leafEnclosingStructs[col];
        if (chain == null) {
            return true;
        }
        for (ColumnAccumulator.StructAccumulator struct : chain) {
            if (!struct.isPendingPresent()) {
                return false;
            }
        }
        return true;
    }

    private int indexOf(ColumnPath path) {
        Integer index = indexByPath.get(path);
        if (index == null) {
            throw new ParquetWriteException("No such column: " + path.dot());
        }
        return index;
    }

    /** Tracks an open struct scope on the beginStruct/endStruct stack. */
    private record StructScope(ColumnPath path, ColumnAccumulator.StructAccumulator structAcc) {}
}
