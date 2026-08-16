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
package io.tileverse.parquetry.columnar;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A map column held in the streaming read path's level form: dense entry-aligned descendant leaf vectors plus their
 * batch-owned repetition/definition level windows, with the eager Dremel assembly deferred to a lazy row view. Row
 * views navigate the levels on demand instead of materializing offset-and-child vectors up front.
 *
 * <p>This vector is produced only by the streaming scan; {@code readBatches()} assembles eager {@link MapVector}s and
 * never produces this form. Views materialized over it are valid only while the owning batch is open.
 *
 * <p>{@link #get(int)} keeps the sealed interface default and throws: nested kinds materialize through the
 * materializer, which holds the schema context a collection needs.
 */
public final class LevelMapVector implements ColumnVector, LevelSource {

    private final ParquetSchema schema;
    private final ColumnPath groupPath;
    private final Map<ColumnPath, ColumnVector> leaves;
    private final Map<ColumnPath, LeafLevels> levels;
    private final ColumnPath structureLeaf;
    private final Validity validity;
    private final int size;
    private final MapMeta meta;
    private final LeafOrder leafOrder;

    // S107: private; only the validating static factories call it
    @SuppressWarnings("java:S107")
    private LevelMapVector(
            ParquetSchema schema,
            ColumnPath groupPath,
            Map<ColumnPath, ColumnVector> leaves,
            Map<ColumnPath, LeafLevels> levels,
            ColumnPath structureLeaf,
            Validity validity,
            int size,
            MapMeta meta,
            LeafOrder leafOrder) {
        this.schema = schema;
        this.groupPath = groupPath;
        this.leaves = leaves;
        this.levels = levels;
        this.structureLeaf = structureLeaf;
        this.validity = validity;
        this.size = size;
        this.meta = meta;
        this.leafOrder = leafOrder;
    }

    /**
     * Builds a level-backed map vector for the group at {@code groupPath}. The structure leaf is the first present
     * descendant under the key, else under the value; its level streams define this group's per-row entry windows.
     * Level constants and key/value resolution are computed once into an immutable {@link MapMeta}.
     */
    public static LevelMapVector of(
            ParquetSchema schema,
            ColumnPath groupPath,
            Map<ColumnPath, ColumnVector> leaves,
            Map<ColumnPath, LeafLevels> levels,
            Validity validity,
            int size) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(groupPath, "groupPath");
        Objects.requireNonNull(leaves, "leaves");
        Objects.requireNonNull(levels, "levels");
        Objects.requireNonNull(validity, "validity");
        Map<ColumnPath, ColumnVector> ownLeaves = Map.copyOf(leaves);
        Map<ColumnPath, LeafLevels> ownLevels = Map.copyOf(levels);
        requireMatchingKeySets(ownLeaves, ownLevels);
        SchemaNode.Group group = resolveGroup(schema, groupPath);
        LeafOrdinals leafOrdinals = LeafOrdinals.of(ownLevels.keySet());
        MapMeta meta = MapMeta.of(schema, group, groupPath, ownLeaves.keySet(), leafOrdinals::ordinalOf);
        return of(schema, groupPath, ownLeaves, ownLevels, validity, size, meta, leafOrdinals);
    }

    /**
     * Builds a level-backed map vector over metadata resolved ahead of the batch: {@code meta} holds this group's level
     * constants and key/value resolution, and {@code leafOrdinals} the leaf numbering its scalar ordinals refer to.
     * Both must have been resolved against the leaf paths of {@code leaves}, which is what makes the metadata's
     * ordinals index the level arrays this vector packs.
     */
    // S107: the vector's own inputs plus the two pieces of metadata resolved for it
    @SuppressWarnings("java:S107")
    public static LevelMapVector of(
            ParquetSchema schema,
            ColumnPath groupPath,
            Map<ColumnPath, ColumnVector> leaves,
            Map<ColumnPath, LeafLevels> levels,
            Validity validity,
            int size,
            MapMeta meta,
            LeafOrdinals leafOrdinals) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(groupPath, "groupPath");
        Objects.requireNonNull(validity, "validity");
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(leafOrdinals, "leafOrdinals");
        Map<ColumnPath, ColumnVector> ownLeaves = Map.copyOf(leaves);
        Map<ColumnPath, LeafLevels> ownLevels = Map.copyOf(levels);
        requireMatchingKeySets(ownLeaves, ownLevels);
        LeafOrder leafOrder = LeafOrder.of(leafOrdinals, ownLevels);
        ColumnPath structureLeaf = meta.structureLeaf();
        requireStructureLeafPresent(structureLeaf, ownLeaves, ownLevels);
        requireRowStartsLength(ownLevels, size);
        return new LevelMapVector(
                schema, groupPath, ownLeaves, ownLevels, structureLeaf, validity, size, meta, leafOrder);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /** The schema this map's entries resolve against. */
    @Override
    public ParquetSchema schema() {
        return schema;
    }

    /** The dot path of the map group this vector holds. */
    public ColumnPath groupPath() {
        return groupPath;
    }

    /** The leaf whose level streams define this group's per-row entry windows. */
    public ColumnPath structureLeaf() {
        return structureLeaf;
    }

    /** The dense entry-aligned descendant leaf vector at {@code path}. */
    @Override
    public ColumnVector leaf(ColumnPath path) {
        return leaves.get(path);
    }

    /** The batch-owned level window for the descendant leaf at {@code path}. */
    @Override
    public LeafLevels levels(ColumnPath path) {
        return levels.get(path);
    }

    /** The paths of the descendant leaves present in this vector (those the projection kept). */
    @Override
    public Set<ColumnPath> leafPaths() {
        return leaves.keySet();
    }

    /** The batch-constant leaf order and per-leaf level streams the navigation reads. */
    @Override
    public LeafOrder leafOrder() {
        return leafOrder;
    }

    /** The precomputed level constants and key/value resolution for this map. */
    public MapMeta meta() {
        return meta;
    }

    @Override
    public long approximateHeapBytes() {
        long total = validity.heapBytes();
        for (ColumnVector leaf : leaves.values()) {
            total += leaf.approximateHeapBytes();
        }
        for (LeafLevels leafLevels : levels.values()) {
            total += leafLevels.rowStarts().heapBytes();
        }
        return total;
    }

    private static void requireMatchingKeySets(
            Map<ColumnPath, ColumnVector> leaves, Map<ColumnPath, LeafLevels> levels) {
        if (!leaves.keySet().equals(levels.keySet())) {
            throw new IllegalArgumentException("leaf vectors and level windows must share the same key set");
        }
    }

    private static SchemaNode.Group resolveGroup(ParquetSchema schema, ColumnPath groupPath) {
        SchemaNode node = schema.find(groupPath)
                .orElseThrow(() -> new IllegalArgumentException("no schema node at " + groupPath.dot()));
        if (!(node instanceof SchemaNode.Group group)) {
            throw new IllegalArgumentException("schema node at " + groupPath.dot() + " is not a group");
        }
        return group;
    }

    private static void requireStructureLeafPresent(
            ColumnPath structureLeaf, Map<ColumnPath, ColumnVector> leaves, Map<ColumnPath, LeafLevels> levels) {
        if (structureLeaf == null || !leaves.containsKey(structureLeaf) || !levels.containsKey(structureLeaf)) {
            throw new IllegalArgumentException("the structure leaf must be present in both the leaf and level maps");
        }
    }

    private static void requireRowStartsLength(Map<ColumnPath, LeafLevels> levels, int size) {
        for (Map.Entry<ColumnPath, LeafLevels> entry : levels.entrySet()) {
            int rowStarts = entry.getValue().rowStarts().size();
            if (rowStarts != size + 1) {
                throw new IllegalArgumentException("leaf %s has %d row starts; expected %d"
                        .formatted(entry.getKey().dot(), rowStarts, size + 1));
            }
        }
    }
}
