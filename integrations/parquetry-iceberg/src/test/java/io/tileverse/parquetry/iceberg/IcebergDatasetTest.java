/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.CatalogSnapshot;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Drives {@link IcebergDataset} through its per-file reconciliation: a file matching the table by field id reads
 * untouched (identity {@link Query}), while an evolved table schema reconciles the same data file into a renamed,
 * null-filled output shape and folds the predicate against the columns the file does not hold.
 */
class IcebergDatasetTest {

    private static final String TABLE = "v2_flat_columns";

    @TempDir
    Path tempDir;

    private final List<AutoCloseable> openResources = new ArrayList<>();

    @AfterEach
    void closeResources() throws Exception {
        for (AutoCloseable resource : openResources) {
            resource.close();
        }
    }

    @Test
    void buildsIdentityQueryWhenFileMatchesTableByFieldId() {
        IcebergDataset dataset = datasetWithSchema(currentFields());

        Query query = dataset.oneFileQueryForTest(0, Predicate.ALWAYS_TRUE, Projection.ALL);

        assertThat(query.outputColumns()).isEmpty();
        assertThat(query.predicate()).isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(query.projection()).isEqualTo(Projection.ALL);
    }

    @Test
    void buildsReconciledQueryWhenTableRenamesAField() {
        IcebergDataset dataset = datasetWithSchema(renamedIdFields());

        Query query = dataset.oneFileQueryForTest(0, Predicate.ALWAYS_TRUE, Projection.ALL);

        assertThat(query.projection()).isInstanceOf(Projection.Of.class);
        Projection.Of of = (Projection.Of) query.projection();
        assertThat(of.columns())
                .contains(new Projection.Column.Physical(ColumnPath.of("identifier"), ColumnPath.of("id")));
    }

    @Test
    void keepsAReservedLineageNameUntouchedBelowFormatV3() {
        // The corpus table is format version 2: row lineage does not exist, and a projection naming _row_id is
        // passed through as an ordinary column instead of being rewritten to a synthesized lineage column.
        IcebergDataset dataset = datasetWithSchema(currentFields());
        Projection lineageNamed = Projection.ofPhysical(List.of(ColumnPath.of("id"), ColumnPath.of("_row_id")));

        Query query = dataset.oneFileQueryForTest(0, Predicate.ALWAYS_TRUE, lineageNamed);

        assertThat(query.projection()).isEqualTo(lineageNamed);
    }

    @Test
    void locatesAMaterializedLineageLeafByReservedFieldId() {
        IcebergFileSchema file = fileSchemaOf(leaf("id", 1), leaf("row_id_alt", Integer.MAX_VALUE - 107));

        Optional<IcebergFileSchema.FileColumn> found =
                IcebergDataset.materializedLineageColumn(file, Integer.MAX_VALUE - 107, ColumnPath.of("_row_id"));

        assertThat(found).map(IcebergFileSchema.FileColumn::path).contains(ColumnPath.of("row_id_alt"));
    }

    @Test
    void fallsBackToTheReservedNameWhenTheFileHasNoFieldIds() {
        IcebergFileSchema file = fileSchemaOf(leaf("id", -1), leaf("_row_id", -1));

        Optional<IcebergFileSchema.FileColumn> found =
                IcebergDataset.materializedLineageColumn(file, Integer.MAX_VALUE - 107, ColumnPath.of("_row_id"));

        assertThat(found).map(IcebergFileSchema.FileColumn::path).contains(ColumnPath.of("_row_id"));
    }

    @Test
    void neverMatchesByNameWhenTheFileHasFieldIds() {
        // A column merely named _row_id with an ordinary field id is user data, not a materialized lineage leaf.
        IcebergFileSchema file = fileSchemaOf(leaf("id", 1), leaf("_row_id", 2));

        Optional<IcebergFileSchema.FileColumn> found =
                IcebergDataset.materializedLineageColumn(file, Integer.MAX_VALUE - 107, ColumnPath.of("_row_id"));

        assertThat(found).isEmpty();
    }

    @Test
    void aMappedUserColumnNamedLikeALineageColumnStaysUserData() {
        IcebergNameMapping mapping = IcebergNameMapping.fromJson("[{\"field-id\": 7, \"names\": [\"_row_id\"]}]");
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf("_row_id", -1)), Optional.empty(), -1);
        IcebergFileSchema file = IcebergFileSchema.of(new ParquetSchema(root), mapping);

        Optional<IcebergFileSchema.FileColumn> found =
                IcebergDataset.materializedLineageColumn(file, Integer.MAX_VALUE - 107, ColumnPath.of("_row_id"));

        assertThat(found).isEmpty();
    }

    private static IcebergFileSchema fileSchemaOf(SchemaNode... leaves) {
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaves), Optional.empty(), -1);
        return IcebergFileSchema.of(new ParquetSchema(root));
    }

    private static SchemaNode.Primitive leaf(String name, int fieldId) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), fieldId);
    }

    @Test
    void foldsAnAddedColumnNullPredicateToKeepEveryRow() {
        IcebergDataset dataset = datasetWithSchema(fieldsWithAddedColumn());
        Predicate addedIsNull = new Predicate.IsNull(ColumnPath.of("extra"));

        Query query = dataset.oneFileQueryForTest(0, addedIsNull, Projection.ALL);

        assertThat(query).isNotNull();
        assertThat(query.predicate()).isEqualTo(Predicate.ALWAYS_TRUE);
    }

    @Test
    void skipsAFileWhenAnAddedColumnEqualityCannotMatch() {
        IcebergDataset dataset = datasetWithSchema(fieldsWithAddedColumn());
        Predicate addedEquals = new Predicate.Eq(ColumnPath.of("extra"), new Value.LongVal(5L));

        Query query = dataset.oneFileQueryForTest(0, addedEquals, Projection.ALL);

        assertThat(query).isNull();
    }

    @Test
    void skipsAFileWhenAConstantColumnEqualityCannotMatch() {
        IcebergDataset dataset = datasetWithSchema(fieldsWithDefaultedColumn());
        Predicate constantEqualsOther = new Predicate.Eq(ColumnPath.of("tier"), new Value.LongVal(2L));

        Query query = dataset.oneFileQueryForTest(0, constantEqualsOther, Projection.ALL);

        assertThat(query).isNull();
    }

    @Test
    void keepsAFileWhenAConstantColumnEqualityMatches() {
        IcebergDataset dataset = datasetWithSchema(fieldsWithDefaultedColumn());
        Predicate constantEqualsDefault = new Predicate.Eq(ColumnPath.of("tier"), new Value.LongVal(7L));

        Query query = dataset.oneFileQueryForTest(0, constantEqualsDefault, Projection.ALL);

        assertThat(query).isNotNull();
        assertThat(query.predicate()).isEqualTo(Predicate.ALWAYS_TRUE);
    }

    @Test
    void readsEveryRowThroughTheReconciledRenamePath() {
        IcebergDataset dataset = datasetWithSchema(renamedIdFields());

        long decoded;
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            decoded = rows.count();
        }

        assertThat(decoded).isEqualTo(10_000L);
        assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS)).isEqualTo(10_000L);
    }

    @Test
    void countMatchesThePassThroughPathForACorpusTable() {
        IcebergDataset dataset = datasetWithSchema(currentFields());

        assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS)).isEqualTo(10_000L);
    }

    private IcebergDataset datasetWithSchema(List<IcebergField> tableFields) {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        Path tableDir = root.resolve(TABLE);
        IcebergFileIO bootstrap = StorageIcebergFileIO.owning(
                StorageFactory.open(tableDir.toUri()), tableDir.toUri().toString());
        openResources.add(bootstrap);
        IcebergTableMetadata metadata = readMetadata(tableDir, bootstrap);

        IcebergFileIO io = StorageIcebergFileIO.owning(StorageFactory.open(tableDir.toUri()), metadata.tableLocation());
        openResources.add(io);
        List<IcebergManifests.DataFileRef> dataFiles =
                IcebergManifests.readDataFiles(metadata.manifestListLocation(), io);
        List<IcebergField> currentFields = metadata.fields();
        List<FileStats> fileStats = new ArrayList<>(dataFiles.size());
        List<ByteRangeSource> sources = new ArrayList<>(dataFiles.size());
        for (IcebergManifests.DataFileRef ref : dataFiles) {
            fileStats.add(IcebergFileStats.from(ref, currentFields, Map.of()));
            ByteRangeSource source = io.open(ref.location());
            openResources.add(source);
            sources.add(source);
        }

        CatalogSnapshot snapshot =
                new CatalogSnapshot(metadata.currentSnapshotId(), metadata.currentSnapshotTimestampMs());
        IcebergPartitionSpec partitionSpec = IcebergPartitionSpec.of(List.of(), List.of());
        IcebergDeletePlan deletePlan = IcebergDeletePlan.of(List.of(), io, null, IcebergNameMapping.empty());
        return new IcebergDataset(
                TABLE,
                snapshot,
                IcebergSchema.of(tableFields),
                partitionSpec,
                dataFiles,
                fileStats,
                sources,
                deletePlan,
                metadata.formatVersion(),
                IcebergNameMapping.empty(),
                OpenOptions.DEFAULTS);
    }

    private static IcebergTableMetadata readMetadata(Path tableDir, IcebergFileIO io) {
        String metadataLocation =
                IcebergMetadataResolver.resolve(io, tableDir.toUri().toString(), IcebergOptions.defaults());
        return IcebergTableMetadata.read(readUtf8(io, metadataLocation), IcebergOptions.defaults());
    }

    private static String readUtf8(IcebergFileIO io, String location) {
        try (ByteRangeSource source = io.open(location)) {
            long size = source.size();
            byte[] bytes = new byte[Math.toIntExact(size)];
            source.readFully(0, MemorySegment.ofArray(bytes));
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static List<IcebergField> currentFields() {
        return List.of(
                new IcebergField(1, "id", "string", false),
                new IcebergField(2, "xmin", "double", false),
                new IcebergField(3, "ymin", "double", false),
                new IcebergField(4, "xmax", "double", false),
                new IcebergField(5, "ymax", "double", false));
    }

    private static List<IcebergField> renamedIdFields() {
        List<IcebergField> fields = new ArrayList<>(currentFields());
        fields.set(0, new IcebergField(1, "identifier", "string", false));
        return fields;
    }

    private static List<IcebergField> fieldsWithAddedColumn() {
        List<IcebergField> fields = new ArrayList<>(currentFields());
        fields.add(new IcebergField(99, "extra", "long", false));
        return fields;
    }

    /**
     * The current fields plus a column the data file does not hold whose schema declares an {@code initial-default}.
     * Reconciliation presents it as a constant for every row, and a predicate over it folds against that default.
     */
    private static List<IcebergField> fieldsWithDefaultedColumn() {
        List<IcebergField> fields = new ArrayList<>(currentFields());
        fields.add(new IcebergField(99, "tier", "long", false, Optional.of(new Value.LongVal(7L))));
        return fields;
    }
}
