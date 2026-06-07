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
package io.tileverse.parquetry.geotools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.filter.Filter;
import org.geotools.data.FilteringFeatureReader;
import org.geotools.data.MaxFeatureReader;
import org.geotools.data.ReTypeFeatureReader;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetDatasetCatalog;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Read-only feature source over a single dataset from a {@link ParquetDatasetCatalog}.
 *
 * <p>Query filtering is pushed down to the read path. The query splits into spatial and attribute filters that
 * translate into a parquetry predicate (with exact geometry tests where the relation supports them), a column
 * projection limited to the requested and residual-filter attributes, and any residual filter the predicate could not
 * express. Record reading ({@link #getReaderInternal(Query)}) streams the dataset through
 * {@link GeoParquetFeatureReader} and layers GeoTools decorators that apply the residual filter, retype to the
 * requested attributes, and enforce the feature cap.
 *
 * <p>Unfiltered and fully-pushable counts delegate to {@link ParquetDataset#count()}; unfiltered bounds read the
 * file-level bbox recorded in GeoParquet metadata.
 */
final class GeoParquetFeatureSource extends ContentFeatureSource {

    private final ParquetDatasetCatalog catalog;

    /**
     * Lazily built on first call; protected by the instance monitor so concurrent schema resolution does not duplicate
     * work.
     */
    private GeoParquetSchemaMapper.Mapping mapping;

    GeoParquetFeatureSource(ContentEntry entry, ParquetDatasetCatalog catalog) {
        super(entry, Query.ALL);
        this.catalog = catalog;
    }

    private ParquetDataset dataset() {
        return catalog.dataset(getEntry().getTypeName());
    }

    /**
     * Returns the schema mapping, building it on the first call.
     *
     * <p>Synchronized to prevent duplicate builds under concurrent access - the first caller wins and subsequent
     * callers reuse the result.
     */
    synchronized GeoParquetSchemaMapper.Mapping mapping() throws IOException {
        if (mapping == null) {
            ParquetDataset ds = dataset();
            String fidColumn = ((GeoParquetDataStore) getDataStore()).fidColumn();
            try {
                mapping = GeoParquetSchemaMapper.map(
                        getEntry().getTypeName(),
                        getDataStore().getNamespaceURI(),
                        ds.schema(),
                        ds.keyValueMetadata(),
                        fidColumn);
            } catch (IllegalArgumentException badConfiguration) {
                throw new IOException(badConfiguration.getMessage(), badConfiguration);
            }
        }
        return mapping;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return mapping().featureType();
    }

    /**
     * Returns the exact row count when the query pushes down fully, or {@code -1} to signal that GeoTools should count
     * by iterating the filtered reader when a residual filter remains.
     *
     * <p>A fully pushable query (including the unfiltered case, which translates to an always-true predicate with no
     * residual) is counted by parquetry's count optimization, which prunes whole row groups from statistics without
     * decoding records.
     */
    @Override
    protected int getCountInternal(Query query) throws IOException {
        QueryTranslator.TranslatedQuery t = translate(query);
        if (t.postFilter() != Filter.INCLUDE) {
            return -1;
        }
        long raw = dataset().count(t.predicate());
        // Clamp to Integer.MAX_VALUE; realistic GeoParquet files are smaller.
        int count = raw > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) raw;
        if (!hasOffset(query) && !query.isMaxFeaturesUnlimited()) {
            // Mirror canLimit(query)==true: the base class will not clamp to maxFeatures, this method must.
            // When an offset is present, canLimit is false and the base class applies offset and limit itself,
            // which means this method returns the raw count.
            int max = query.getMaxFeatures();
            if (count > max) {
                count = max;
            }
        }
        return count;
    }

    /**
     * Returns the file-level bounding box for unfiltered queries from the GeoParquet {@code "geo"} metadata, or
     * {@code null} when a filter is present or the metadata is absent.
     */
    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        if (!isUnfiltered(query)) {
            return null;
        }
        Optional<BoundingBox> bbox = primaryGeometryBbox();
        if (bbox.isEmpty()) {
            return null;
        }
        BoundingBox b = bbox.get();
        SimpleFeatureType ft = getSchema();
        return new ReferencedEnvelope(b.xmin(), b.xmax(), b.ymin(), b.ymax(), ft.getCoordinateReferenceSystem());
    }

    /**
     * Reads the primary geometry column's bounding box from GeoParquet {@code "geo"} key-value metadata. Returns empty
     * when the metadata is absent, unparseable, or the column has no bbox.
     */
    private Optional<BoundingBox> primaryGeometryBbox() {
        String geoJson = dataset().keyValueMetadata().get("geo");
        if (geoJson == null || geoJson.isBlank()) {
            return Optional.empty();
        }
        try {
            GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson);
            GeoColumn primary = geo.columns().get(geo.primaryColumn());
            return primary == null ? Optional.empty() : primary.bbox();
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /** Returns {@code true} when the query has no filter or an INCLUDE filter. */
    private static boolean isUnfiltered(Query query) {
        Filter filter = query.getFilter();
        return filter == null || Filter.INCLUDE.equals(filter);
    }

    @Override
    protected boolean canFilter(Query query) {
        return true;
    }

    @Override
    protected boolean canRetype(Query query) {
        return true;
    }

    @Override
    protected boolean canLimit(Query query) {
        // Claim native limit only when there is no offset; a paging query (startIndex + maxFeatures) is left entirely
        // to GeoTools, which applies offset before limit in the correct order.
        return !hasOffset(query);
    }

    private static boolean hasOffset(Query query) {
        Integer startIndex = query.getStartIndex();
        return startIndex != null && startIndex > 0;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        GeoParquetSchemaMapper.Mapping m = mapping();
        QueryTranslator.TranslatedQuery t = translate(query);

        SimpleFeatureType readType = readType(m, t);
        List<GeoParquetSchemaMapper.AttributeMapping> readAttributes = attributesFor(m, readType);

        FeatureReader<SimpleFeatureType, SimpleFeature> reader = new GeoParquetFeatureReader(
                readType,
                readAttributes,
                m.geometrySrids(),
                dataset(),
                t.predicate(),
                t.readProjection(),
                m.fidAttribute());

        if (t.postFilter() != Filter.INCLUDE) {
            reader = new FilteringFeatureReader<>(reader, t.postFilter());
        }

        SimpleFeatureType targetType = targetType(m, t);
        if (!readType.equals(targetType)) {
            reader = new ReTypeFeatureReader(reader, targetType);
        }

        return applyLimit(reader, query);
    }

    /**
     * Translates the query into a pushable predicate, residual filter, and projection. A spatial filter literal whose
     * CRS does not match the dataset native CRS is rejected here as an {@link IOException}.
     */
    private QueryTranslator.TranslatedQuery translate(Query query) throws IOException {
        try {
            return new QueryTranslator(mapping()).translate(query);
        } catch (IllegalArgumentException invalidFilter) {
            throw new IOException(invalidFilter.getMessage(), invalidFilter);
        }
    }

    /**
     * Caps the reader as the outermost layer when the query has no offset. {@link MaxFeatureReader} stops pulling once
     * the cap is reached; the pull-based parquetry stream underneath decodes no further row groups past the cap. The
     * cap applies to output features, never to raw candidates, and stays correct with or without a residual filter.
     *
     * <p>A paging query (an offset plus a limit) is left untouched here. {@link #canLimit(Query)} is false for it,
     * which lets GeoTools apply the offset and then the limit in the correct offset-then-limit order.
     */
    private static FeatureReader<SimpleFeatureType, SimpleFeature> applyLimit(
            FeatureReader<SimpleFeatureType, SimpleFeature> reader, Query query) {
        if (hasOffset(query)) {
            return reader;
        }
        if (query.isMaxFeaturesUnlimited()) {
            return reader;
        }
        return new MaxFeatureReader<>(reader, query.getMaxFeatures());
    }

    /** The feature type to decode: the requested output attributes plus any the residual filter needs. */
    private static SimpleFeatureType readType(GeoParquetSchemaMapper.Mapping m, QueryTranslator.TranslatedQuery t) {
        if (t.outputNames() == Query.ALL_NAMES) {
            return m.featureType();
        }
        Set<String> names = new LinkedHashSet<>();
        Collections.addAll(names, t.outputNames());
        names.addAll(QueryTranslator.attributeNames(t.postFilter()));
        return retype(m.featureType(), names);
    }

    /** The feature type the caller asked for: exactly the requested output attributes. */
    private static SimpleFeatureType targetType(GeoParquetSchemaMapper.Mapping m, QueryTranslator.TranslatedQuery t) {
        if (t.outputNames() == Query.ALL_NAMES) {
            return m.featureType();
        }
        Set<String> names = new LinkedHashSet<>();
        Collections.addAll(names, t.outputNames());
        return retype(m.featureType(), names);
    }

    private static SimpleFeatureType retype(SimpleFeatureType full, Set<String> attributeNames) {
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName(full.getName());
        b.setCRS(full.getCoordinateReferenceSystem());
        for (AttributeDescriptor d : full.getAttributeDescriptors()) {
            if (attributeNames.contains(d.getLocalName())) {
                b.add(d);
            }
        }
        return b.buildFeatureType();
    }

    private static List<GeoParquetSchemaMapper.AttributeMapping> attributesFor(
            GeoParquetSchemaMapper.Mapping m, SimpleFeatureType type) {
        List<GeoParquetSchemaMapper.AttributeMapping> result = new ArrayList<>();
        for (GeoParquetSchemaMapper.AttributeMapping attr : m.attributes()) {
            if (type.getDescriptor(attr.name()) != null) {
                result.add(attr);
            }
        }
        return result;
    }
}
