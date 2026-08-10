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
package io.tileverse.parquetry.geotools.data;

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
import org.geotools.data.util.ScreenMap;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.factory.Hints;

import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.GeoParquetDataset;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Read-only feature source over a single dataset from a {@link DatasetCatalog}.
 *
 * <p>Query filtering is pushed down to the read path. The query splits into spatial and attribute filters that
 * translate into a parquetry predicate (with exact geometry tests where the relation supports them), a column
 * projection limited to the requested and residual-filter attributes, and any residual filter the predicate could not
 * express. Record reading ({@link #getReaderInternal(Query)}) streams the dataset through {@link DatasetFeatureReader}
 * and layers GeoTools decorators that apply the residual filter, retype to the requested attributes, and enforce the
 * feature cap.
 *
 * <p>Unfiltered and fully-pushable counts delegate to the dataset's count optimization, and the matching bounds to the
 * dataset's exact bounds; the unfiltered case reduces to the dataset's full aggregated extent.
 */
final class CatalogFeatureSource extends ContentFeatureSource {

    private final DatasetCatalog catalog;

    /**
     * Lazily built on first call; protected by the instance monitor so concurrent schema resolution does not duplicate
     * work.
     */
    private FeatureTypeMapper.Mapping mapping;

    CatalogFeatureSource(ContentEntry entry, DatasetCatalog catalog) {
        super(entry, Query.ALL);
        this.catalog = catalog;
    }

    private ParquetDataset dataset() {
        return catalog.dataset(getEntry().getTypeName());
    }

    /**
     * Returns the schema mapping, building it on the first call.
     *
     * <p>Reads GeoParquet metadata only when the dataset provides it; a plain dataset yields an attribute-only feature
     * type. Synchronized to prevent duplicate builds under concurrent access - the first caller wins and subsequent
     * callers reuse the result.
     */
    synchronized FeatureTypeMapper.Mapping mapping() throws IOException {
        if (mapping == null) {
            ParquetDataset ds = dataset();
            Optional<GeoParquetMetadata> geo =
                    ds instanceof GeoParquetDataset geoDataset ? geoDataset.geoMetadata() : Optional.empty();
            String fidColumn = ((CatalogDataStore) getDataStore()).fidColumn();
            try {
                mapping = FeatureTypeMapper.map(
                        getEntry().getTypeName(), getDataStore().getNamespaceURI(), ds.schema(), geo, fidColumn);
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
        long raw = dataset().count(t.predicate(), ReadOptions.DEFAULTS);
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
     * Returns the exact bounding box of the query's matching rows when the whole filter pushes down, or {@code null} to
     * signal that GeoTools should compute the bounds by iterating the filtered reader.
     *
     * <p>A residual post-filter means the pushed predicate over-approximates the query; an exact bounds cannot be
     * promised, and returning {@code null} is the documented answer that hands the computation back to the caller. An
     * empty result - no geometry column, or no matching rows - returns {@code null} for the same reason. The unfiltered
     * case translates to an always-true predicate and the dataset answers it from its aggregated spatial extent.
     */
    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        QueryTranslator.TranslatedQuery t = translate(query);
        if (t.postFilter() != Filter.INCLUDE) {
            return null;
        }
        // The GeoTools bounds contract tolerates a conservative answer (a WFS BoundedBy is explicitly approximate),
        // and callers use it per request: prefer the statistics-only estimate over an exact answer that may scan.
        Optional<BoundingBox> bbox = dataset().estimatedBounds(t.predicate());
        if (bbox.isEmpty()) {
            bbox = dataset().bounds(t.predicate(), ReadOptions.DEFAULTS);
        }
        if (bbox.isEmpty()) {
            return null;
        }
        BoundingBox b = bbox.get();
        SimpleFeatureType ft = getSchema();
        return new ReferencedEnvelope(b.xmin(), b.xmax(), b.ymin(), b.ymax(), ft.getCoordinateReferenceSystem());
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

    /**
     * Advertises support for the renderer's {@link Hints#SCREENMAP}. The renderer attaches its ScreenMap to the query
     * only when the feature source claims the hint; {@link #getReaderInternal(Query)} reads it and decimates the read.
     */
    @Override
    protected void addHints(Set<Hints.Key> hints) {
        hints.add(Hints.SCREENMAP);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        FeatureTypeMapper.Mapping m = mapping();
        QueryTranslator.TranslatedQuery t = translate(query);

        SimpleFeatureType readType = readType(m, t);
        List<FeatureTypeMapper.AttributeMapping> readAttributes = attributesFor(m, readType);

        FeatureReader<SimpleFeatureType, SimpleFeature> reader = new DatasetFeatureReader(
                readType,
                readAttributes,
                m.geometrySrids(),
                dataset(),
                t.predicate(),
                t.readProjection(),
                m.fidAttribute(),
                readOptionsFor(query, t));

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
     * The read options for this query. When the renderer attaches a {@link Hints#SCREENMAP} and the query pushes down
     * fully (no residual filter remains), the read runs through a {@link ScreenMapReadProbe} that decimates redundant
     * features per painted pixel inside the core read. Otherwise the read uses the default options.
     *
     * <p>The probe is withheld when a residual filter remains because the probe paints during the core read, before the
     * residual {@link FilteringFeatureReader} runs. A painted feature later rejected by the residual would leave its
     * pixel painted but empty and wrongly skip a valid later feature in that pixel. With no residual the renderer's
     * pushed query selects exactly the output features, which makes painting safe.
     */
    private static ReadOptions readOptionsFor(Query query, QueryTranslator.TranslatedQuery t) {
        Object hint = query.getHints().get(Hints.SCREENMAP);
        if (hint instanceof ScreenMap screenMap && t.postFilter() == Filter.INCLUDE) {
            return ReadOptions.builder()
                    .spatialReadProbe(new ScreenMapReadProbe(screenMap))
                    .build();
        }
        return ReadOptions.DEFAULTS;
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
    private static SimpleFeatureType readType(FeatureTypeMapper.Mapping m, QueryTranslator.TranslatedQuery t) {
        if (t.outputNames() == Query.ALL_NAMES) {
            return m.featureType();
        }
        Set<String> names = new LinkedHashSet<>();
        Collections.addAll(names, t.outputNames());
        names.addAll(QueryTranslator.attributeNames(t.postFilter()));
        return retype(m.featureType(), names);
    }

    /** The feature type the caller asked for: exactly the requested output attributes. */
    private static SimpleFeatureType targetType(FeatureTypeMapper.Mapping m, QueryTranslator.TranslatedQuery t) {
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

    private static List<FeatureTypeMapper.AttributeMapping> attributesFor(
            FeatureTypeMapper.Mapping m, SimpleFeatureType type) {
        List<FeatureTypeMapper.AttributeMapping> result = new ArrayList<>();
        for (FeatureTypeMapper.AttributeMapping attr : m.attributes()) {
            if (type.getDescriptor(attr.name()) != null) {
                result.add(attr);
            }
        }
        return result;
    }
}
