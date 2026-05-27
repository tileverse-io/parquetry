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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.geotools.api.data.Query;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.Id;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.Capabilities;
import org.geotools.filter.visitor.CapabilitiesFilterSplitter;
import org.geotools.filter.visitor.DefaultFilterVisitor;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.geotools.GeoParquetSchemaMapper.AttributeMapping;
import io.tileverse.parquetry.geotools.GeoParquetSchemaMapper.Mapping;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Splits a GeoTools {@link Query} into the three pieces a parquetry read needs: a pushable {@link Predicate}, the
 * residual {@link Filter} GeoTools must still apply, and the {@link Projection} of columns to decode.
 *
 * <p>The split runs in two passes. {@link CapabilitiesFilterSplitter} first does a coarse pre/post separation against
 * {@link GeoParquetFilterCapabilities}; then {@link FilterToPredicate} translates the pre-part exactly, returning its
 * own residual for anything it cannot express. The two residuals combine into the post-filter.
 */
final class QueryTranslator {

    /**
     * The translated query parts. {@code predicate} pushes into parquetry; {@code postFilter} is what GeoTools must
     * re-apply (INCLUDE when nothing is left); {@code readProjection} is the column set to decode; {@code outputNames}
     * is the requested output attribute set ({@link Query#ALL_NAMES} for all).
     */
    record TranslatedQuery(Predicate predicate, Filter postFilter, Projection readProjection, String[] outputNames) {}

    private static final Capabilities CAPABILITIES = GeoParquetFilterCapabilities.create();
    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    private final Mapping mapping;
    private final FilterToPredicate translator;

    QueryTranslator(Mapping mapping) {
        this.mapping = mapping;
        this.translator = new FilterToPredicate(mapping, mapping.featureType().getCoordinateReferenceSystem());
    }

    TranslatedQuery translate(Query query) {
        Filter filter = query.getFilter() == null ? Filter.INCLUDE : query.getFilter();
        requireFeatureIdColumnForIdFilter(filter);
        Filter preCandidate = coarsePreFilter(filter);
        Filter coarsePost = coarsePostFilter(filter);
        FilterToPredicate.Result translated = translator.translate(preCandidate);
        Filter postFilter = and(coarsePost, translated.residual());
        Projection readProjection = readProjection(query, postFilter);
        return new TranslatedQuery(translated.predicate(), postFilter, readProjection, query.getPropertyNames());
    }

    private Filter coarsePreFilter(Filter filter) {
        return runSplitter(filter).getFilterPre();
    }

    private Filter coarsePostFilter(Filter filter) {
        return runSplitter(filter).getFilterPost();
    }

    private CapabilitiesFilterSplitter runSplitter(Filter filter) {
        CapabilitiesFilterSplitter splitter = new CapabilitiesFilterSplitter(CAPABILITIES, mapping.featureType(), null);
        filter.accept(splitter, null);
        return splitter;
    }

    /**
     * The columns to decode: the requested output columns plus the columns the post-filter still reads.
     * Pushed-predicate columns are intentionally excluded - parquetry decodes them itself while evaluating the
     * predicate, and they are not needed in the output rows.
     */
    private Projection readProjection(Query query, Filter postFilter) {
        if (query.getPropertyNames() == Query.ALL_NAMES) {
            return Projection.ALL;
        }
        Set<ColumnPath> kept = new LinkedHashSet<>();
        for (String outputName : query.getPropertyNames()) {
            addColumn(kept, outputName);
        }
        for (String postName : attributeNames(postFilter)) {
            addColumn(kept, postName);
        }
        // The reader needs the feature id column to assign feature ids; decode it even when the query omits it.
        mapping.fidAttribute().ifPresent(fid -> kept.add(fid.path()));
        return Projection.of(kept);
    }

    /**
     * Rejects an {@code Id} filter when no feature id column is resolved. Without a stable feature id, the synthetic
     * per-read ids an Id filter would match against are meaningless; rejecting the filter reports a programming error
     * rather than returning a silently empty result. When a feature id column is present, GeoTools applies the Id
     * filter on the materialized features: it is not in {@link GeoParquetFilterCapabilities} and stays in the
     * post-filter.
     */
    private void requireFeatureIdColumnForIdFilter(Filter filter) {
        if (mapping.fidAttribute().isPresent()) {
            return;
        }
        if (containsIdFilter(filter)) {
            throw new IllegalArgumentException(
                    "Id filters require a feature id column, but none is configured and no 'id' column is present; "
                            + "set the 'fid' connection parameter or add an 'id' column");
        }
    }

    private static boolean containsIdFilter(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE) {
            return false;
        }
        Set<Object> found = new LinkedHashSet<>();
        filter.accept(new IdFilterDetector(), found);
        return !found.isEmpty();
    }

    private void addColumn(Set<ColumnPath> kept, String attributeName) {
        for (AttributeMapping attribute : mapping.attributes()) {
            if (attribute.name().equals(attributeName)) {
                kept.add(attribute.path());
                return;
            }
        }
    }

    /**
     * The local property names referenced by a filter. INCLUDE and EXCLUDE reference none. Exposed for the feature
     * source, which needs the same attribute set when deciding what the post-filter requires.
     */
    static Set<String> attributeNames(Filter filter) {
        Set<String> names = new LinkedHashSet<>();
        if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE) {
            return names;
        }
        filter.accept(new PropertyNameCollector(), names);
        return names;
    }

    /** Folds INCLUDE away when conjoining: an INCLUDE operand adds nothing to an AND. */
    private static Filter and(Filter a, Filter b) {
        if (a == Filter.INCLUDE) {
            return b;
        }
        if (b == Filter.INCLUDE) {
            return a;
        }
        return FF.and(List.of(a, b));
    }

    /** Records a marker into the {@code Set} passed as visitor data when the filter contains an {@link Id} node. */
    private static final class IdFilterDetector extends DefaultFilterVisitor {
        @Override
        @SuppressWarnings("unchecked")
        public Object visit(Id filter, Object data) {
            ((Set<Object>) data).add(Boolean.TRUE);
            return data;
        }
    }

    /** Accumulates every referenced property's local name into the {@code Set<String>} passed as visitor data. */
    private static final class PropertyNameCollector extends DefaultFilterVisitor {
        @Override
        @SuppressWarnings("unchecked")
        public Object visit(PropertyName expression, Object data) {
            ((Set<String>) data).add(expression.getPropertyName());
            return data;
        }
    }
}
