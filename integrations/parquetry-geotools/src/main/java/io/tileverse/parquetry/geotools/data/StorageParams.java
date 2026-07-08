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
package io.tileverse.parquetry.geotools.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.Parameter;
import org.geotools.util.SimpleInternationalString;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.spi.StorageProvider;

/**
 * Bridges the tileverse storage configuration to GeoTools connection parameters. Every {@code storage.*} key a provider
 * understands becomes an advanced, optional {@link Param}; secrets are marked as password fields and sample values
 * become selectable options. The set is discovered from the {@link StorageProvider} service registry; a new provider on
 * the classpath contributes its parameters with no change here.
 */
public final class StorageParams {

    private static final String STORAGE_PREFIX = "storage.";

    /** Block alignment and block size are not exposed: parquet reads are byte-range native and need neither. */
    private static final Set<String> EXCLUDED_KEYS =
            Set.of("storage.caching.blockaligned", "storage.caching.blocksize");

    /** The storage connection parameters, the provider selector first, then the backend parameters by group. */
    static final List<Param> PROVIDER_PARAMS = buildProviderParams();

    private StorageParams() {}

    /** Returns the given core parameters followed by all storage parameters. */
    public static Param[] withStorageParams(Param... core) {
        List<Param> all = new ArrayList<>(Arrays.asList(core));
        all.addAll(PROVIDER_PARAMS);
        return all.toArray(Param[]::new);
    }

    /**
     * Returns the given core parameters followed by the storage parameters, minus the provider selector. A STAC store
     * auto-detects each container's backend from its own URI - a public HTTP catalog and its private object-store
     * assets open side by side - and a single forced provider would misroute one of them. The provider selector is
     * therefore omitted from the STAC store's form; the backend connection parameters are still offered.
     */
    public static Param[] withStorageParamsNoProvider(Param... core) {
        List<Param> all = new ArrayList<>(Arrays.asList(core));
        for (Param param : PROVIDER_PARAMS) {
            if (!param.key.equals(StorageConfig.PROVIDER_ID_KEY)) {
                all.add(param);
            }
        }
        return all.toArray(Param[]::new);
    }

    /** Copies every set {@code storage.*} connection parameter into a Properties for the storage backend. */
    public static Properties toProperties(Map<String, ?> params) {
        Properties props = new Properties();
        for (Map.Entry<String, ?> entry : params.entrySet()) {
            if (entry.getKey().startsWith(STORAGE_PREFIX) && entry.getValue() != null) {
                props.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return props;
    }

    private static List<Param> buildProviderParams() {
        List<Param> params = new ArrayList<>();
        params.add(providerSelectorParam());
        Set<String> seen = new LinkedHashSet<>();
        seen.add(StorageConfig.PROVIDER_ID_KEY);
        StorageProvider.getProviders().stream()
                .flatMap(provider -> provider.getParameters().stream())
                .filter(parameter -> !EXCLUDED_KEYS.contains(parameter.key()))
                .sorted(byGroupThenKey())
                .forEach(parameter -> {
                    if (seen.add(parameter.key())) {
                        params.add(toParam(parameter));
                    }
                });
        return List.copyOf(params);
    }

    private static Comparator<StorageParameter<?>> byGroupThenKey() {
        Comparator<StorageParameter<?>> byGroup = Comparator.comparing(StorageParameter::group);
        return byGroup.thenComparing(StorageParameter::key);
    }

    private static Param providerSelectorParam() {
        // A mutable list: GeoServer's ParamInfo sorts the OPTIONS metadata in place.
        List<String> providerIds = new ArrayList<>(StorageProvider.getProviders().stream()
                .map(StorageProvider::getId)
                .sorted()
                .toList());
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(Parameter.LEVEL, "advanced");
        if (!providerIds.isEmpty()) {
            metadata.put(Parameter.OPTIONS, providerIds);
        }
        return param(
                StorageConfig.PROVIDER_ID_KEY,
                String.class,
                "Storage provider",
                "Storage provider; auto-detected from the URI when unset",
                null,
                metadata);
    }

    private static Param toParam(StorageParameter<?> parameter) {
        Class<?> type = paramType(parameter.type());
        // A non-scalar type (e.g. Duration) has no GeoTools scalar binding; render and round-trip it as text.
        boolean coerceToString = type == String.class && parameter.type() != String.class;
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(Parameter.LEVEL, "advanced");
        if (parameter.password()) {
            metadata.put(Parameter.IS_PASSWORD, Boolean.TRUE);
        }
        if (!parameter.sampleValues().isEmpty()) {
            // A mutable list: GeoServer's ParamInfo sorts the OPTIONS metadata in place.
            List<Object> options = new ArrayList<>(parameter.sampleValues().stream()
                    .map(value -> coerceToString ? (Object) String.valueOf(value) : value)
                    .toList());
            metadata.put(Parameter.OPTIONS, options);
        }
        Object sample = parameter
                .defaultValue()
                .map(value -> coerceToString ? (Object) String.valueOf(value) : value)
                .orElse(null);
        // GeoServer's store edit page uses the Param title as the field tooltip and resolves the field
        // label separately from GeoServerApplication.properties. Pass the full description as the title
        // so the tooltip is informative; the short label comes from the resource bundle.
        return param(parameter.key(), type, parameter.description(), parameter.description(), sample, metadata);
    }

    private static Param param(
            String key, Class<?> type, String title, String description, Object sample, Map<String, Object> metadata) {
        return new Param(
                key,
                type,
                new SimpleInternationalString(title),
                new SimpleInternationalString(description),
                false,
                0,
                1,
                sample,
                metadata);
    }

    /** Scalar types pass through; anything else (e.g. Duration) renders and round-trips as text. */
    private static Class<?> paramType(Class<?> type) {
        if (type == String.class
                || type == Boolean.class
                || type == Integer.class
                || type == Long.class
                || type == Double.class) {
            return type;
        }
        return String.class;
    }
}
