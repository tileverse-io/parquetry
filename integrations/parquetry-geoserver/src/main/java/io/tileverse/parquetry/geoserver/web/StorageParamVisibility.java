/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 */
package io.tileverse.parquetry.geoserver.web;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.tileverse.storage.spi.StorageProvider;

/**
 * Decides which connection-parameter field a store edit panel shows for the selected storage backend(s). The rule is
 * derived from the key shape {@code storage.<group>[.*]} and stays pure to keep it testable without a running Wicket
 * page.
 *
 * <p>Core fields - every non-{@code storage.*} key and the {@code storage.provider} selector itself - are always
 * visible. A backend group's fields appear only when that group is selected. The memory cache toggle appears when at
 * least one remote cloud backend is selected.
 *
 * <p>The four remote cloud backends - {@code s3}, {@code azure}, {@code gcs}, {@code http} - are the only configurable
 * groups; {@link #CLOUD_GROUPS} is the single source of truth for that set. The local {@code file} backend is the
 * baseline: it exposes no remote connection parameters to configure, and its lone idle-timeout knob takes the
 * tileverse-storage default, hence {@code file} is never a selectable group.
 */
final class StorageParamVisibility {

    private static final Set<String> CLOUD_GROUPS = Set.of("s3", "azure", "gcs", "http");
    private static final String STORAGE_PREFIX = "storage.";
    private static final String CACHING_GROUP = "caching";
    private static final String PROVIDER_GROUP = "provider";

    private StorageParamVisibility() {}

    /**
     * True for the fields shown regardless of the selected backend(s): every non-{@code storage.*} key (namespace,
     * feature id column, ...) and the {@code storage.provider} selector itself. These stay out of the group-driven
     * show/hide toggle and keep GeoServer's stock handling - in particular the namespace field keeps following the
     * workspace as GeoServer expects.
     */
    static boolean isAlwaysVisible(String paramKey) {
        String group = groupOf(paramKey);
        return group.isEmpty() || PROVIDER_GROUP.equals(group);
    }

    /**
     * Whether the field for {@code paramKey} shows given the {@code selectedGroups}. Always-visible core fields show
     * unconditionally; the caching toggle shows when at least one remote cloud backend is selected; a backend group's
     * fields show only when that group is selected.
     */
    static boolean isVisible(String paramKey, Set<String> selectedGroups) {
        String group = groupOf(paramKey);
        if (group.isEmpty() || PROVIDER_GROUP.equals(group)) {
            return true;
        }
        if (CACHING_GROUP.equals(group)) {
            return selectedGroups.stream().anyMatch(CLOUD_GROUPS::contains);
        }
        return selectedGroups.contains(group);
    }

    /**
     * The subset of {@code orderedKeys} that begin a titled backend group, in encounter order. A key qualifies when its
     * group renders a header and differs from the previous titled group's; keys of untitled groups - the core fields,
     * the provider selector, the local {@code file} baseline - never qualify and never interrupt a titled group's run.
     * The store edit panel renders that group's header immediately before each returned key's field. The parameters
     * arrive contiguous per group, hence one key per group qualifies.
     */
    static Set<String> firstParamKeysPerGroup(List<String> orderedKeys) {
        Set<String> firstKeys = new LinkedHashSet<>();
        String previousTitledGroup = null;
        for (String key : orderedKeys) {
            String group = groupOf(key);
            if (!hasGroupHeader(group)) {
                continue;
            }
            if (!group.equals(previousTitledGroup)) {
                firstKeys.add(key);
            }
            previousTitledGroup = group;
        }
        return firstKeys;
    }

    /**
     * Whether a backend group renders a titled section header above its first field. The four remote cloud backends and
     * the memory cache toggle do; the provider selector, the core fields, and the local {@code file} baseline do not.
     */
    static boolean hasGroupHeader(String group) {
        return CLOUD_GROUPS.contains(group) || CACHING_GROUP.equals(group);
    }

    /**
     * The remote cloud backend groups a user can select between, sorted. Derived from the registered providers'
     * parameters and restricted to {@link #CLOUD_GROUPS}, hence {@code provider}, {@code caching}, and the local
     * {@code file} baseline are excluded. Expected to be {@code [azure, gcs, http, s3]} against the standard provider
     * registry.
     */
    static List<String> selectableGroups() {
        return StorageProvider.getProviders().stream()
                .flatMap(provider -> provider.getParameters().stream())
                .map(parameter -> groupOf(parameter.key()))
                .filter(CLOUD_GROUPS::contains)
                .distinct()
                .sorted()
                .toList();
    }

    /**
     * The remote cloud backend groups a provider's {@link StorageProvider#getParameters() parameters} report. A
     * provider whose parameters live under a shared group reports that group: {@code azure-datalake} reuses Azure's
     * {@code storage.azure.*} parameters and reports {@code {azure}}. The local {@code file} baseline reports an empty
     * set because its idle-timeout parameter is not a configurable remote backend group.
     */
    static Set<String> groupsForProvider(String providerId) {
        return StorageProvider.getProviders().stream()
                .filter(provider -> provider.getId().equals(providerId))
                .flatMap(provider -> provider.getParameters().stream())
                .map(parameter -> groupOf(parameter.key()))
                .filter(CLOUD_GROUPS::contains)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * The remote cloud backend groups already present in a store's {@code connectionParameters}, derived from any
     * {@code storage.<group>.*} keys and restricted to {@link #CLOUD_GROUPS}. Used to pre-select the backends when
     * editing an existing store.
     */
    static Set<String> selectedGroupsFromParameters(Map<String, ?> connectionParameters) {
        return connectionParameters.keySet().stream()
                .map(StorageParamVisibility::groupOf)
                .filter(CLOUD_GROUPS::contains)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * The {@code <group>} token in a {@code storage.<group>[.*]} key, or the empty string for a non-{@code storage.*}
     * key. For example {@code storage.s3.region} yields {@code s3}, {@code storage.provider} yields {@code provider},
     * {@code storage.caching.enabled} yields {@code caching}, and {@code namespace} yields {@code ""}.
     */
    static String groupOf(String paramKey) {
        if (!paramKey.startsWith(STORAGE_PREFIX)) {
            return "";
        }
        String rest = paramKey.substring(STORAGE_PREFIX.length());
        int dot = rest.indexOf('.');
        return dot < 0 ? rest : rest.substring(0, dot);
    }
}
