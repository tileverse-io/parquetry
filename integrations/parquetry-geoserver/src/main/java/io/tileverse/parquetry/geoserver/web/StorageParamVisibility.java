/*
 * Copyright (c) 2026 Multivers.io
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

import java.util.Set;

/**
 * Decides which connection-parameter field the store edit panel shows for a selected storage provider. Core fields are
 * always visible; a backend's parameters appear only when its provider is selected; the memory cache toggle appears for
 * cloud providers. The rule is pure to keep it testable without a running Wicket page.
 */
final class StorageParamVisibility {

    private static final Set<String> ALWAYS_VISIBLE = Set.of("filetype", "uri", "namespace", "fid", "storage.provider");
    private static final Set<String> CLOUD_PROVIDERS = Set.of("s3", "azure", "gcs", "http");
    private static final String CACHING_PREFIX = "storage.caching.";
    private static final String STORAGE_PREFIX = "storage.";

    private StorageParamVisibility() {}

    static boolean isVisible(String paramKey, String providerId) {
        if (ALWAYS_VISIBLE.contains(paramKey)) {
            return true;
        }
        String provider = providerId == null ? "" : providerId;
        if (paramKey.startsWith(CACHING_PREFIX)) {
            return CLOUD_PROVIDERS.contains(provider);
        }
        if (!paramKey.startsWith(STORAGE_PREFIX)) {
            return true;
        }
        if (CLOUD_PROVIDERS.contains(provider)) {
            return paramKey.startsWith(STORAGE_PREFIX + provider + ".");
        }
        return false;
    }
}
