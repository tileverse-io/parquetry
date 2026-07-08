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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.geotools.api.data.DataAccessFactory;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStoreFactory;

/**
 * Pins the fail-loud guard the ordered-key sourcing relies on. Sourcing the store's parameter order re-resolves the
 * store factory; an unresolvable factory must fail with a clear, store-named diagnostic rather than a
 * NullPointerException that would leave the store edit page with an opaque stack trace.
 */
class StorageAwareDataStoreEditPanelTest {

    @Test
    void requireResolvedFailsLoudNamingTheStoreWhenTheFactoryIsNull() {
        assertThatThrownBy(() -> StorageAwareDataStoreEditPanel.requireResolved(null, "acme:places"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("acme:places");
    }

    @Test
    void requireResolvedReturnsTheResolvedFactory() {
        DataAccessFactory factory = new GeoParquetDataStoreFactory();
        assertThat(StorageAwareDataStoreEditPanel.requireResolved(factory, "acme:places"))
                .isSameAs(factory);
    }
}
