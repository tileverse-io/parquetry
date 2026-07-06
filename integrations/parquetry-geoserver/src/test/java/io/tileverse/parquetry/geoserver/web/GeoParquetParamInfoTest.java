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

import static org.assertj.core.api.Assertions.assertThatCode;

import org.geoserver.web.data.store.ParamInfo;
import org.geotools.api.data.DataAccessFactory.Param;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStoreFactory;

/**
 * GeoServer's store pages wrap every connection parameter in a {@link ParamInfo}, whose constructor sorts an options
 * list in place. A parameter declared with an immutable options list breaks the store edit page with an
 * {@code UnsupportedOperationException} before it renders. Constructing a {@link ParamInfo} for every factory parameter
 * pins that each one survives what the page actually does to it.
 */
class GeoParquetParamInfoTest {

    @Test
    void everyFactoryParamSurvivesParamInfoConstruction() {
        for (Param param : new GeoParquetDataStoreFactory().getParametersInfo()) {
            assertThatCode(() -> new ParamInfo(param)).as(param.key).doesNotThrowAnyException();
        }
    }
}
