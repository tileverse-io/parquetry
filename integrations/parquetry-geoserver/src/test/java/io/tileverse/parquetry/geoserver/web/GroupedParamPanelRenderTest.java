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

import org.apache.wicket.markup.html.panel.EmptyPanel;
import org.apache.wicket.model.Model;
import org.apache.wicket.util.tester.WicketTester;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Renders a {@link GroupedParamPanel} in isolation to pin its markup and the header show/hide contract: the wrapped
 * field nests under the wrapper's own child slot, the section header shows above it only when a header model is given,
 * and no header markup is emitted otherwise.
 */
class GroupedParamPanelRenderTest {

    private static final String HEADER_ID = "header";

    private WicketTester tester;

    @BeforeEach
    void startTester() {
        tester = new WicketTester();
    }

    @AfterEach
    void stopTester() {
        tester.destroy();
    }

    @Test
    void rendersTheHeaderAboveTheFieldWhenAHeaderModelIsGiven() {
        GroupedParamPanel panel = new GroupedParamPanel(
                "panel", new EmptyPanel(GroupedParamPanel.FIELD_ID), Model.of("AWS S3 parameters"));

        tester.startComponentInPage(panel);

        assertThat(panel.get(HEADER_ID).isVisibleInHierarchy()).isTrue();
        assertThat(panel.get(GroupedParamPanel.FIELD_ID)).isNotNull();
        assertThat(tester.getLastResponseAsString()).contains("AWS S3 parameters");
    }

    @Test
    void hidesTheHeaderWhenNoHeaderModelIsGiven() {
        GroupedParamPanel panel = new GroupedParamPanel("panel", new EmptyPanel(GroupedParamPanel.FIELD_ID), null);

        tester.startComponentInPage(panel);

        assertThat(panel.get(HEADER_ID).isVisibleInHierarchy()).isFalse();
        assertThat(tester.getLastResponseAsString()).doesNotContain("<h4");
    }
}
