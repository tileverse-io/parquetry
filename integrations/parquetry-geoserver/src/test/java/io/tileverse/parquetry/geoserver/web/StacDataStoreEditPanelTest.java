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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.wicket.markup.html.form.CheckGroup;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.util.tester.WicketTester;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the STAC store panel's storage-backend multi-select wiring without a running GeoServer: the backends the panel
 * pre-checks are derived from the store's connection-parameter keys, the checkbox group it builds renders one box per
 * selectable backend and pre-checks the derived ones, and the group-based visibility rule shows exactly the checked
 * backends' parameters. The full store edit page - a checkbox toggle re-rendering live GeoServer fields - is exercised
 * by hand through the {@code StartGeoServer} launcher.
 */
class StacDataStoreEditPanelTest {

    private static final List<String> SELECTABLE_BACKENDS = List.of("azure", "gcs", "http", "s3");

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
    void derivesTheCheckedBackendsFromTheStoredParameterKeys() {
        Map<String, String> connectionParameters = Map.of(
                "geoparquet-stac", "s3://bucket/catalog.json",
                "storage.s3.region", "us-east-1",
                "storage.http.bearer-token", "secret");

        Set<String> checked = StorageParamVisibility.selectedGroupsFromParameters(connectionParameters);

        assertThat(checked).containsExactlyInAnyOrder("s3", "http");
    }

    @Test
    void rendersOneCheckboxForEverySelectableBackend() {
        CheckGroupParamPanel panel = backendSelector(Set.of());

        tester.startComponentInPage(panel);

        assertThat(checkboxCount(tester.getLastResponseAsString())).isEqualTo(SELECTABLE_BACKENDS.size());
    }

    @Test
    void preChecksTheSeededBackendsAndLeavesTheRestUnchecked() {
        CheckGroupParamPanel panel = backendSelector(Set.of("s3", "http"));

        tester.startComponentInPage(panel);
        CheckGroup<String> group = panel.getFormComponent();

        assertThat(group.getModelObject()).containsExactlyInAnyOrder("s3", "http");
        assertThat(checkedCount(tester.getLastResponseAsString())).isEqualTo(2);
    }

    @Test
    void showsOnlyTheCheckedBackendsParameters() {
        Set<String> checked = Set.of("s3", "http");

        assertThat(StorageParamVisibility.isVisible("storage.s3.region", checked))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.http.bearer-token", checked))
                .isTrue();
        assertThat(StorageParamVisibility.isVisible("storage.azure.account-key", checked))
                .isFalse();
        assertThat(StorageParamVisibility.isVisible("storage.gcs.project-id", checked))
                .isFalse();
    }

    private static CheckGroupParamPanel backendSelector(Set<String> initiallyChecked) {
        IModel<String> label = Model.of("Storage backends");
        return new CheckGroupParamPanel("panel", label, initiallyChecked, SELECTABLE_BACKENDS, Model::of);
    }

    private static int checkboxCount(String html) {
        return countOccurrences(html, "type=\"checkbox\"");
    }

    private static int checkedCount(String html) {
        return countOccurrences(html, "checked=\"checked\"");
    }

    private static int countOccurrences(String text, String token) {
        int count = 0;
        int from = text.indexOf(token);
        while (from >= 0) {
            count++;
            from = text.indexOf(token, from + token.length());
        }
        return count;
    }
}
