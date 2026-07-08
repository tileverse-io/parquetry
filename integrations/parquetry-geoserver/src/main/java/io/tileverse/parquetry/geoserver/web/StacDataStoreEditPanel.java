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
import java.util.Set;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.form.AjaxFormChoiceComponentUpdatingBehavior;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.ResourceModel;
import org.geoserver.catalog.DataStoreInfo;

/**
 * A store edit panel for the STAC GeoParquet DataStore, which reads from more than one storage backend at once: its
 * catalog document and its data assets may live on different backends. The store therefore has no single
 * {@code storage.provider} parameter; the user ticks the backends in play instead, and the panel shows each ticked
 * backend's connection parameters through the base panel's group-based visibility.
 *
 * <p>The ticked set is seeded from the backends already present in the store's parameters, which opens an existing
 * store with the right backends checked. It is transient view state, never written back as a connection parameter - see
 * {@link CheckGroupParamPanel}.
 *
 * <p>Adapted from GeoServer's {@code PMTilesDataStoreEditPanel} (c) Open Source Geospatial Foundation, GPL-2.0.
 */
// S110: the GeoServer/Wicket panel hierarchy (DefaultDataStoreEditPanel) exceeds Sonar's parent-count limit.
@SuppressWarnings({"serial", "java:S110"})
public class StacDataStoreEditPanel extends StorageAwareDataStoreEditPanel {

    private final CheckGroupParamPanel backendSelector;

    public StacDataStoreEditPanel(String componentId, Form<DataStoreInfo> storeEditForm) {
        super(componentId, storeEditForm);
        backendSelector = buildBackendSelector();
        add(backendSelector);
    }

    @Override
    protected Set<String> selectedGroups() {
        return new LinkedHashSet<>(backendSelector.getFormComponent().getModelObject());
    }

    private CheckGroupParamPanel buildBackendSelector() {
        List<String> backends = StorageParamVisibility.selectableGroups();
        Set<String> initiallyChecked = selectedBackendsFromStore();
        IModel<String> label = new ResourceModel("storage.backends", "Storage backends");
        CheckGroupParamPanel panel =
                new CheckGroupParamPanel("backends", label, initiallyChecked, backends, this::backendLabel);
        panel.getFormComponent().add(reapplyVisibilityOnToggle());
        return panel;
    }

    /** The backends already present in the store's saved connection parameters, which pre-check the boxes on open. */
    private Set<String> selectedBackendsFromStore() {
        DataStoreInfo storeInfo = (DataStoreInfo) storeEditForm.getModelObject();
        return StorageParamVisibility.selectedGroupsFromParameters(storeInfo.getConnectionParameters());
    }

    private IModel<String> backendLabel(String backend) {
        return new ResourceModel("storage.provider." + backend, backend);
    }

    private AjaxFormChoiceComponentUpdatingBehavior reapplyVisibilityOnToggle() {
        return new AjaxFormChoiceComponentUpdatingBehavior() {
            @Override
            protected void onUpdate(AjaxRequestTarget target) {
                applyVisibility(selectedGroups(), target);
            }
        };
    }
}
