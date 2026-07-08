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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.form.AjaxFormChoiceComponentUpdatingBehavior;
import org.apache.wicket.event.Broadcast;
import org.apache.wicket.event.IEvent;
import org.apache.wicket.markup.head.CssHeaderItem;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.RadioGroup;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.ResourceModel;
import org.apache.wicket.request.resource.PackageResourceReference;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.web.data.store.ParamInfo;
import org.geoserver.web.util.MapModel;

/**
 * A store edit panel for the GeoParquet DataStore that shows only the selected storage provider's parameters. The
 * single backend is chosen with a segmented radio toggle; changing it broadcasts a {@link ProviderChanged} event that
 * maps the new provider to its backend groups and re-applies field visibility through the base panel.
 *
 * <p>Adapted from GeoServer's {@code PMTilesDataStoreEditPanel} (c) Open Source Geospatial Foundation, GPL-2.0.
 */
// S110: the GeoServer/Wicket panel hierarchy (DefaultDataStoreEditPanel) exceeds Sonar's parent-count limit.
@SuppressWarnings({"serial", "java:S110"})
public class GeoParquetDataStoreEditPanel extends StorageAwareDataStoreEditPanel {

    private static final String PROVIDER_KEY = "storage.provider";

    public GeoParquetDataStoreEditPanel(String componentId, Form<DataStoreInfo> storeEditForm) {
        super(componentId, storeEditForm);
    }

    @Override
    public void renderHead(IHeaderResponse response) {
        super.renderHead(response);
        response.render(CssHeaderItem.forReference(
                new PackageResourceReference(RadioGroupParamPanel.class, "RadioGroupParamPanel.css")));
    }

    @Override
    protected Set<String> selectedGroups() {
        DataStoreInfo storeInfo = (DataStoreInfo) storeEditForm.getModelObject();
        String providerId = (String) storeInfo.getConnectionParameters().get(PROVIDER_KEY);
        return StorageParamVisibility.groupsForProvider(providerId);
    }

    @Override
    protected Panel buildInputPanel(
            String componentId, IModel<Map<String, Serializable>> paramsModel, ParamInfo paramMetadata) {
        if (PROVIDER_KEY.equals(paramMetadata.getName())) {
            return providerSelector(componentId, paramsModel, paramMetadata);
        }
        return super.buildInputPanel(componentId, paramsModel, paramMetadata);
    }

    private RadioGroupParamPanel<String> providerSelector(
            String componentId, IModel<Map<String, Serializable>> paramsModel, ParamInfo paramInfo) {
        IModel<String> label = new ResourceModel(paramInfo.getName(), paramInfo.getName());
        IModel<String> model = new MapModel<>(paramsModel, PROVIDER_KEY);
        List<String> options =
                paramInfo.getOptions().stream().map(String::valueOf).toList();
        RadioGroupParamPanel<String> paramPanel =
                new RadioGroupParamPanel<>(componentId, label, model, options, this::providerLabel);
        RadioGroup<String> radioGroup = paramPanel.getFormComponent();
        radioGroup.add(new AjaxFormChoiceComponentUpdatingBehavior() {
            @Override
            protected void onUpdate(AjaxRequestTarget target) {
                sendEvent(new ProviderChanged(radioGroup.getModel().getObject(), target));
            }
        });
        return paramPanel;
    }

    private IModel<String> providerLabel(String providerId) {
        return new ResourceModel(PROVIDER_KEY + "." + providerId, providerId);
    }

    @Override
    public void onEvent(IEvent<?> event) {
        if (event.getPayload() instanceof ProviderChanged providerChanged) {
            Set<String> groups = StorageParamVisibility.groupsForProvider(providerChanged.providerId());
            applyVisibility(groups, providerChanged.target());
        }
    }

    private <T> void sendEvent(T payload) {
        send(getPage(), Broadcast.BREADTH, payload);
    }

    record ProviderChanged(String providerId, AjaxRequestTarget target) {}
}
