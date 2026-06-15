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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.geoserver.catalog.StoreInfo;
import org.geoserver.web.data.store.DefaultDataStoreEditPanel;
import org.geoserver.web.data.store.ParamInfo;
import org.geoserver.web.util.MapModel;

/**
 * A store edit panel for the GeoParquet DataStore that shows only the selected storage provider's parameters. The
 * provider is chosen with a segmented radio toggle; changing it broadcasts a {@link ProviderChanged} event that toggles
 * the visibility of every cached parameter panel through {@link StorageParamVisibility}.
 *
 * <p>Adapted from GeoServer's {@code PMTilesDataStoreEditPanel} (c) Open Source Geospatial Foundation, GPL-2.0.
 */
// S110: the GeoServer/Wicket panel hierarchy (DefaultDataStoreEditPanel) exceeds Sonar's parent-count limit.
@SuppressWarnings({"serial", "java:S110"})
public class GeoParquetDataStoreEditPanel extends DefaultDataStoreEditPanel {

    private static final String PROVIDER_KEY = "storage.provider";
    private static final String S3_REGION_KEY = "storage.s3.region";

    // keyed by param name; repopulated 1:1 with the parameters ListView, hence it neither grows nor leaks
    private final Map<String, Panel> panelsByKey = new HashMap<>();

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
    protected Panel getInputComponent(
            String componentId, IModel<Map<String, Serializable>> paramsModel, ParamInfo paramMetadata) {
        String paramName = paramMetadata.getName();
        Panel panel = inputPanel(componentId, paramsModel, paramMetadata, paramName);
        panel.setOutputMarkupId(true);
        // Only the provider-dependent parameters take part in the show/hide toggle. The always-visible core
        // parameters are left to the base panel and never cached or re-rendered here; in particular this keeps
        // the namespace field under GeoServer's own namespace-follows-workspace synchronization.
        if (!StorageParamVisibility.isAlwaysVisible(paramName)) {
            panel.setOutputMarkupPlaceholderTag(true);
            panelsByKey.put(paramName, panel);
        }
        return panel;
    }

    private Panel inputPanel(
            String componentId,
            IModel<Map<String, Serializable>> paramsModel,
            ParamInfo paramMetadata,
            String paramName) {
        if (PROVIDER_KEY.equals(paramName)) {
            return providerSelector(componentId, paramsModel, paramMetadata);
        }
        if (S3_REGION_KEY.equals(paramName)) {
            return s3Region(componentId, paramsModel, paramMetadata);
        }
        return super.getInputComponent(componentId, paramsModel, paramMetadata);
    }

    @Override
    protected void applyParamDefault(ParamInfo paramInfo, StoreInfo info) {
        super.applyParamDefault(paramInfo, info);
        List<Serializable> options = paramInfo.getOptions();
        if (options != null && !options.isEmpty()) {
            // An options-bearing parameter must not be pre-filled with its first option. Leave it empty until the
            // user picks one.
            info.getConnectionParameters().remove(paramInfo.getName());
        }
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

    private Select2ChoiceParamPanel<String> s3Region(
            String componentId, IModel<Map<String, Serializable>> paramsModel, ParamInfo paramInfo) {
        IModel<String> label = new ResourceModel(paramInfo.getName(), paramInfo.getName());
        IModel<String> model = new MapModel<>(paramsModel, paramInfo.getName());
        List<String> options =
                paramInfo.getOptions().stream().map(String::valueOf).sorted().toList();
        return Select2ChoiceParamPanel.ofStrings(componentId, label, model, options)
                .allowCustomValues(true)
                .setPlaceHolder("us-east-1");
    }

    private IModel<String> providerLabel(String providerId) {
        return new ResourceModel(PROVIDER_KEY + "." + providerId, providerId);
    }

    @Override
    public void onEvent(IEvent<?> event) {
        if (event.getPayload() instanceof ProviderChanged providerChanged) {
            applyVisibility(providerChanged);
        }
    }

    @Override
    protected void onBeforeRender() {
        super.onBeforeRender();
        DataStoreInfo storeInfo = (DataStoreInfo) storeEditForm.getModelObject();
        String providerId = (String) storeInfo.getConnectionParameters().get(PROVIDER_KEY);
        sendEvent(new ProviderChanged(providerId, null));
    }

    private void applyVisibility(ProviderChanged event) {
        panelsByKey.forEach((key, panel) -> {
            panel.setVisible(StorageParamVisibility.isVisible(key, event.providerId()));
            if (event.target() != null) {
                event.target().add(panel);
            }
        });
    }

    private <T> void sendEvent(T payload) {
        send(getPage(), Broadcast.BREADTH, payload);
    }

    record ProviderChanged(String providerId, AjaxRequestTarget target) {}
}
