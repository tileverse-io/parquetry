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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.ResourceModel;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.NamespaceInfo;
import org.geoserver.catalog.StoreInfo;
import org.geoserver.catalog.WorkspaceInfo;
import org.geoserver.web.GeoServerApplication;
import org.geoserver.web.data.store.DefaultDataStoreEditPanel;
import org.geoserver.web.data.store.ParamInfo;
import org.geoserver.web.util.MapModel;
import org.geotools.api.data.DataAccessFactory;
import org.geotools.api.data.DataAccessFactory.Param;

/**
 * Base store edit panel for the parquetry DataStores that shows only the connection parameters belonging to the
 * selected storage backend(s). It caches every backend-dependent parameter panel and toggles each panel's visibility
 * through {@link StorageParamVisibility}, given the set of backend groups the subclass reports as selected.
 *
 * <p>A concrete panel supplies the current selection by implementing {@link #selectedGroups()} - a single-backend store
 * derives it from a provider radio, a multi-backend store from a set of checkboxes - and drives updates on user input
 * by calling {@link #applyVisibility(Set, AjaxRequestTarget)} with the recomputed selection. The always-visible core
 * fields (every non-{@code storage.*} key and the {@code storage.provider} selector) stay out of the cache and keep
 * GeoServer's stock handling, which is what keeps the namespace field following the workspace.
 *
 * <p>Adapted from GeoServer's {@code PMTilesDataStoreEditPanel} (c) Open Source Geospatial Foundation, GPL-2.0.
 */
// S110: the GeoServer/Wicket panel hierarchy (DefaultDataStoreEditPanel) exceeds Sonar's parent-count limit.
@SuppressWarnings({"serial", "java:S110"})
abstract class StorageAwareDataStoreEditPanel extends DefaultDataStoreEditPanel {

    private static final String S3_REGION_KEY = "storage.s3.region";
    private static final String NAMESPACE_KEY = "namespace";

    // keyed by param name; repopulated 1:1 with the parameters ListView, hence it neither grows nor leaks
    private final Map<String, Panel> panelsByKey = new HashMap<>();

    // the storage keys that begin a titled backend group; derived once from the store's ordered parameters
    private Set<String> firstGroupParamKeys;

    protected StorageAwareDataStoreEditPanel(String componentId, Form<DataStoreInfo> storeEditForm) {
        super(componentId, storeEditForm);
    }

    /**
     * The storage backend groups currently selected in this panel, as understood by {@link StorageParamVisibility}. The
     * base panel asks for this on every render to seed the initial field visibility.
     */
    protected abstract Set<String> selectedGroups();

    @Override
    protected void onBeforeRender() {
        super.onBeforeRender();
        DataStoreInfo storeInfo = (DataStoreInfo) storeEditForm.getModelObject();
        alignNamespaceWithWorkspace(storeInfo);
        applyVisibility(selectedGroups(), null);
    }

    /**
     * Forces the namespace connection parameter to the store's workspace namespace. GeoServer keeps the namespace in
     * step with the workspace only when the workspace dropdown is changed; on the workspace-scoped "Add new store" flow
     * the workspace is pre-selected and never fires that change, leaving the namespace seeded from the default
     * workspace. A store saved that way reads over WMS but fails WFS GetFeature, whose catalog lookup is keyed by the
     * namespace. Re-deriving the namespace from the workspace on every render keeps a parquetry store correct
     * regardless, and matches GeoServer's own intent that the namespace follows the workspace.
     */
    private void alignNamespaceWithWorkspace(DataStoreInfo storeInfo) {
        WorkspaceInfo workspace = storeInfo.getWorkspace();
        if (workspace == null) {
            return;
        }
        NamespaceInfo namespace = GeoServerApplication.get().getCatalog().getNamespaceByPrefix(workspace.getName());
        if (namespace == null) {
            return;
        }
        storeInfo.getConnectionParameters().put(NAMESPACE_KEY, namespace.getURI());
    }

    /**
     * Toggles every cached backend-dependent panel to match {@code selectedGroups}. When {@code targetOrNull} is
     * non-null the panel is added to the Ajax response for the browser to re-render it; a null target is the initial,
     * non-Ajax render. The group set is computed once by the caller and applied to every key, never recomputed per
     * panel.
     */
    protected void applyVisibility(Set<String> selectedGroups, AjaxRequestTarget targetOrNull) {
        panelsByKey.forEach((key, panel) -> {
            panel.setVisible(StorageParamVisibility.isVisible(key, selectedGroups));
            if (targetOrNull != null) {
                targetOrNull.add(panel);
            }
        });
    }

    @Override
    protected Panel getInputComponent(
            String componentId, IModel<Map<String, Serializable>> paramsModel, ParamInfo paramMetadata) {
        String paramName = paramMetadata.getName();
        // The always-visible core parameters (and the provider selector) are left to the superclass and never cached
        // or re-rendered here; in particular this keeps the namespace field under GeoServer's own
        // namespace-follows-workspace synchronization.
        if (StorageParamVisibility.isAlwaysVisible(paramName)) {
            Panel corePanel = buildInputPanel(componentId, paramsModel, paramMetadata);
            corePanel.setOutputMarkupId(true);
            return corePanel;
        }
        return groupedField(componentId, paramsModel, paramMetadata, paramName);
    }

    /**
     * Wraps one backend-dependent field, rendering its backend group's header above it when the field is the first of
     * that group. The wrapper, not the inner field, is cached and toggled: hiding a deselected group hides its header
     * together with its fields, leaving no orphan header behind.
     */
    private Panel groupedField(
            String componentId,
            IModel<Map<String, Serializable>> paramsModel,
            ParamInfo paramMetadata,
            String paramName) {
        Panel field = buildInputPanel(GroupedParamPanel.FIELD_ID, paramsModel, paramMetadata);
        field.setOutputMarkupId(true);
        GroupedParamPanel wrapper = new GroupedParamPanel(componentId, field, groupHeaderModel(paramName));
        wrapper.setOutputMarkupId(true);
        wrapper.setOutputMarkupPlaceholderTag(true);
        panelsByKey.put(paramName, wrapper);
        return wrapper;
    }

    /**
     * The section-header model for {@code paramName}, or {@code null} when the field is not the first of its titled
     * group. The header text is resolved from {@code storage.group.<group>} in the resource bundle.
     */
    private IModel<String> groupHeaderModel(String paramName) {
        if (!firstGroupParamKeys().contains(paramName)) {
            return null;
        }
        String group = StorageParamVisibility.groupOf(paramName);
        return new ResourceModel("storage.group." + group);
    }

    /** The storage keys that begin a titled backend group, derived once from the store's ordered parameter keys. */
    private Set<String> firstGroupParamKeys() {
        if (firstGroupParamKeys == null) {
            firstGroupParamKeys = StorageParamVisibility.firstParamKeysPerGroup(orderedParameterKeys());
        }
        return firstGroupParamKeys;
    }

    /**
     * The store's connection-parameter keys in the order GeoServer lays them out, which is the order the store factory
     * reports them. The parameters ListView is built from this same order, hence a group's first key here is its first
     * rendered field.
     */
    private List<String> orderedParameterKeys() {
        DataStoreInfo storeInfo = (DataStoreInfo) storeEditForm.getModelObject();
        Param[] parameters = dataStoreFactory(storeInfo).getParametersInfo();
        return Arrays.stream(parameters).map(parameter -> parameter.key).toList();
    }

    private DataAccessFactory dataStoreFactory(DataStoreInfo storeInfo) {
        String store = describe(storeInfo);
        try {
            return requireResolved(getCatalog().getResourcePool().getDataStoreFactory(storeInfo), store);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to resolve the data store factory for " + store, e);
        }
    }

    /**
     * Returns {@code factory}, or fails loud with the store named when it is {@code null}. The resource pool returns
     * {@code null} for a store whose factory is not on the classpath; without this guard the caller would dereference
     * {@code null} and the store edit page would fail with an opaque NullPointerException.
     */
    static DataAccessFactory requireResolved(DataAccessFactory factory, String store) {
        if (factory == null) {
            throw new IllegalStateException("No data store factory resolved for " + store);
        }
        return factory;
    }

    private static String describe(DataStoreInfo storeInfo) {
        String name = storeInfo.getName();
        String storeName = (name == null || name.isBlank()) ? "(unnamed store)" : name;
        WorkspaceInfo workspace = storeInfo.getWorkspace();
        if (workspace == null) {
            return storeName;
        }
        return workspace.getName() + ":" + storeName;
    }

    /**
     * Builds the input panel for one connection parameter. Subclasses override to contribute store-specific widgets
     * (for example a provider selector) and defer to {@code super} for the shared cases: the s3-region autocomplete and
     * GeoServer's stock input for everything else.
     */
    protected Panel buildInputPanel(
            String componentId, IModel<Map<String, Serializable>> paramsModel, ParamInfo paramMetadata) {
        if (S3_REGION_KEY.equals(paramMetadata.getName())) {
            return s3Region(componentId, paramsModel, paramMetadata);
        }
        return super.getInputComponent(componentId, paramsModel, paramMetadata);
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
}
