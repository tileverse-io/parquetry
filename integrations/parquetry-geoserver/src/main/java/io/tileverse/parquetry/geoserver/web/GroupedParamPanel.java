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

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;

/**
 * Wraps one connection-parameter field, optionally titling it with a section header. The store edit panel passes a
 * header model for the first field of each titled backend group and {@code null} for the rest, which names each group
 * once and leaves the remaining fields bare. Toggling this wrapper's visibility hides the header together with its
 * field, keeping a deselected group from leaving an orphan header behind.
 *
 * <p>The wrapper takes the {@code componentId} the store page assigns to a parameter field; the wrapped field takes the
 * wrapper's own {@link #FIELD_ID} child slot.
 */
@SuppressWarnings("serial")
class GroupedParamPanel extends Panel {

    /** The wicket id the wrapped field panel must be constructed with to nest inside this wrapper. */
    static final String FIELD_ID = "field";

    GroupedParamPanel(String id, Panel field, IModel<String> headerOrNull) {
        super(id);
        add(sectionHeader(headerOrNull));
        add(field);
    }

    private static Label sectionHeader(IModel<String> headerOrNull) {
        Label header = new Label("header", headerOrNull != null ? headerOrNull : Model.of(""));
        header.setVisible(headerOrNull != null);
        return header;
    }
}
