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

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Check;
import org.apache.wicket.markup.html.form.CheckGroup;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.util.SetModel;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * A labelled group of checkboxes over a fixed list of choices, whose model is the set of checked choice codes. It is
 * the multi-select counterpart of the single-select {@link RadioGroupParamPanel}: same label plus {@link ListView}
 * shape and choice-label function, with {@code CheckGroup}/{@code Check} in place of {@code RadioGroup}/{@code Radio}
 * and a set of checked codes in place of a single selected value.
 *
 * <p>The checked set is transient panel state, never a persisted connection parameter. The panel owns a private
 * {@link SetModel} seeded from a plain set and never binds the group to a store's connection-parameters map, which
 * keeps the multi-select a pure view over which backends' fields to show and stops it from ever writing a
 * {@code storage.provider} key (which the STAC store deliberately has none of).
 *
 * <p>Adapted from GeoServer's {@code RadioGroupParamPanel} (c) Open Source Geospatial Foundation, GPL-2.0.
 */
@SuppressWarnings("serial")
public class CheckGroupParamPanel extends Panel {

    private final CheckGroup<String> group;

    /**
     * @param id the wicket id of this panel
     * @param label the label naming the whole checkbox group
     * @param initiallyChecked the choice codes to pre-check; copied into the panel's own transient model
     * @param choices the selectable choice codes, one checkbox each, in render order
     * @param choiceLabels maps a choice code to the label shown beside its checkbox
     */
    public CheckGroupParamPanel(
            String id,
            IModel<String> label,
            Set<String> initiallyChecked,
            List<String> choices,
            SerializableFunction<String, IModel<String>> choiceLabels) {
        super(id);
        group = new CheckGroup<>("group", new SetModel<>(new LinkedHashSet<>(initiallyChecked)));
        group.add(new DynamicCheckChoices("choices", choices, choiceLabels));
        add(new Label("paramName", label));
        add(group);
    }

    /** The check group whose model object is the set of currently checked choice codes. */
    public CheckGroup<String> getFormComponent() {
        return group;
    }

    /** ListView that renders one checkbox per choice code. */
    private static class DynamicCheckChoices extends ListView<String> {

        private final SerializableFunction<String, IModel<String>> choiceLabels;

        DynamicCheckChoices(
                String id, List<String> choices, SerializableFunction<String, IModel<String>> choiceLabels) {
            super(id, choices);
            this.choiceLabels = choiceLabels;
        }

        @Override
        protected void populateItem(ListItem<String> item) {
            item.add(new StableValueCheck("paramValue", item.getModel()));
            IModel<String> labelModel = labelModel(item.getModelObject());
            item.add(new Label("label", labelModel));
        }

        private IModel<String> labelModel(String code) {
            IModel<String> labelModel = choiceLabels.apply(code);
            if (labelModel == null) {
                labelModel = new Model<>(code);
            }
            return labelModel;
        }
    }

    /**
     * A checkbox whose submitted form value is its choice code rather than Wicket's default per-render generated value.
     * The list rebuilds its checkboxes on each render of the group; a code-based value stays stable across those
     * rebuilds, keeping the selection intact when the surrounding form re-renders after a failed submit.
     */
    private static final class StableValueCheck extends Check<String> {

        StableValueCheck(String id, IModel<String> code) {
            super(id, code);
        }

        @Override
        public String getValue() {
            return getModelObject();
        }
    }
}
