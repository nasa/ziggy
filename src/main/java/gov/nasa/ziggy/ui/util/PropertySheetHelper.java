package gov.nasa.ziggy.ui.util;

import java.beans.PropertyEditor;
import java.util.Collection;

import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.ui.util.collections.ArrayPropertyEditor;
import gov.nasa.ziggy.ui.util.collections.ArrayTableCellRenderer;

/**
 * Helper methods for working with L2fprod's {@link PropertySheetPanel}.
 * <p>
 * Note: if and when additional public static populatePropertySheet methods are added, the
 * {@link Parameter#readFromObject(Object)} and {@link Parameter#writeToObject(Object)} must also be
 * modified to support whatever class is used as the argument to the new method.
 *
 * @author Todd Klaus
 * @author PT
 */
public class PropertySheetHelper {

    private PropertySheetHelper() {
    }

    /**
     * Helper method that populates a {@link PropertySheetPanel} with the contents of a
     * {@link ParameterSet} object. Automatically adds an {@link ArrayPropertyEditor} and an
     * {@link ArrayTableCellRenderer} for array fields that do not already have a custom
     * {@link PropertyEditor}.
     */
    public static void populatePropertySheet(ParameterSet parameterSet,
        PropertySheetPanel propertySheetPanel) throws Exception {
        propertySheetPanel.setProperties(parameterSet.getParameters().toArray(new Parameter[0]));
        propertySheetPanel.setRendererFactory(new ParameterRendererFactory());
        propertySheetPanel.setEditorFactory(new ParameterEditorFactory());
    }

    /**
     * Helper method that populates a {@link PropertySheetPanel} with the contents of a
     * {@link Collection} of {@link Parameter} objects. Automatically adds an
     * {@link ArrayPropertyEditor} and an {@link ArrayTableCellRenderer} for array fields that do
     * not already have a custom {@link PropertyEditor}.
     */
    public static void populatePropertySheet(Collection<Parameter> parameters,
        PropertySheetPanel propertySheetPanel) throws Exception {
        propertySheetPanel.setProperties(parameters.toArray(new Parameter[0]));
        propertySheetPanel.setRendererFactory(new ParameterRendererFactory());
        propertySheetPanel.setEditorFactory(new ParameterEditorFactory());
    }
}
