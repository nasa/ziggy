package gov.nasa.ziggy.ui.util;

import java.beans.PropertyEditor;

import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.ui.util.collections.ArrayPropertyEditor;
import gov.nasa.ziggy.ui.util.collections.ArrayTableCellRenderer;

/**
 * Helper methods for working with L2fprod's {@link PropertySheetPanel}
 *
 * @author Todd Klaus
 */
public class PropertySheetHelper {

    private PropertySheetHelper() {
    }

    /**
     * Helper method that populates a {@link PropertySheetPanel} with the contents of a Parameters
     * object. Automatically adds an {@link ArrayPropertyEditor} and an
     * {@link ArrayTableCellRenderer} for array fields that do not already have a custom
     * {@link PropertyEditor}.
     */
    public static void populatePropertySheet(ParametersInterface parameters,
        PropertySheetPanel propertySheetPanel) throws Exception {
        propertySheetPanel.setProperties(parameters.getParameters().toArray(new TypedParameter[0]));
        propertySheetPanel.setRendererFactory(new TypedParameterRendererFactory());
        propertySheetPanel.setEditorFactory(new TypedParameterEditorFactory());
    }
}
