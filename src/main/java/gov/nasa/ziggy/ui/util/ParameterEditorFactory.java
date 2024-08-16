package gov.nasa.ziggy.ui.util;

import java.beans.PropertyEditor;

import com.l2fprod.common.propertysheet.Property;
import com.l2fprod.common.propertysheet.PropertyEditorFactory;
import com.l2fprod.common.propertysheet.PropertyEditorRegistry;

import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.ui.util.collections.ArrayPropertyEditor;

/**
 * Creates new editors for {@link Parameter} instances based on their contents.
 *
 * @author PT
 */
public class ParameterEditorFactory implements PropertyEditorFactory {

    @Override
    public PropertyEditor createPropertyEditor(Property property) {
        if (!(property instanceof Parameter)) {
            return null;
        }
        Parameter typedProperty = (Parameter) property;
        if (!typedProperty.isScalar()) {
            return new ArrayPropertyEditor(true);
        }
        return PropertyEditorRegistry.INSTANCE.getEditor(typedProperty.getType());
    }
}
