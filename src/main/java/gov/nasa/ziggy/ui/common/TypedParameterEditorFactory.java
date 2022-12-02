package gov.nasa.ziggy.ui.common;

import java.beans.PropertyEditor;

import com.l2fprod.common.propertysheet.Property;
import com.l2fprod.common.propertysheet.PropertyEditorFactory;
import com.l2fprod.common.propertysheet.PropertyEditorRegistry;

import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.ui.collections.ArrayPropertyEditor;

/**
 * Creates new editors for {@link TypedParameter} instances based on their contents.
 *
 * @author PT
 */
public class TypedParameterEditorFactory implements PropertyEditorFactory {

    @Override
    public PropertyEditor createPropertyEditor(Property property) {
        if (!(property instanceof TypedParameter)) {
            return null;
        }
        TypedParameter typedProperty = (TypedParameter) property;
        if (!typedProperty.isScalar()) {
            return new ArrayPropertyEditor();
        }
        return PropertyEditorRegistry.INSTANCE.getEditor(typedProperty.getType());
    }
}
