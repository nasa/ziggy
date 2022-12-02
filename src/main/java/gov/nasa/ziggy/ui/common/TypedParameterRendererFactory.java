package gov.nasa.ziggy.ui.common;

import javax.swing.table.TableCellRenderer;

import com.l2fprod.common.propertysheet.Property;
import com.l2fprod.common.propertysheet.PropertyRendererFactory;
import com.l2fprod.common.propertysheet.PropertyRendererRegistry;

import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.ui.collections.ArrayTableCellRenderer;

/**
 * Generates new renderers for {@link TypedParameter} instances based on their contents.
 *
 * @author PT
 */
public class TypedParameterRendererFactory implements PropertyRendererFactory {

    /**
     * Returns an appropriate {@link TableCellRenderer} for a {@link TypedParameter}. for arrays,
     * the {@link ArrayTableCellRenderer} is returned. For scalar values, the default renderers from
     * the {@link PropertyRendererRegistry} are used.
     */
    @Override
    public TableCellRenderer createTableCellRenderer(Property property) {
        if (!(property instanceof TypedParameter)) {
            return null;
        }
        TypedParameter typedProperty = (TypedParameter) property;
        if (!typedProperty.isScalar()) {
            return new ArrayTableCellRenderer();
        }
        return createTableCellRenderer(typedProperty.getDataType().getClass());
    }

    /**
     * Returns {@link TableCellRenderer} instances given a class to be rendered. The default
     * renderers from the {@link PropertyRendererRegistry} are used.
     */
    @Override
    public TableCellRenderer createTableCellRenderer(Class clazz) {
        return new PropertyRendererRegistry().getRenderer(clazz);
    }

}
