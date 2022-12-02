package gov.nasa.ziggy.ui.common;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.lang.reflect.Array;
import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.l2fprod.common.beans.ExtendedPropertyDescriptor;
import com.l2fprod.common.propertysheet.PropertySheetPanel;

import gov.nasa.ziggy.parameters.DefaultParameters;
import gov.nasa.ziggy.ui.collections.ArrayPropertyEditor;
import gov.nasa.ziggy.ui.collections.ArrayTableCellRenderer;

/**
 * Helper methods for working with L2fprod's {@link PropertySheetPanel}
 *
 * @author Todd Klaus
 */
public class PropertySheetHelper {
    private static final Logger log = LoggerFactory.getLogger(PropertySheetHelper.class);

    private PropertySheetHelper() {
    }

    /**
     * Initialize null fields since the property editors may not work correctly otherwise. This
     * can't be done in the property editors themselves since they only get an Object and don't know
     * the type (foo.getClass() doesn't work if foo is null!)
     *
     * @param parametersBean
     * @throws Exception
     */
    public static void deNullify(Object parametersBean) throws Exception {
        Class<?> beanClass = parametersBean.getClass();
        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {
            // allow us to access the value of private fields
            field.setAccessible(true);

            Object fieldValue = field.get(parametersBean);
            Class<?> fieldClass = field.getType();
            Object initialValue = null;

            if (fieldValue == null) {
                log.debug("Initializing null field: " + field.getName());

                if (fieldClass.isArray()) {
                    initialValue = Array.newInstance(fieldClass.getComponentType(), 0);
                } else {
                    initialValue = fieldClass.newInstance();
                }
                field.set(parametersBean, initialValue);
            }
        }
    }

    /**
     * Helper method that populates a {@link PropertySheetPanel} with the contents of a bean
     * instance. Automatically adds an {@link ArrayPropertyEditor} and an
     * {@link ArrayTableCellRenderer} for array fields that do not already have a custom
     * {@link PropertyEditor}.
     *
     * @param parametersBean
     * @param propertySheetPanel
     * @throws IntrospectionException
     */
    public static void populatePropertySheet(Object parametersBean,
        PropertySheetPanel propertySheetPanel) throws Exception {
        // initialize null fields so the property editors won't get tripped up.

        Class<?> beanClass = parametersBean.getClass();
        if (beanClass.equals(DefaultParameters.class)) {
            DefaultParameters parameters = (DefaultParameters) parametersBean;
            propertySheetPanel.setProperties(parameters.typedProperties());
            propertySheetPanel.setRendererFactory(new TypedParameterRendererFactory());
            propertySheetPanel.setEditorFactory(new TypedParameterEditorFactory());
        } else {
            PropertySheetHelper.deNullify(parametersBean);
            BeanInfo beanInfo;

            beanInfo = Introspector.getBeanInfo(beanClass, Object.class);
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

            /*
         * Add an ArrayPropertyEditor for fields that are arrays and don't already have a custom
         * PropertyEditor assigned.
         *
         * @formatter:off
         * if class is array
         *   if propertyDescriptor is NOT ExtendedPropertyDescriptor
         *     convert to ExtendedPropertyDescriptor
         *   if getPropertyEditorClass not set
         *     set to ArrayPropertyEditor
         *   if getPropertyTableRendererClass not set
         *     set to ArrayTableCellRenderer
         * add property to propertySheetPanel
         * @formatter:on
         */

            PropertyDescriptor[] newPropertyDescriptors = new PropertyDescriptor[propertyDescriptors.length];
            int index = 0;

            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                Class<?> clazz = propertyDescriptor.getPropertyType();
                log.debug("property class = " + clazz);

                if (clazz.isArray()) {
                    ExtendedPropertyDescriptor extendedPropertyDescriptor;

                    if (propertyDescriptor instanceof ExtendedPropertyDescriptor) {
                        extendedPropertyDescriptor = (ExtendedPropertyDescriptor) propertyDescriptor;
                    } else {
                        extendedPropertyDescriptor = ExtendedPropertyDescriptor
                            .newPropertyDescriptor(propertyDescriptor.getName(), beanClass);
                    }

                    if (extendedPropertyDescriptor.getPropertyEditorClass() == null) {
                        extendedPropertyDescriptor
                            .setPropertyEditorClass(ArrayPropertyEditor.class);
                    }

                    if (extendedPropertyDescriptor.getPropertyTableRendererClass() == null) {
                        extendedPropertyDescriptor
                            .setPropertyTableRendererClass(ArrayTableCellRenderer.class);
                    }

                    newPropertyDescriptors[index++] = extendedPropertyDescriptor;
                } else {
                    newPropertyDescriptors[index++] = propertyDescriptor;
                }
            }

            propertySheetPanel.setProperties(newPropertyDescriptors);
        }
        propertySheetPanel.readFromObject(parametersBean);
    }

}
