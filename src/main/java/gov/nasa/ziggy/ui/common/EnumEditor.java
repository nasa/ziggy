package gov.nasa.ziggy.ui.common;

import com.l2fprod.common.beans.editor.ComboBoxPropertyEditor;

/**
 * Property editor for enumerated types
 *
 * @author Todd Klaus
 */
public class EnumEditor extends ComboBoxPropertyEditor {
    Object value;

    public EnumEditor() {
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setValue(Object value) {
        this.value = value;

        Object[] values = value.getClass().getEnumConstants();

        String[] stringValues = new String[values.length];

        for (int i = 0; i < values.length; i++) {
            stringValues[i] = values[i].toString();
        }

        setAvailableValues(stringValues);
    }
}
