package gov.nasa.ziggy.ui.collections;

import java.awt.event.ActionListener;
import java.lang.reflect.Array;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.l2fprod.common.beans.editor.AbstractPropertyEditor;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;

/**
 * Custom property editor for Java arrays
 *
 * @author Todd Klaus
 */
public class ArrayPropertyEditor extends AbstractPropertyEditor {
    private static final Logger log = LoggerFactory.getLogger(ArrayPropertyEditor.class);

    private Object array;
    private final InTableButtonsPanel tableUi;

    public ArrayPropertyEditor() {
        this(false);
    }

    public ArrayPropertyEditor(boolean asTableEditor) {
        ActionListener editButtonListener = e -> editArray();

        ActionListener cancelButtonListener = e -> selectNull();

        tableUi = new InTableButtonsPanel(asTableEditor, editButtonListener, cancelButtonListener);

        editor = tableUi;
    }

    @Override
    public Object getValue() {
        log.debug("getValue() called");

        Object newArray = null;
        String text = tableUi.getText();

        if (text.length() > 0) {
            newArray = ZiggyDataType.stringToObject(text, ZiggyDataType.getDataType(array), true);
        }

        return newArray;
    }

    @Override
    public void setValue(Object value) {
        if (value != null) {
            log.debug("setValue() called with value = " + ReflectionToStringBuilder.toString(value)
                + ", class = " + value.getClass().getSimpleName());
        }

        array = value;
        tableUi.setText(ZiggyDataType.objectToString(array));
    }

    protected void editArray() {
        log.debug("editArray() called");

        Object newArray = ZiggyArrayUtils.copyArray(array, array.getClass().getComponentType());
        ArrayEditorDialog dialog = ArrayEditorDialog.newDialog(editor, newArray);
        try {
            dialog.setVisible(true);
            if (!dialog.wasCancelled()) {
                newArray = dialog.editedArray();
                tableUi.setText(ZiggyDataType.objectToString(newArray));
                firePropertyChange(array, newArray);
            }
        } finally {
            dialog.dispose();
        }
    }

    protected void selectNull() {
        log.debug("selectNull() called");

        Object originalArray = array;
        tableUi.setText("");

        Class<?> originalArrayComponentType = originalArray.getClass().getComponentType();
        array = Array.newInstance(originalArrayComponentType, 0);

        firePropertyChange(originalArray, array);
    }
}
