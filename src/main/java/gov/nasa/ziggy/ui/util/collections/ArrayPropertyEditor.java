package gov.nasa.ziggy.ui.util.collections;

import java.awt.event.ActionListener;
import java.lang.reflect.Array;

import javax.swing.SwingUtilities;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.l2fprod.common.beans.editor.AbstractPropertyEditor;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.ui.util.MessageUtils;

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
            log.debug("value = {}, class={}", ReflectionToStringBuilder.toString(value),
                value.getClass().getSimpleName());
        }

        array = value;
        tableUi.setText(ZiggyDataType.objectToString(array));
    }

    protected void editArray() {
        ArrayEditorDialog dialog = null;
        try {
            Object newArray = ZiggyArrayUtils.copyArray(array, array.getClass().getComponentType());
            dialog = new ArrayEditorDialog(SwingUtilities.getWindowAncestor(editor), newArray);
            dialog.setVisible(true);
            if (!dialog.isCancelled()) {
                newArray = dialog.editedArray();
                tableUi.setText(ZiggyDataType.objectToString(newArray));
                firePropertyChange(array, newArray);
            }
        } catch (Exception e) {
            MessageUtils.showError(null, e);
        } finally {
            if (dialog != null) {
                dialog.dispose();
            }
        }
    }

    protected void selectNull() {
        Object originalArray = array;
        tableUi.setText("");

        Class<?> originalArrayComponentType = originalArray.getClass().getComponentType();
        array = Array.newInstance(originalArrayComponentType, 0);

        firePropertyChange(originalArray, array);
    }
}
