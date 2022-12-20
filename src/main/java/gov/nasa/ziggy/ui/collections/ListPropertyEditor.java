package gov.nasa.ziggy.ui.collections;

import java.awt.event.ActionListener;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;

import javax.swing.event.ListSelectionEvent;

import org.apache.commons.lang3.StringUtils;

import com.l2fprod.common.beans.editor.AbstractPropertyEditor;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;

/**
 * Edit arrays with a JList. This will not work with arrays of primitive types.
 *
 * @author Sean McCauliff
 */
public class ListPropertyEditor extends AbstractPropertyEditor {
    private Object[] currentValues;
    /** This array is sorted. */
    private Object[] availableValues;
    private Comparator<Object> comp;
    private Class<?> arrayComponentType;
    private final InTableButtonsPanel tableUi;
    private final String title;
    private ListEditorDialog dialog;

    public ListPropertyEditor(String title) {
        this.title = title;

        ActionListener editActionListener = e -> showEditorUi();

        ActionListener cancelActionListener = e -> eraseValue();

        tableUi = new InTableButtonsPanel(false, editActionListener, cancelActionListener);
        tableUi.setEditable(false);
        editor = tableUi;
    }

    private int[] selectedIndicesFromOldValues() {
        final int[] selectedIndices = new int[currentValues.length];
        for (int i = 0; i < currentValues.length; i++) {
            selectedIndices[i] = Arrays.binarySearch(availableValues, currentValues[i], comp);
        }
        return selectedIndices;
    }

    private void showEditorUi() {
        dialog = ListEditorDialog.newDialog(editor);
        dialog.setAvailableValues(availableValues);
        if (currentValues != null) {
            dialog.setSelectedIndices(selectedIndicesFromOldValues());
        }
        dialog.addListSelectionListener(this::listChanged);
        dialog.setTitle(title);
        dialog.setLocationRelativeTo(tableUi);
        dialog.pack();
        Object[] oldValues = currentValues;
        // Important! modal.
        dialog.setVisible(true);
        tableUi.setText(valuesToString());
        // Important! Only do this once per edit. Because the editor component
        // gets removed after the first property change is triggered.
        firePropertyChange(oldValues, currentValues);
    }

    private void eraseValue() {
        setValue(Array.newInstance(arrayComponentType, 0));
    }

    private void listChanged(ListSelectionEvent event) {
        if (event.getValueIsAdjusting()) {
            return;
        }

        final Object[] fromListValues = dialog.getSelectedValues();
        currentValues = (Object[]) ZiggyArrayUtils.copyArray(fromListValues, arrayComponentType);
        tableUi.setText(valuesToString());
    }

    /**
     * @return Here Object is actually an array of the arrayComponentType.
     */
    @Override
    public Object getValue() {
        return currentValues;
    }

    /**
     * @param objValue is actually an array of objects.
     */
    @Override
    public void setValue(Object objValue) {
        final Object[] values = (Object[]) objValue;
        if (values == null) {
            currentValues = null;
            return;
        }
        currentValues = values;

        // jList.setSelectedIndices(selectedIndices);
        arrayComponentType = objValue.getClass().getComponentType();
        tableUi.setText(valuesToString());
    }

    public void setAvailableValues(Object[] values, Comparator<Object> comp) {
        availableValues = Arrays.copyOf(values, values.length);
        Arrays.sort(availableValues, comp);
        this.comp = comp;
    }

    public void setArrayComponentType(Class<?> arrayComponentType) {
        this.arrayComponentType = arrayComponentType;
    }

    private String valuesToString() {
        if (currentValues == null) {
            return "";
        }

        return StringUtils.join(currentValues, ", ");
    }
}
