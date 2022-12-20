package gov.nasa.ziggy.ui.collections;

import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.List;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.collections.ZiggyArrayUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;

/**
 * Table model for editing an Object[]
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ArrayEditorTableModel extends AbstractTableModel {

    private List<Object> elements = new LinkedList<>();

    private ZiggyDataType componentType;

    public ArrayEditorTableModel(Object array) {
        int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            elements.add(Array.get(array, i));
        }

        componentType = ZiggyDataType.getDataType(array);
    }

    public Object asArray() {
        Object newArray = ZiggyArrayUtils.constructFullArray(new long[] { elements.size() },
            componentType, false);

        for (int index = 0; index < elements.size(); index++) {
            Array.set(newArray, index, elements.get(index));
        }

        return newArray;
    }

    public List<String> asStringList() {
        List<String> newList = new LinkedList<>();
        for (Object element : elements) {
            newList.add(ZiggyDataType.objectToString(element));
        }
        return newList;
    }

    public void replaceWith(List<String> newValues) {
        elements = new LinkedList<>();
        for (String newValue : newValues) {
            elements.add(componentType.typedValue(newValue));
        }

        fireTableDataChanged();
    }

    public void insertElementAt(int index, String text) {
        elements.add(index, componentType.typedValue(text));
        fireTableDataChanged();
    }

    public void insertElementAtEnd(String text) {
        insertElementAt(elements.size(), text);
    }

    public void removeElementAt(int selectedIndex) {
        elements.remove(selectedIndex);
        fireTableDataChanged();
    }

    @Override
    public int getColumnCount() {
        return 2;
    }

    @Override
    public int getRowCount() {
        return elements.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        switch (columnIndex) {
            case 0:
                return rowIndex;
            case 1:
                return elements.get(rowIndex);
            default:
                throw new IllegalArgumentException("invalid columnIndex = " + columnIndex);
        }
    }

    @Override
    public void setValueAt(Object value, int rowIndex, int columnIndex) {
        if (columnIndex == 1) {
            elements.set(rowIndex, value);
        }
    }

    @Override
    public String getColumnName(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return "idx";
            case 1:
                return "value";
            default:
                throw new IllegalArgumentException("invalid columnIndex = " + columnIndex);
        }
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        return columnIndex == 1;
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        switch (columnIndex) {
            case 0:
                return Integer.class;
            case 1:
                return componentType.getJavaBoxedClass();
            default:
                throw new IllegalArgumentException("invalid columnIndex = " + columnIndex);
        }
    }

}
