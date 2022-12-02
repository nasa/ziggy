package gov.nasa.ziggy.ui.ops.parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.pipeline.definition.TypedParameter;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterPropsTableModel extends AbstractTableModel {
    private final List<String> propNames;
    private final List<String> propValues;

    public ParameterPropsTableModel(Set<TypedParameter> properties) {
        propNames = new ArrayList<>(properties.size());
        propValues = new ArrayList<>(properties.size());

        for (TypedParameter property : properties) {
            propNames.add(property.getName());
            propValues.add(property.getString());
        }
    }

    /**
     * @see javax.swing.table.TableModel#getColumnCount()
     */
    @Override
    public int getColumnCount() {
        return 2;
    }

    /**
     * @see javax.swing.table.TableModel#getRowCount()
     */
    @Override
    public int getRowCount() {
        return propNames.size();
    }

    /**
     * @see javax.swing.table.TableModel#getValueAt(int, int)
     */
    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        switch (columnIndex) {
            case 0:
                return propNames.get(rowIndex);
            case 1:
                return propValues.get(rowIndex);
            default:
                return "Huh?";
        }
    }

    /**
     * @see javax.swing.table.AbstractTableModel#getColumnName(int)
     */
    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Name";
            case 1:
                return "Value";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
