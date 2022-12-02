package gov.nasa.ziggy.ui.ops.triggers;

import java.util.List;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterClassTableModel extends AbstractTableModel {
    private List<ClassWrapper<Parameters>> list = null;

    public ParameterClassTableModel(List<ClassWrapper<Parameters>> list) {
        this.list = list;
    }

    @Override
    public int getColumnCount() {
        return 1;
    }

    @Override
    public int getRowCount() {
        return list.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return list.get(rowIndex);
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Type";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
