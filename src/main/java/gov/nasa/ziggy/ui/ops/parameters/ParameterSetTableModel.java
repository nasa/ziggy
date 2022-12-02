package gov.nasa.ziggy.ui.ops.parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetTableModel extends AbstractTableModel {
    protected List<ParameterSet> paramSets = new ArrayList<>();
    protected List<ClassWrapper<Parameters>> paramSetTypes = new ArrayList<>();

    public ParameterSetTableModel(Map<ClassWrapper<Parameters>, ParameterSet> parameterSetsMap) {
        update(parameterSetsMap);
    }

    public void update(Map<ClassWrapper<Parameters>, ParameterSet> parameterSetsMap) {
        paramSets.clear();
        paramSetTypes.clear();

        if (parameterSetsMap != null) {
            for (ClassWrapper<Parameters> classWrapper : parameterSetsMap.keySet()) {
                ParameterSet paramSet = parameterSetsMap.get(classWrapper);

                paramSets.add(paramSet);
                paramSetTypes.add(classWrapper);
            }
        }

        fireTableDataChanged();
    }

    public ParameterSet getParamSetAtRow(int rowIndex) {
        return paramSets.get(rowIndex);
    }

    @Override
    public int getRowCount() {
        return paramSets.size();
    }

    @Override
    public int getColumnCount() {
        return 2;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        ParameterSet paramSet = paramSets.get(rowIndex);
        ClassWrapper<Parameters> paramSetType = paramSetTypes.get(rowIndex);

        switch (columnIndex) {
            case 0:
                return paramSetType.getClazz().getSimpleName();
            case 1:
                return paramSet.getName();
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Type";
            case 1:
                return "Selected Parameter Set";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
