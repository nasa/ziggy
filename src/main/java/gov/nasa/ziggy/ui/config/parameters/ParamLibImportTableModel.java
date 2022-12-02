package gov.nasa.ziggy.ui.config.parameters;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.parameters.ParameterSetDescriptor;
import gov.nasa.ziggy.parameters.ParameterSetDescriptor.State;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParamLibImportTableModel extends AbstractTableModel {
    private List<ParameterSetDescriptor> paramMap = new LinkedList<>();
    // private List<String> names = new ArrayList<>();
    private List<Boolean> includeFlags = new ArrayList<>();

    public ParamLibImportTableModel() {
    }

    public ParamLibImportTableModel(List<ParameterSetDescriptor> paramMap) {
        this.paramMap = paramMap;

        // names = new ArrayList<>(paramMap.keySet());
        // initially in alphabetical order (until the user sorts by some other column)
        // Collections.sort(names);

        // everything included by default
        includeFlags = new ArrayList<>();
        for (@SuppressWarnings("unused")
        ParameterSetDescriptor param : paramMap) {
            includeFlags.add(true);
        }
    }

    public List<String> getExcludeList() {
        List<String> excludeList = new LinkedList<>();

        for (int index = 0; index < paramMap.size(); index++) {
            if (!includeFlags.get(index)) {
                excludeList.add(paramMap.get(index).getName());
            }
        }
        return excludeList;
    }

    public ParameterSetDescriptor getDescriptorAt(int rowIndex) {
        // String name = names.get(rowIndex);
        // ParameterSetDescriptor param = paramMap.get(name);

        return paramMap.get(rowIndex);
    }

    @Override
    public int getColumnCount() {
        return 4;
    }

    @Override
    public int getRowCount() {
        return paramMap.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        // String name = names.get(rowIndex);
        boolean include = includeFlags.get(rowIndex);
        ParameterSetDescriptor param = paramMap.get(rowIndex);
        String className = param.shortClassName();

        switch (columnIndex) {
            case 0:
                return include;
            case 1:
                return param.getName();
            case 2:
                return className;
            case 3:
                State state = param.getState();
                String color = "black";

                switch (state) {
                    case CREATE:
                        color = "blue";
                        break;

                    case IGNORE:
                    case CLASS_MISSING:
                        color = "maroon";
                        break;

                    case SAME:
                        color = "green";
                        break;

                    case UPDATE:
                        color = "red";
                        break;

                    case LIBRARY_ONLY:
                        color = "purple";
                        break;

                    case NONE:
                        color = "black";
                        break;

                    default:
                }

                return "<html><b><font color=" + color + ">" + state.toString()
                    + "</font></b></html>";
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Include";
            case 1:
                return "Parameter Set Name";
            case 2:
                return "Class";
            case 3:
                return "Action";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        if (columnIndex == 0) {
            return true;
        } else {
            return super.isCellEditable(rowIndex, columnIndex);
        }
    }

    @Override
    public void setValueAt(Object value, int rowIndex, int columnIndex) {
        if (columnIndex == 0) {
            Boolean newInclude = (Boolean) value;
            includeFlags.set(rowIndex, newInclude);
        } else {
            throw new IllegalArgumentException("read-only columnIndex = " + columnIndex);
        }
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        if (columnIndex == 0) {
            return Boolean.class;
        } else {
            return super.getColumnClass(columnIndex);
        }
    }
}
