package gov.nasa.ziggy.ui.ops.instances;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.util.dispmod.TaskSummaryDisplayModel;

@SuppressWarnings("serial")
public class TaskSummaryTableModel extends AbstractTableModel {
    private final TaskSummaryDisplayModel taskSummaryDisplayModel = new TaskSummaryDisplayModel();

    public TaskSummaryTableModel() {
    }

    public void update(TasksTableModel tasksTableModel) {
        taskSummaryDisplayModel.update(tasksTableModel.getTaskStates());

        fireTableDataChanged();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return taskSummaryDisplayModel.getValueAt(rowIndex, columnIndex);
    }

    @Override
    public int getColumnCount() {
        return taskSummaryDisplayModel.getColumnCount();
    }

    @Override
    public String getColumnName(int column) {
        return taskSummaryDisplayModel.getColumnName(column);
    }

    @Override
    public int getRowCount() {
        return taskSummaryDisplayModel.getRowCount();
    }
}
