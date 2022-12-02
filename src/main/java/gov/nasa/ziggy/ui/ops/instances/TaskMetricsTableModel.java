package gov.nasa.ziggy.ui.ops.instances;

import java.util.List;

import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TaskMetricsTableModel extends AbstractTableModel {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TaskMetricsTableModel.class);

    private TaskMetricsDisplayModel taskMetricsDisplayModel = null;
    private boolean completedTasksOnly = false;

    public TaskMetricsTableModel(List<PipelineTask> tasks, List<String> orderedModuleNames,
        boolean completedTasksOnly) {
        this.completedTasksOnly = completedTasksOnly;
        update(tasks, orderedModuleNames);
    }

    public void update(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        try {
            taskMetricsDisplayModel = new TaskMetricsDisplayModel(tasks, orderedModuleNames,
                completedTasksOnly);
        } catch (ConsoleSecurityException ignore) {
        }

        fireTableDataChanged();
    }

    @Override
    public int getRowCount() {
        return taskMetricsDisplayModel.getRowCount();
    }

    @Override
    public int getColumnCount() {
        return taskMetricsDisplayModel.getColumnCount();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return taskMetricsDisplayModel.getValueAt(rowIndex, columnIndex);
    }

    @Override
    public String getColumnName(int column) {
        return taskMetricsDisplayModel.getColumnName(column);
    }
}
