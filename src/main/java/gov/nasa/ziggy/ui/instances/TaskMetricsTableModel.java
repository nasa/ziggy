package gov.nasa.ziggy.ui.instances;

import java.util.List;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.models.TableModelContentClass;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel.ModuleTaskMetrics;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TaskMetricsTableModel extends AbstractTableModel
    implements TableModelContentClass<ModuleTaskMetrics> {

    private TaskMetricsDisplayModel taskMetricsDisplayModel;
    private boolean completedTasksOnly;

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

    @Override
    public Class<ModuleTaskMetrics> tableModelContentClass() {
        return ModuleTaskMetrics.class;
    }
}
