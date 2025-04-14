package gov.nasa.ziggy.ui.instances;

import java.util.List;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.util.dispmod.ModelContentClass;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel.PipelineStepTaskMetrics;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class TaskMetricsTableModel extends AbstractTableModel
    implements ModelContentClass<PipelineStepTaskMetrics> {

    private TaskMetricsDisplayModel taskMetricsDisplayModel;
    private boolean completedTasksOnly;

    public TaskMetricsTableModel(List<PipelineTaskDisplayData> tasks,
        List<String> orderedPipelineStepNames, boolean completedTasksOnly) {
        this.completedTasksOnly = completedTasksOnly;
        update(tasks, orderedPipelineStepNames);
    }

    public void update(List<PipelineTaskDisplayData> tasks, List<String> orderedPipelineStepNames) {
        taskMetricsDisplayModel = new TaskMetricsDisplayModel(tasks, orderedPipelineStepNames,
            completedTasksOnly);
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
    public Class<PipelineStepTaskMetrics> tableModelContentClass() {
        return PipelineStepTaskMetrics.class;
    }
}
