package gov.nasa.ziggy.util.dispmod;

import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.Counts;

/**
 * {@link DisplayModel} for the pipeline task summary. This class is used to format the pipeline
 * task summary for display on the console.
 *
 * @author Todd Klaus
 */
public class TaskSummaryDisplayModel extends DisplayModel {

    private static final String[] COLUMN_NAMES = { "Node", "Waiting to run", "Processing",
        "Completed", "Failed", "Subtasks" };

    private TaskCounts taskCounts = new TaskCounts();

    public TaskSummaryDisplayModel() {
    }

    public TaskSummaryDisplayModel(TaskCounts taskCounts) {
        this.taskCounts = taskCounts;
    }

    public void update(TaskCounts taskCounts) {
        this.taskCounts = taskCounts;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        int nodeCount = taskCounts.getPipelineStepNames().size();
        boolean isTotalsRow = rowIndex == nodeCount;
        String pipelineStepName = isTotalsRow ? ""
            : taskCounts.getPipelineStepNames().get(rowIndex);
        TaskCounts.Counts counts = isTotalsRow ? taskCounts.getTotalCounts()
            : taskCounts.getPipelineStepCounts().get(pipelineStepName);

        return switch (columnIndex) {
            case 0 -> pipelineStepName;
            case 1 -> counts.getWaitingToRunTaskCount();
            case 2 -> counts.getProcessingTaskCount();
            case 3 -> counts.getCompletedTaskCount();
            case 4 -> counts.getFailedTaskCount();
            case 5 -> counts.subtaskCountsLabel();
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };
    }

    @Override
    public int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    public Counts getContentAtRow(int row) {
        return taskCounts.getPipelineStepCounts().get(taskCounts.getPipelineStepNames().get(row));
    }

    @Override
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }

    @Override
    public int getRowCount() {
        if (taskCounts != null) {
            return taskCounts.getPipelineStepNames().size() + 1;
        }
        return 0;
    }

    public TaskCounts getTaskCounts() {
        return taskCounts;
    }
}
