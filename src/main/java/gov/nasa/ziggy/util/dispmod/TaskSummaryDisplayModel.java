package gov.nasa.ziggy.util.dispmod;

import gov.nasa.ziggy.util.TasksStates;
import gov.nasa.ziggy.util.TasksStates.Summary;

/**
 * {@link DisplayModel} for the pipeline task summary. This class is used to format the pipeline
 * task summary for display on the console.
 *
 * @author Todd Klaus
 */
public class TaskSummaryDisplayModel extends DisplayModel {

    private static final String[] COLUMN_NAMES = { "Module", "Submitted", "Processing", "Completed",
        "Failed", "Subtasks" };

    private TasksStates taskStates = new TasksStates();

    public TaskSummaryDisplayModel() {
    }

    public TaskSummaryDisplayModel(TasksStates taskStates) {
        this.taskStates = taskStates;
    }

    public void update(TasksStates taskStates) {
        this.taskStates = taskStates;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        int moduleCount = taskStates.getModuleNames().size();
        boolean isTotalsRow = rowIndex == moduleCount;
        String moduleName = "";
        TasksStates.Summary moduleSummary = null;

        if (!isTotalsRow) {
            moduleName = taskStates.getModuleNames().get(rowIndex);
            moduleSummary = taskStates.getModuleStates().get(moduleName);
        }

        switch (columnIndex) {
            case 0: // Module
                return moduleName;
            case 1: // Submitted
                if (isTotalsRow) {
                    return taskStates.getTotalSubmittedCount();
                } else {
                    return moduleSummary.getSubmittedCount();
                }
            case 2: // Processing
                if (isTotalsRow) {
                    return taskStates.getTotalProcessingCount();
                } else {
                    return moduleSummary.getProcessingCount();
                }
            case 3: // Completed
                if (isTotalsRow) {
                    return taskStates.getTotalCompletedCount();
                } else {
                    return moduleSummary.getCompletedCount();
                }
            case 4: // Failed
                if (isTotalsRow) {
                    return taskStates.getTotalErrorCount();
                } else {
                    return moduleSummary.getErrorCount();
                }
            case 5: // SubTasks
                if (isTotalsRow) {
                    return taskStates.getTotalSubTaskTotalCount() + " / "
                        + taskStates.getTotalSubTaskCompleteCount() + " / "
                        + taskStates.getTotalSubTaskFailedCount();
                } else {
                    return moduleSummary.getSubTaskTotalCount() + " / "
                        + moduleSummary.getSubTaskCompleteCount() + " / "
                        + moduleSummary.getSubTaskFailedCount();
                }
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    public Summary getContentAtRow(int row) {
        return taskStates.getModuleStates().get(taskStates.getModuleNames().get(row));
    }

    @Override
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }

    @Override
    public int getRowCount() {
        if (taskStates != null) {
            return taskStates.getModuleNames().size() + 1;
        }
        return 0;
    }

    public TasksStates getTaskStates() {
        return taskStates;
    }
}
