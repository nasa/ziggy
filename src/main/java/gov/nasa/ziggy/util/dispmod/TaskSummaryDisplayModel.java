package gov.nasa.ziggy.util.dispmod;

import gov.nasa.ziggy.util.TasksStates;

/**
 * {@link DisplayModel} for the pipeline task summary. This class is used to format the pipeline
 * task summary for display on the console.
 *
 * @author Todd Klaus
 */
public class TaskSummaryDisplayModel extends DisplayModel {
    private static final int NUM_COLUMNS = 6;
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
                    String allTotals = taskStates.getTotalSubTaskTotalCount() + " / "
                        + taskStates.getTotalSubTaskCompleteCount() + " / "
                        + taskStates.getTotalSubTaskFailedCount();
                    return allTotals;
                } else {
                    String moduleTotals = moduleSummary.getSubTaskTotalCount() + " / "
                        + moduleSummary.getSubTaskCompleteCount() + " / "
                        + moduleSummary.getSubTaskFailedCount();
                    return moduleTotals;
                }
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public int getColumnCount() {
        return NUM_COLUMNS;
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Module";
            case 1:
                return "Submitted";
            case 2:
                return "Processing";
            case 3:
                return "Completed";
            case 4:
                return "Failed";
            case 5:
                return "SubTasks";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }

    @Override
    public int getRowCount() {
        if (taskStates != null) {
            return taskStates.getModuleNames().size() + 1;
        } else {
            return 0;
        }
    }

    public TasksStates getTaskStates() {
        return taskStates;
    }
}
