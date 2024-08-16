package gov.nasa.ziggy.util.dispmod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;

import gov.nasa.ziggy.metrics.TaskMetrics;
import gov.nasa.ziggy.metrics.TimeAndPercentile;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;

/**
 * Display a table containing a row for each pipeline module and a column for each category defined
 * in PipelineTask.summaryMetrics.
 * <p>
 * The cells of the table contain the total time spent on each category for all tasks for the
 * pipeline module and the percentage of the total processing time for all of the tasks.
 *
 * @author Todd Klaus
 */
public class TaskMetricsDisplayModel extends DisplayModel {

    private List<ModuleTaskMetrics> categorySummariesByModule = new LinkedList<>();
    private List<String> seenCategories = new ArrayList<>();
    private int numColumns = 0;

    private boolean completedTasksOnly = false;

    public TaskMetricsDisplayModel(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        this(tasks, orderedModuleNames, true);
    }

    public TaskMetricsDisplayModel(List<PipelineTask> tasks, List<String> orderedModuleNames,
        boolean completedTasksOnly) {
        this.completedTasksOnly = completedTasksOnly;

        update(tasks, orderedModuleNames);
    }

    private void update(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        categorySummariesByModule = new LinkedList<>();
        seenCategories = new ArrayList<>();

        Map<String, List<PipelineTask>> tasksByModule = new HashMap<>();

        // partition the tasks by module
        for (PipelineTask task : tasks) {
            if (!completedTasksOnly || task.getProcessingStep() == ProcessingStep.COMPLETE) {
                String moduleName = task.getModuleName();

                List<PipelineTask> taskListForModule = tasksByModule.get(moduleName);
                if (taskListForModule == null) {
                    taskListForModule = new LinkedList<>();
                    tasksByModule.put(moduleName, taskListForModule);
                }
                taskListForModule.add(task);
            }
        }

        // for each module, aggregate the summary metrics by category
        // and build a list of categories
        for (String moduleName : orderedModuleNames) {
            List<PipelineTask> taskListForModule = tasksByModule.get(moduleName);
            TaskMetrics taskMetrics = new TaskMetrics(taskListForModule);
            taskMetrics.calculate();
            categorySummariesByModule.add(new ModuleTaskMetrics(moduleName, taskMetrics));

            Set<String> categories = taskMetrics.getCategoryMetrics().keySet();
            for (String category : categories) {
                if (!seenCategories.contains(category)) {
                    seenCategories.add(category);
                }
            }
        }
        numColumns = seenCategories.size() + 3;
    }

    @Override
    public int getColumnCount() {
        return numColumns;
    }

    @Override
    public String getColumnName(int column) {
        if (column == 0) {
            return "Module";
        }
        if (column == 1) {
            return "Total";
        }
        if (column == numColumns - 1) {
            return "Other";
        }
        return seenCategories.get(column - 2);
    }

    @Override
    public int getRowCount() {
        return categorySummariesByModule.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        ModuleTaskMetrics row = categorySummariesByModule.get(rowIndex);

        if (columnIndex == 0) {
            return row.getModuleName(); // module name
        }
        if (columnIndex == 1) {
            return formatDuration(row.getTaskMetrics().getTotalProcessingTimeMillis()); // total
        }
        if (columnIndex == numColumns - 1) {
            return categoryValuesString(row.getTaskMetrics().getUnallocatedTime());
        }
        String category = seenCategories.get(columnIndex - 2);
        TimeAndPercentile categoryValues = row.getTaskMetrics().getCategoryMetrics().get(category);
        return categoryValuesString(categoryValues);
    }

    private String categoryValuesString(TimeAndPercentile categoryValues) {
        if (categoryValues != null) {
            long categoryTimeMillis = categoryValues.getTimeMillis();
            double categoryPercent = categoryValues.getPercent();
            return String.format("%s (%4.1f%%)", formatDuration(categoryTimeMillis),
                categoryPercent);
        }
        return "--";
    }

    private String formatDuration(long durationMillis) {
        return durationMillis >= 0 ? DurationFormatUtils.formatDuration(durationMillis, "HHH:mm:ss")
            : Long.toString(durationMillis / 1000);
    }

    public static class ModuleTaskMetrics {

        private final String moduleName;
        private final TaskMetrics taskMetrics;

        public ModuleTaskMetrics(String moduleName, TaskMetrics taskMetrics) {
            this.moduleName = moduleName;
            this.taskMetrics = taskMetrics;
        }

        public String getModuleName() {
            return moduleName;
        }

        public TaskMetrics getTaskMetrics() {
            return taskMetrics;
        }

        @Override
        public int hashCode() {
            return Objects.hash(moduleName, taskMetrics);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ModuleTaskMetrics other = (ModuleTaskMetrics) obj;
            return Objects.equals(moduleName, other.moduleName)
                && Objects.equals(taskMetrics, other.taskMetrics);
        }
    }
}
