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
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;

/**
 * Display a table containing a row for each pipeline node and a column for each category defined in
 * PipelineTask.pipelineTaskMetrics.
 * <p>
 * The cells of the table contain the total time spent on each category for all tasks for the
 * pipeline node and the percentage of the total processing time for all of the tasks.
 *
 * @author Todd Klaus
 */
public class TaskMetricsDisplayModel extends DisplayModel {

    private List<PipelineStepTaskMetrics> categorySummariesByNode = new LinkedList<>();
    private List<String> seenCategories = new ArrayList<>();
    private int numColumns = 0;

    private boolean completedTasksOnly = false;

    public TaskMetricsDisplayModel(List<PipelineTaskDisplayData> tasks,
        List<String> orderedPipelineStepNames) {
        this(tasks, orderedPipelineStepNames, true);
    }

    public TaskMetricsDisplayModel(List<PipelineTaskDisplayData> tasks,
        List<String> orderedPipelineStepNames, boolean completedTasksOnly) {
        this.completedTasksOnly = completedTasksOnly;

        update(tasks, orderedPipelineStepNames);
    }

    private void update(List<PipelineTaskDisplayData> tasks,
        List<String> orderedPipelineStepNames) {
        categorySummariesByNode = new LinkedList<>();
        seenCategories = new ArrayList<>();

        Map<String, List<PipelineTaskDisplayData>> tasksByNode = new HashMap<>();

        // Partition the tasks by node.
        for (PipelineTaskDisplayData task : tasks) {
            if (!completedTasksOnly || task.getProcessingStep() == ProcessingStep.COMPLETE) {
                String pipelineStepName = task.getPipelineStepName();

                List<PipelineTaskDisplayData> tasksForNode = tasksByNode.get(pipelineStepName);
                if (tasksForNode == null) {
                    tasksForNode = new LinkedList<>();
                    tasksByNode.put(pipelineStepName, tasksForNode);
                }
                tasksForNode.add(task);
            }
        }

        // For each node, aggregate the summary metrics by category
        // and build a list of categories.
        for (String pipelineStepName : orderedPipelineStepNames) {
            List<PipelineTaskDisplayData> tasksForNode = tasksByNode.get(pipelineStepName);
            TaskMetrics taskMetrics = new TaskMetrics(tasksForNode);
            taskMetrics.calculate();
            categorySummariesByNode.add(new PipelineStepTaskMetrics(pipelineStepName, taskMetrics));

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
            return "Node";
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
        return categorySummariesByNode.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineStepTaskMetrics row = categorySummariesByNode.get(rowIndex);

        if (columnIndex == 0) {
            return row.getPipelineStepName();
        }
        if (columnIndex == 1) {
            return formatDuration(row.getTaskMetrics().getTotalProcessingTimeMillis());
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

    public static class PipelineStepTaskMetrics {

        private final String pipelineStepName;
        private final TaskMetrics taskMetrics;

        public PipelineStepTaskMetrics(String pipelineStepName, TaskMetrics taskMetrics) {
            this.pipelineStepName = pipelineStepName;
            this.taskMetrics = taskMetrics;
        }

        public String getPipelineStepName() {
            return pipelineStepName;
        }

        public TaskMetrics getTaskMetrics() {
            return taskMetrics;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pipelineStepName, taskMetrics);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PipelineStepTaskMetrics other = (PipelineStepTaskMetrics) obj;
            return Objects.equals(pipelineStepName, other.pipelineStepName)
                && Objects.equals(taskMetrics, other.taskMetrics);
        }
    }
}
