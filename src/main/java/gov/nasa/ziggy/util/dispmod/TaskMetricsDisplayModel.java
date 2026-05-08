package gov.nasa.ziggy.util.dispmod;

import static com.lowagie.text.Element.ALIGN_LEFT;
import static gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric.ALGORITHM_TIME;
import static gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric.INPUTS_SIZE;
import static gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric.MARSHALING_TIME;
import static gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric.OUTPUTS_SIZE;
import static gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric.PERSISTING_TIME;
import static gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric.QUEUE_TIME;

import java.awt.Color;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.jfree.chart.ChartColor;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Unit;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Display a table containing a row for each pipeline node and a column for each category defined in
 * PipelineTask.pipelineTaskMetrics.
 * <p>
 * The cells of the table contain the total time spent on each category for all tasks for the
 * pipeline node and the percentage of the total processing time for all of the tasks.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class TaskMetricsDisplayModel extends DisplayModel {

    public record MetricDisplayInfo(Metric metric, String title, Color color) {
    }

    /**
     * The order that metrics should be presented and metadata used in their display. The metric in
     * the record can be null if only the title is needed.
     */
    public static final List<MetricDisplayInfo> METRIC_DISPLAY_INFO = List.of(
        new MetricDisplayInfo(null, "Node", null),
        new MetricDisplayInfo(INPUTS_SIZE, "Inputs size", ChartColor.DARK_RED),
        new MetricDisplayInfo(MARSHALING_TIME, "Marshalling time", ChartColor.ORANGE),
        new MetricDisplayInfo(QUEUE_TIME, "Queue time", ChartColor.DARK_YELLOW),
        new MetricDisplayInfo(ALGORITHM_TIME, "Algorithm time", ChartColor.DARK_GREEN),
        new MetricDisplayInfo(OUTPUTS_SIZE, "Outputs size", ChartColor.DARK_BLUE),
        new MetricDisplayInfo(PERSISTING_TIME, "Persisting time", ChartColor.DARK_MAGENTA),
        new MetricDisplayInfo(null, "Total time", null));

    private static final int[] COLUMN_ALIGNMENT = { ALIGN_LEFT, ALIGN_LEFT, ALIGN_LEFT, ALIGN_LEFT,
        ALIGN_LEFT, ALIGN_LEFT, ALIGN_LEFT, ALIGN_LEFT };

    private static final long BYTES_PER_GIGABYTE = 1_000_000_000L;

    private List<PipelineStepTaskMetrics> pipelineStepsTaskMetrics;

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

        Map<String, List<PipelineTaskDisplayData>> tasksByNode = new HashMap<>();

        // Partition the tasks by node.
        for (PipelineTaskDisplayData task : tasks) {
            if (!completedTasksOnly || task.getProcessingStep() == ProcessingStep.COMPLETE) {
                String pipelineStepName = task.getPipelineStepName();

                List<PipelineTaskDisplayData> tasksForNode = tasksByNode.get(pipelineStepName);
                if (tasksForNode == null) {
                    tasksForNode = new ArrayList<>();
                    tasksByNode.put(pipelineStepName, tasksForNode);
                }
                tasksForNode.add(task);
            }
        }

        // For each node, aggregate the summary metrics by category
        // and build a list of categories.
        pipelineStepsTaskMetrics = new ArrayList<>();
        for (String pipelineStepName : orderedPipelineStepNames) {
            List<PipelineTaskDisplayData> tasksForNode = tasksByNode.get(pipelineStepName);
            TaskMetrics taskMetrics = new TaskMetrics(tasksForNode);
            pipelineStepsTaskMetrics
                .add(new PipelineStepTaskMetrics(pipelineStepName, taskMetrics));
        }
    }

    @Override
    public int getColumnCount() {
        return METRIC_DISPLAY_INFO.size();
    }

    @Override
    public String getColumnName(int column) {
        MetricDisplayInfo metricDisplayInfo = METRIC_DISPLAY_INFO.get(column);
        String title = metricDisplayInfo.title();
        if (metricDisplayInfo.metric != null && metricDisplayInfo.metric.unit() == Unit.BYTES) {
            title += " (GB)";
        }
        return title;
    }

    @Override
    public int getAlignment(int column) {
        return COLUMN_ALIGNMENT[column];
    }

    @Override
    public int getRowCount() {
        return pipelineStepsTaskMetrics.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineStepTaskMetrics pipelineStepTaskMetrics = pipelineStepsTaskMetrics.get(rowIndex);
        TaskMetrics taskMetrics = pipelineStepTaskMetrics.getTaskMetrics();

        if (METRIC_DISPLAY_INFO.get(columnIndex).metric() != null) {
            return value(METRIC_DISPLAY_INFO.get(columnIndex).metric(), taskMetrics);
        }
        if (columnIndex == 0) {
            return pipelineStepTaskMetrics.getPipelineStepName();
        }
        if (columnIndex == getColumnCount() - 1) {
            return formatDuration(taskMetrics.getTotalProcessingTimeMillis());
        }
        throw new IllegalArgumentException("Unexpected value: " + columnIndex);
    }

    private String value(Metric category, TaskMetrics taskMetrics) {
        ValuePercentile metrics = taskMetrics.getMetricsByCategory().get(category);

        if (metrics == null) {
            return ZiggyStringUtils.NO_DATA;
        }

        return switch (category.unit()) {
            case MILLIS -> String.format("%s (%.1f%%)", formatDuration(metrics.getValue()),
                metrics.getPercent());
            case BYTES -> String.format("%.3f (%.1f%%)",
                (double) metrics.getValue() / (double) BYTES_PER_GIGABYTE, metrics.getPercent());
            default -> throw new IllegalStateException("Unknown unit " + category.unit());
        };
    }

    private String formatDuration(long durationMillis) {
        return durationMillis >= 0 ? DurationFormatUtils.formatDuration(durationMillis, "HHH:mm:ss")
            : ZiggyStringUtils.NO_DATA;
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
