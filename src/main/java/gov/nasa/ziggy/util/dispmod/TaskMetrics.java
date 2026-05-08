package gov.nasa.ziggy.util.dispmod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Unit;

/**
 * Compute the time spent on the specified category for a list of tasks and the percentage of the
 * total time spent on that category.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class TaskMetrics {
    private Map<Metric, ValuePercentile> metricsByCategory;
    private long totalProcessingTimeMillis;

    public TaskMetrics(List<PipelineTaskDisplayData> pipelineTasks) {
        if (pipelineTasks == null) {
            return;
        }
        metricsByCategory = sumMetrics(pipelineTasks);
    }

    private Map<Metric, ValuePercentile> sumMetrics(List<PipelineTaskDisplayData> pipelineTasks) {
        Map<Metric, Long> summedValueByCategory = new HashMap<>();
        totalProcessingTimeMillis = 0;
        long totalSizeBytes = 0;

        for (PipelineTaskDisplayData task : pipelineTasks) {
            totalProcessingTimeMillis += task.getExecutionClock().totalExecutionTime();

            List<PipelineTaskMetric> pipelineTaskMetrics = task.getPipelineTaskMetrics();
            for (PipelineTaskMetric metrics : pipelineTaskMetrics) {
                Metric category = metrics.getMetric();
                if (category.unit() == Unit.BYTES) {
                    totalSizeBytes += metrics.getValue();
                }
                Long summedValue = summedValueByCategory.get(category);
                if (summedValue == null) {
                    summedValueByCategory.put(category, metrics.getValue());
                } else {
                    summedValueByCategory.put(category, summedValue + metrics.getValue());
                }
            }
        }

        Map<Metric, ValuePercentile> metricsByCategory = new HashMap<>();
        for (Metric category : summedValueByCategory.keySet()) {
            long value = summedValueByCategory.get(category);
            long total = switch (category.unit()) {
                case MILLIS -> totalProcessingTimeMillis;
                case BYTES -> totalSizeBytes;
                default -> throw new IllegalStateException("Unknown unit " + category.unit());
            };
            double percent = (double) value / (double) total * 100.0;
            metricsByCategory.put(category, new ValuePercentile(value, percent));
        }

        return metricsByCategory;
    }

    public Map<Metric, ValuePercentile> getMetricsByCategory() {
        return metricsByCategory;
    }

    public long getTotalProcessingTimeMillis() {
        return totalProcessingTimeMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricsByCategory, totalProcessingTimeMillis);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskMetrics other = (TaskMetrics) obj;
        return Objects.equals(metricsByCategory, other.metricsByCategory)
            && totalProcessingTimeMillis == other.totalProcessingTimeMillis;
    }
}
