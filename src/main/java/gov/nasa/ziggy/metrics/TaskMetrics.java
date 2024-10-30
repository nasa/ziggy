package gov.nasa.ziggy.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;

/**
 * Compute the time spent on the specified category for a list of tasks and the percentage of the
 * total time spent on that category
 *
 * @author Todd Klaus
 */
public class TaskMetrics {
    private final Map<String, TimeAndPercentile> categoryMetrics = new HashMap<>();
    private final List<PipelineTaskDisplayData> pipelineTasks;
    private TimeAndPercentile unallocatedTime = null;
    private long totalProcessingTimeMillis;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    public TaskMetrics(List<PipelineTaskDisplayData> taskListForModule) {
        pipelineTasks = taskListForModule;
    }

    public void calculate() {
        totalProcessingTimeMillis = 0;
        Map<String, Long> allocatedTimeByCategory = new HashMap<>();

        if (pipelineTasks != null) {
            for (PipelineTaskDisplayData task : pipelineTasks) {
                totalProcessingTimeMillis += task.getExecutionClock().totalExecutionTime();

                List<PipelineTaskMetric> pipelineTaskMetrics = task.getPipelineTaskMetrics();
                for (PipelineTaskMetric metrics : pipelineTaskMetrics) {
                    String category = metrics.getCategory();
                    Long categoryTimeMillis = allocatedTimeByCategory.get(category);
                    if (categoryTimeMillis == null) {
                        allocatedTimeByCategory.put(category, metrics.getValue());
                    } else {
                        allocatedTimeByCategory.put(category,
                            categoryTimeMillis + metrics.getValue());
                    }
                }
            }
        }

        long unallocatedTimeMillis = totalProcessingTimeMillis;

        for (String category : allocatedTimeByCategory.keySet()) {
            long categoryTimeMillis = allocatedTimeByCategory.get(category);
            double categoryPercent = (double) categoryTimeMillis
                / (double) totalProcessingTimeMillis * 100.0;

            categoryMetrics.put(category,
                new TimeAndPercentile(categoryTimeMillis, categoryPercent));

            unallocatedTimeMillis -= categoryTimeMillis;
        }

        double unallocatedPercent = (double) unallocatedTimeMillis
            / (double) totalProcessingTimeMillis * 100.0;
        unallocatedTime = new TimeAndPercentile(unallocatedTimeMillis, unallocatedPercent);
    }

    public Map<String, TimeAndPercentile> getCategoryMetrics() {
        return categoryMetrics;
    }

    public TimeAndPercentile getUnallocatedTime() {
        return unallocatedTime;
    }

    public long getTotalProcessingTimeMillis() {
        return totalProcessingTimeMillis;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    @Override
    public int hashCode() {
        return Objects.hash(categoryMetrics, totalProcessingTimeMillis, unallocatedTime);
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
        return Objects.equals(categoryMetrics, other.categoryMetrics)
            && totalProcessingTimeMillis == other.totalProcessingTimeMillis
            && Objects.equals(unallocatedTime, other.unallocatedTime);
    }
}
