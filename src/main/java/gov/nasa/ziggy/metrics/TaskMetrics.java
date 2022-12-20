package gov.nasa.ziggy.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.util.dispmod.DisplayModel;

/**
 * Compute the time spent on the specified category for a list of tasks and the percentage of the
 * total time spent on that category
 *
 * @author Todd Klaus
 */
public class TaskMetrics {
    // Map<category,Pair<categoryTimeMillis,categoryPercent>>
    private final Map<String, TimeAndPercentile> categoryMetrics = new HashMap<>();
    private TimeAndPercentile unallocatedTime = null;
    private long totalProcessingTimeMillis;

    public TaskMetrics(List<PipelineTask> tasks) {
        totalProcessingTimeMillis = 0;
        // Map<category,categoryTimeMillis>
        Map<String, Long> allocatedTimeByCategory = new HashMap<>();

        if (tasks != null) {
            for (PipelineTask task : tasks) {
                totalProcessingTimeMillis += DisplayModel.getProcessingMillis(
                    task.getStartProcessingTime(), task.getEndProcessingTime());

                List<PipelineTaskMetrics> summaryMetrics = task.getSummaryMetrics();
                for (PipelineTaskMetrics metrics : summaryMetrics) {
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

    @Override
    public int hashCode() {
        return Objects.hash(categoryMetrics, totalProcessingTimeMillis, unallocatedTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TaskMetrics other = (TaskMetrics) obj;
        return Objects.equals(categoryMetrics, other.categoryMetrics)
            && totalProcessingTimeMillis == other.totalProcessingTimeMillis
            && Objects.equals(unallocatedTime, other.unallocatedTime);
    }
}
