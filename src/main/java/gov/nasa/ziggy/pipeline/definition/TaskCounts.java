package gov.nasa.ziggy.pipeline.definition;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Summary statistics for a collection of pipeline tasks.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class TaskCounts {

    private final int taskCount;
    private final Map<String, Counts> moduleCounts = new LinkedHashMap<>();
    private final Counts totalCounts = new Counts();
    private List<String> moduleNames;

    public TaskCounts() {
        taskCount = 0;
        moduleNames = new ArrayList<>();
    }

    public TaskCounts(List<PipelineTaskDisplayData> tasks) {
        taskCount = tasks.size();
        for (PipelineTaskDisplayData task : tasks) {
            Counts counts = moduleCounts.get(task.getModuleName());
            if (counts == null) {
                counts = new Counts();
                moduleCounts.put(task.getModuleName(), counts);
            }

            if (!task.isError()) {
                task.getProcessingStep().incrementTaskCount(counts);
            } else {
                counts.failedTaskCount++;
            }

            counts.subtaskCounts.totalSubtaskCount += task.getTotalSubtaskCount();
            counts.subtaskCounts.completedSubtaskCount += task.getCompletedSubtaskCount();
            counts.subtaskCounts.failedSubtaskCount += task.getFailedSubtaskCount();
        }

        for (Counts counts : moduleCounts.values()) {
            totalCounts.waitingToRunTaskCount += counts.getWaitingToRunTaskCount();
            totalCounts.processingTaskCount += counts.getProcessingTaskCount();
            totalCounts.completedTaskCount += counts.getCompletedTaskCount();
            totalCounts.failedTaskCount += counts.getFailedTaskCount();
            totalCounts.subtaskCounts.totalSubtaskCount += counts.getTotalSubtaskCount();
            totalCounts.subtaskCounts.completedSubtaskCount += counts.getCompletedSubtaskCount();
            totalCounts.subtaskCounts.failedSubtaskCount += counts.getFailedSubtaskCount();
        }

        moduleNames = new ArrayList<>(moduleCounts.keySet());
    }

    public static String subtaskCountsLabel(int completeCount, int totalCount, int failedCount) {
        StringBuilder s = new StringBuilder().append(completeCount).append("/").append(totalCount);
        if (failedCount > 0) {
            s.append(" (").append(failedCount).append(")");
        }
        return s.toString();
    }

    /**
     * Returns true if getTaskCount() == getTotalCounts().getCompletedTaskCount().
     */
    public boolean isPipelineTasksComplete() {
        return getTotalCounts().getCompletedTaskCount() == taskCount;
    }

    /**
     * Returns true if getTaskCount() == getTotalCounts().getCompletedTaskCount() +
     * getTotalCounts().getFailedTaskCount() for positive task counts.
     */
    public boolean isPipelineTasksExecutionComplete() {
        return getTotalCounts().getCompletedTaskCount()
            + getTotalCounts().getFailedTaskCount() == taskCount && taskCount > 0;
    }

    public int getTaskCount() {
        return taskCount;
    }

    public List<String> getModuleNames() {
        return moduleNames;
    }

    public Map<String, Counts> getModuleCounts() {
        return moduleCounts;
    }

    public Counts getTotalCounts() {
        return totalCounts;
    }

    @Override
    public int hashCode() {
        return Objects.hash(moduleCounts, moduleNames, taskCount, totalCounts);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskCounts other = (TaskCounts) obj;
        return Objects.equals(moduleCounts, other.moduleCounts)
            && Objects.equals(moduleNames, other.moduleNames) && taskCount == other.taskCount
            && Objects.equals(totalCounts, other.totalCounts);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
            "taskCount={0}, waitingToRunTaskCount={1}, completedTaskCount={2}, failedTaskCount={3}",
            getTaskCount(), getTotalCounts().getWaitingToRunTaskCount(),
            getTotalCounts().getCompletedTaskCount(), getTotalCounts().getFailedTaskCount());
    }

    public static class Counts {
        private int waitingToRunTaskCount;
        private int processingTaskCount;
        private int completedTaskCount;
        private int failedTaskCount;

        private SubtaskCounts subtaskCounts = new SubtaskCounts();

        public String subtaskCountsLabel() {
            return TaskCounts.subtaskCountsLabel(getCompletedSubtaskCount(), getTotalSubtaskCount(),
                getFailedSubtaskCount());
        }

        public int getWaitingToRunTaskCount() {
            return waitingToRunTaskCount;
        }

        public int getProcessingTaskCount() {
            return processingTaskCount;
        }

        public int getCompletedTaskCount() {
            return completedTaskCount;
        }

        public int getFailedTaskCount() {
            return failedTaskCount;
        }

        public int getTotalSubtaskCount() {
            return subtaskCounts.getTotalSubtaskCount();
        }

        public int getCompletedSubtaskCount() {
            return subtaskCounts.getCompletedSubtaskCount();
        }

        public int getFailedSubtaskCount() {
            return subtaskCounts.getFailedSubtaskCount();
        }

        public void incrementInitializingTaskCount() {
            waitingToRunTaskCount++;
        }

        public void incrementWaitingToRunTaskCount() {
            waitingToRunTaskCount++;
        }

        public void incrementMarshallingTaskCount() {
            processingTaskCount++;
        }

        public void incrementSubmittingTaskCount() {
            processingTaskCount++;
        }

        public void incrementQueuedTaskCount() {
            processingTaskCount++;
        }

        public void incrementExecutingTaskCount() {
            processingTaskCount++;
        }

        public void incrementWaitingToStoreTaskCount() {
            processingTaskCount++;
        }

        public void incrementStoringTaskCount() {
            processingTaskCount++;
        }

        public void incrementCompleteTaskCount() {
            completedTaskCount++;
        }

        @Override
        public int hashCode() {
            return Objects.hash(completedTaskCount, failedTaskCount, processingTaskCount,
                subtaskCounts, waitingToRunTaskCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            Counts other = (Counts) obj;
            return completedTaskCount == other.completedTaskCount
                && failedTaskCount == other.failedTaskCount
                && processingTaskCount == other.processingTaskCount
                && Objects.equals(subtaskCounts, other.subtaskCounts)
                && waitingToRunTaskCount == other.waitingToRunTaskCount;
        }
    }

    public static class SubtaskCounts {
        private int totalSubtaskCount;
        private int completedSubtaskCount;
        private int failedSubtaskCount;

        public SubtaskCounts() {
        }

        public SubtaskCounts(int totalSubtaskCount, int completedSubtaskCount,
            int failedSubtaskCount) {
            this.totalSubtaskCount = totalSubtaskCount;
            this.completedSubtaskCount = completedSubtaskCount;
            this.failedSubtaskCount = failedSubtaskCount;
        }

        public int getTotalSubtaskCount() {
            return totalSubtaskCount;
        }

        public int getCompletedSubtaskCount() {
            return completedSubtaskCount;
        }

        public int getFailedSubtaskCount() {
            return failedSubtaskCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(completedSubtaskCount, failedSubtaskCount, totalSubtaskCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SubtaskCounts other = (SubtaskCounts) obj;
            return completedSubtaskCount == other.completedSubtaskCount
                && failedSubtaskCount == other.failedSubtaskCount
                && totalSubtaskCount == other.totalSubtaskCount;
        }
    }
}
