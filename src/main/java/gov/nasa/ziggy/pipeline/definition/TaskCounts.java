package gov.nasa.ziggy.pipeline.definition;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Summary statistics for a collection of pipeline tasks.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class TaskCounts {

    public class Counts {
        private int waitingToRunTaskCount;
        private int processingTaskCount;
        private int completedTaskCount;
        private int failedTaskCount;

        private int completedSubtaskCount;
        private int failedSubtaskCount;
        private int totalSubtaskCount;

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

        public int getCompletedSubtaskCount() {
            return completedSubtaskCount;
        }

        public int getFailedSubtaskCount() {
            return failedSubtaskCount;
        }

        public int getTotalSubtaskCount() {
            return totalSubtaskCount;
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
    }

    private final int taskCount;
    private final Map<String, Counts> moduleCounts = new LinkedHashMap<>();
    private final Counts totalCounts = new Counts();
    private List<String> moduleNames;

    public TaskCounts() {
        taskCount = 0;
        moduleNames = new ArrayList<>();
    }

    public TaskCounts(List<PipelineTask> tasks) {
        taskCount = tasks.size();
        for (PipelineTask task : tasks) {
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

            counts.totalSubtaskCount += task.getTotalSubtaskCount();
            counts.completedSubtaskCount += task.getCompletedSubtaskCount();
            counts.failedSubtaskCount += task.getFailedSubtaskCount();
        }

        for (Counts counts : moduleCounts.values()) {
            totalCounts.waitingToRunTaskCount += counts.getWaitingToRunTaskCount();
            totalCounts.processingTaskCount += counts.getProcessingTaskCount();
            totalCounts.completedTaskCount += counts.getCompletedTaskCount();
            totalCounts.failedTaskCount += counts.getFailedTaskCount();
            totalCounts.totalSubtaskCount += counts.getTotalSubtaskCount();
            totalCounts.completedSubtaskCount += counts.getCompletedSubtaskCount();
            totalCounts.failedSubtaskCount += counts.getFailedSubtaskCount();
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
    public String toString() {
        return MessageFormat.format(
            "taskCount={0}, waitingToRunTaskCount={1}, completedTaskCount={2}, failedTaskCount={3}",
            getTaskCount(), getTotalCounts().getWaitingToRunTaskCount(),
            getTotalCounts().getCompletedTaskCount(), getTotalCounts().getFailedTaskCount());
    }
}
