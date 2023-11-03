package gov.nasa.ziggy.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;

/**
 * Summary statistics for the tasks of a pipeline instance
 *
 * @author Todd Klaus
 */
public class TasksStates {
    public class Summary {
        private int submittedCount = 0;
        private int processingCount = 0;
        private int errorCount = 0;
        private int completedCount = 0;
        private int subTaskTotalCount = 0;
        private int subTaskCompleteCount = 0;
        private int subTaskFailedCount = 0;

        public int getSubmittedCount() {
            return submittedCount;
        }

        public int getProcessingCount() {
            return processingCount;
        }

        public int getErrorCount() {
            return errorCount;
        }

        public int getCompletedCount() {
            return completedCount;
        }

        public int getSubTaskTotalCount() {
            return subTaskTotalCount;
        }

        public int getSubTaskCompleteCount() {
            return subTaskCompleteCount;
        }

        public int getSubTaskFailedCount() {
            return subTaskFailedCount;
        }
    }

    private int totalSubmittedCount = 0;
    private int totalProcessingCount = 0;
    private int totalErrorCount = 0;
    private int totalCompletedCount = 0;
    private int totalSubTaskTotalCount = 0;
    private int totalSubTaskCompleteCount = 0;
    private int totalSubTaskFailedCount = 0;

    private final List<String> moduleNames = new LinkedList<>();
    private final Map<String, Summary> moduleStates = new HashMap<>();

    public TasksStates() {
    }

    public TasksStates(List<PipelineTask> tasks, Map<Long, ProcessingSummary> taskAttrs) {
        update(tasks, taskAttrs);
    }

    public void update(List<PipelineTask> tasks, Map<Long, ProcessingSummary> taskAttrs) {
        clear();

        for (PipelineTask task : tasks) {
            String moduleName = task.getPipelineInstanceNode()
                .getPipelineModuleDefinition()
                .getName();

            Summary s = moduleStates.get(moduleName);
            if (s == null) {
                s = new Summary();
                moduleStates.put(moduleName, s);
                moduleNames.add(moduleName);
            }

            switch (task.getState()) {
                case INITIALIZED:
                    break;
                case SUBMITTED:
                    s.submittedCount++;
                    totalSubmittedCount++;
                    break;
                case PROCESSING:
                    s.processingCount++;
                    totalProcessingCount++;
                    break;
                case ERROR:
                    s.errorCount++;
                    totalErrorCount++;
                    break;
                case COMPLETED:
                case PARTIAL:
                    s.completedCount++;
                    totalCompletedCount++;
                    break;
                default:
                    break;
            }

            ProcessingSummary taskAttributes = taskAttrs.get(task.getId());

            if (taskAttributes != null) {
                totalSubTaskTotalCount += taskAttributes.getTotalSubtaskCount();
                s.subTaskTotalCount += taskAttributes.getTotalSubtaskCount();
                totalSubTaskCompleteCount += taskAttributes.getCompletedSubtaskCount();
                s.subTaskCompleteCount += taskAttributes.getCompletedSubtaskCount();
                totalSubTaskFailedCount += taskAttributes.getFailedSubtaskCount();
                s.subTaskFailedCount += taskAttributes.getFailedSubtaskCount();
            }
        }
    }

    private void clear() {
        moduleNames.clear();
        moduleStates.clear();

        totalSubmittedCount = 0;
        totalProcessingCount = 0;
        totalErrorCount = 0;
        totalCompletedCount = 0;
        totalSubTaskTotalCount = 0;
        totalSubTaskCompleteCount = 0;
        totalSubTaskFailedCount = 0;
    }

    public Map<String, Summary> getModuleStates() {
        return moduleStates;
    }

    public List<String> getModuleNames() {
        return moduleNames;
    }

    public int getTotalSubmittedCount() {
        return totalSubmittedCount;
    }

    public int getTotalProcessingCount() {
        return totalProcessingCount;
    }

    public int getTotalErrorCount() {
        return totalErrorCount;
    }

    public int getTotalCompletedCount() {
        return totalCompletedCount;
    }

    public int getTotalSubTaskTotalCount() {
        return totalSubTaskTotalCount;
    }

    public int getTotalSubTaskCompleteCount() {
        return totalSubTaskCompleteCount;
    }

    public int getTotalSubTaskFailedCount() {
        return totalSubTaskFailedCount;
    }
}
