package gov.nasa.ziggy.pipeline.definition;

import java.util.Collection;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Result class for a query against PipelineInstanceNode.
 *
 * @author Todd Klaus
 */
public class TaskCounts {
    private final Logger log = LoggerFactory.getLogger(TaskCounts.class);

    private int taskCount;
    private int submittedTaskCount;
    private int completedTaskCount;
    private int failedTaskCount;

    public TaskCounts(int total, int submitted, int completed, int failed) {
        taskCount = total;
        submittedTaskCount = submitted;
        completedTaskCount = completed;
        failedTaskCount = failed;
    }

    public TaskCounts(TaskCounts other) {
        taskCount = other.taskCount;
        submittedTaskCount = other.submittedTaskCount;
        completedTaskCount = other.completedTaskCount;
        failedTaskCount = other.failedTaskCount;
    }

    /**
     * Constructs an instance of {@link TaskCounts} from a collection of {@link PipelineTask}
     * states.
     */
    public static TaskCounts from(Collection<PipelineTask.State> states) {
        int taskCount = states.size();
        int submittedTaskCount = 0;
        int completedTaskCount = 0;
        int failedTaskCount = 0;
        for (PipelineTask.State state : states) {
            switch (state) {
                case INITIALIZED:
                    break;
                case SUBMITTED:
                    submittedTaskCount++;
                    break;
                case PROCESSING:
                    submittedTaskCount++;
                    break;
                case ERROR:
                    submittedTaskCount++;
                    failedTaskCount++;
                    break;
                case COMPLETED:
                    submittedTaskCount++;
                    completedTaskCount++;
                    break;
                case PARTIAL:
                    submittedTaskCount++;
                default:
                    throw new PipelineException("Unsupported task state: " + state.toString());
            }
        }
        return new TaskCounts(taskCount, submittedTaskCount, completedTaskCount, failedTaskCount);
    }

    /**
     * True if numCompletedTasks == numTasks
     *
     * @return
     */
    public boolean isInstanceNodeComplete() {
        log.info("numTasks/numCompletedTasks = " + taskCount + "/" + completedTaskCount);

        return completedTaskCount == taskCount;
    }

    /**
     * True if the sum of completed tasks and failed tasks == total tasks. Indicates that the
     * pipeline has at least attempted to process every task.
     */
    public boolean isInstanceNodeExecutionComplete() {
        log.debug("numTasks/numCompletedTasks/numFailedTasks = {} / {} / {}", taskCount,
            completedTaskCount, failedTaskCount);
        return completedTaskCount + failedTaskCount == taskCount && submittedTaskCount > 0;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    public String log() {
        return "taskCounts: numTasks/numSubmittedTasks/numCompletedTasks/numFailedTasks =  "
            + taskCount + "/" + submittedTaskCount + "/" + completedTaskCount + "/"
            + failedTaskCount;
    }

    public int getTaskCount() {
        return taskCount;
    }

    public int getSubmittedTaskCount() {
        return submittedTaskCount;
    }

    public int getCompletedTaskCount() {
        return completedTaskCount;
    }

    public int getFailedTaskCount() {
        return failedTaskCount;
    }

    public void setTaskCount(int total) {
        taskCount = total;
    }

    public void setSubmittedTaskCount(int submitted) {
        submittedTaskCount = submitted;
    }

    public void setCompletedTaskCount(int completed) {
        completedTaskCount = completed;
    }

    public void setFailedTaskCount(int failed) {
        failedTaskCount = failed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(completedTaskCount, failedTaskCount, submittedTaskCount, taskCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TaskCounts other = (TaskCounts) obj;
        if (!Objects.equals(completedTaskCount, other.completedTaskCount)
            || !Objects.equals(failedTaskCount, other.failedTaskCount)
            || !Objects.equals(submittedTaskCount, other.submittedTaskCount)
            || !Objects.equals(taskCount, other.taskCount)) {
            return false;
        }
        return true;
    }
}
