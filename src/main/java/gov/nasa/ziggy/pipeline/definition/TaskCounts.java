package gov.nasa.ziggy.pipeline.definition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Result class for a query against PipelineInstanceNode.
 *
 * @author Todd Klaus
 */
public class TaskCounts {
    private final Logger log = LoggerFactory.getLogger(TaskCounts.class);

    private long total;
    private long submitted;
    private long completed;
    private long failed;

    public TaskCounts(long total, long submitted, long completed, long failed) {
        this.total = total;
        this.submitted = submitted;
        this.completed = completed;
        this.failed = failed;
    }

    public TaskCounts(TaskCounts other) {
        total = other.total;
        submitted = other.submitted;
        completed = other.completed;
        failed = other.failed;
    }

    /**
     * True if numCompletedTasks == numTasks
     *
     * @return
     */
    public boolean isInstanceNodeComplete() {
        log.info("numTasks/numCompletedTasks = " + total + "/" + completed);

        return completed == total;
    }

    public String log() {
        return "taskCounts: numTasks/numSubmittedTasks/numCompletedTasks/numFailedTasks =  " + total
            + "/" + submitted + "/" + completed + "/" + failed;
    }

    public long getTotal() {
        return total;
    }

    public long getSubmitted() {
        return submitted;
    }

    public long getCompleted() {
        return completed;
    }

    public long getFailed() {
        return failed;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public void setSubmitted(long submitted) {
        this.submitted = submitted;
    }

    public void setCompleted(long completed) {
        this.completed = completed;
    }

    public void setFailed(long failed) {
        this.failed = failed;
    }
}
