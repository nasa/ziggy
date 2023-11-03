package gov.nasa.ziggy.services.messages;

import java.util.Date;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.process.StatusMessage;

/**
 * Informs recipients of the status of a worker process.
 *
 * @author PT
 */
public class WorkerStatusMessage extends StatusMessage implements Comparable<WorkerStatusMessage> {
    private static final long serialVersionUID = 20230522L;

    public static Logger log = LoggerFactory.getLogger(WorkerStatusMessage.class);

    private final int workerNumber;
    private final String state;
    private final String instanceId;
    private final String taskId;
    private final String module;
    private final String moduleUow;
    private final long processingStartTime;
    private final boolean lastMessageFromWorker;

    public WorkerStatusMessage(int workerNumber, String state, String instanceId, String taskId,
        String module, String moduleUow, long processingStartTime, boolean lastMessageFromWorker) {
        this.workerNumber = workerNumber;
        this.state = state;
        this.instanceId = instanceId;
        this.taskId = taskId;
        this.module = module;
        this.moduleUow = moduleUow;
        this.processingStartTime = processingStartTime;
        this.lastMessageFromWorker = lastMessageFromWorker;
    }

    @Override
    public String uniqueKey() {
        return super.uniqueKey() + ":" + workerNumber;
    }

    public String getModule() {
        return module;
    }

    public long getProcessingStartTime() {
        return processingStartTime;
    }

    public String getState() {
        return state;
    }

    public String getModuleUow() {
        return moduleUow;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    @Override
    public String briefStatus() {
        return "WS:" + super.briefStatus() + "(" + workerNumber + "):" + "[" + state + ":"
            + instanceId + "-" + taskId + ":" + module + "{" + moduleUow + "}:since "
            + new Date(processingStartTime) + "]";
    }

    public String getTaskId() {
        return taskId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public boolean isLastMessageFromWorker() {
        return lastMessageFromWorker;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        return prime * result + Objects.hash(taskId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || getClass() != obj.getClass()) {
            return false;
        }
        WorkerStatusMessage other = (WorkerStatusMessage) obj;
        return Objects.equals(taskId, other.taskId);
    }

    @Override
    public int compareTo(WorkerStatusMessage o) {
        return (int) (Long.parseLong(taskId) - Long.parseLong(o.taskId));
    }
}
