package gov.nasa.ziggy.services.messages;

import java.util.Date;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.process.StatusMessage;

/**
 * Informs recipients of the status of a worker process.
 *
 * @author PT
 * @author Bill Wohler
 */
public class WorkerStatusMessage extends StatusMessage implements Comparable<WorkerStatusMessage> {
    private static final long serialVersionUID = 20240909L;

    public static Logger log = LoggerFactory.getLogger(WorkerStatusMessage.class);

    private final int workerNumber;
    private final String state;
    private final String instanceId;
    private final PipelineTask pipelineTask;
    private final String module;
    private final String moduleUow;
    private final long processingStartTime;
    private final boolean lastMessageFromWorker;

    // Cached value of database lookup.

    public WorkerStatusMessage(int workerNumber, String state, String instanceId,
        PipelineTask pipelineTask, String module, String moduleUow, long processingStartTime,
        boolean lastMessageFromWorker) {
        this.workerNumber = workerNumber;
        this.state = state;
        this.instanceId = instanceId;
        this.pipelineTask = pipelineTask;
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
            + instanceId + "-" + pipelineTask.getId() + ":" + module + "{" + moduleUow + "}:since "
            + new Date(processingStartTime) + "]";
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
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
        return prime * result + Objects.hash(pipelineTask);
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
        return Objects.equals(pipelineTask, other.pipelineTask);
    }

    @Override
    public int compareTo(WorkerStatusMessage o) {
        return pipelineTask.compareTo(o.pipelineTask);
    }
}
