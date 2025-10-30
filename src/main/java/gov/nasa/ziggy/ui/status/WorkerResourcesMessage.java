package gov.nasa.ziggy.ui.status;

import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.worker.WorkerResources;

/**
 * This message communicates an update of the WorkerResources currently in use.
 */
public class WorkerResourcesMessage extends PipelineMessage {

    private static final long serialVersionUID = 20250424L;

    private final WorkerResources workerResources;

    public WorkerResourcesMessage(WorkerResources workerResources) {
        this.workerResources = workerResources;
    }

    public WorkerResources getWorkerResources() {
        return workerResources;
    }
}
