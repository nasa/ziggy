package gov.nasa.ziggy.ui.mon.master;

import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.process.StatusMessage;

public class WorkerThreadNode extends StatusNode {
    private int threadNumber;
    private String state;
    private String instanceId;
    private String taskId;
    private String module;
    private String moduleUow;
    private long processingStartTime;

    public WorkerThreadNode(WorkerStatusMessage workerStatusMessage) {
        update(workerStatusMessage);
    }

    @Override
    public void update(StatusMessage statusMessage) {
        WorkerStatusMessage workerStatusMessage = (WorkerStatusMessage) statusMessage;

        threadNumber = workerStatusMessage.getThreadNumber();
        state = workerStatusMessage.getState();
        instanceId = workerStatusMessage.getInstanceId();
        taskId = workerStatusMessage.getTaskId();
        module = workerStatusMessage.getModule();
        moduleUow = workerStatusMessage.getModuleUow();
        processingStartTime = workerStatusMessage.getProcessingStartTime();
    }

    @Override
    public String toString() {
        return "Thread #" + threadNumber;
    }

    public int getThreadNumber() {
        return threadNumber;
    }

    public String getState() {
        return state;
    }

    public String getModule() {
        return module;
    }

    public String getModuleUow() {
        return moduleUow;
    }

    public long getProcessingStartTime() {
        return processingStartTime;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getTaskId() {
        return taskId;
    }
}
