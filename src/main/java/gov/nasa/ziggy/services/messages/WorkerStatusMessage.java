package gov.nasa.ziggy.services.messages;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.process.StatusMessage;

public class WorkerStatusMessage extends StatusMessage {
    private static final long serialVersionUID = 20210310L;

    public static Logger log = LoggerFactory.getLogger(WorkerStatusMessage.class);

    private int threadNumber;
    private String state;
    private String instanceId;
    private String taskId;
    private String module;
    private String moduleUow;
    private long processingStartTime;

    /**
     * @param state
     * @param module
     * @param moduleUow
     * @param processingStartTime
     */
    public WorkerStatusMessage(int threadNumber, String state, String instanceId, String taskId,
        String module, String moduleUow, long processingStartTime) {
        this.threadNumber = threadNumber;
        this.state = state;
        this.instanceId = instanceId;
        this.taskId = taskId;
        this.module = module;
        this.moduleUow = moduleUow;
        this.processingStartTime = processingStartTime;
    }

    public WorkerStatusMessage() {
    }

    @Override
    public String uniqueKey() {
        return super.uniqueKey() + ":" + threadNumber;
    }

    /**
     * @return Returns the module.
     */
    public String getModule() {
        return module;
    }

    /**
     * @param module The module to set.
     */
    public void setModule(String module) {
        this.module = module;
    }

    /**
     * @return Returns the processingStartTime.
     */
    public long getProcessingStartTime() {
        return processingStartTime;
    }

    /**
     * @param processingStartTime The processingStartTime to set.
     */
    public void setProcessingStartTime(long processingStartTime) {
        this.processingStartTime = processingStartTime;
    }

    /**
     * @return Returns the state.
     */
    public String getState() {
        return state;
    }

    /**
     * @param state The state to set.
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * @return the moduleUow
     */
    public String getModuleUow() {
        return moduleUow;
    }

    /**
     * @param moduleUow the moduleUow to set
     */
    public void setModuleUow(String moduleUow) {
        this.moduleUow = moduleUow;
    }

    public int getThreadNumber() {
        return threadNumber;
    }

    public void setThreadNumber(int threadNumber) {
        this.threadNumber = threadNumber;
    }

    @Override
    public String briefStatus() {
        return "WS:" + super.briefStatus() + "(" + threadNumber + "):" + "[" + state + ":"
            + instanceId + "-" + taskId + ":" + module + "{" + moduleUow + "}:since "
            + new Date(processingStartTime) + "]";
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    @Override
    public Object handleMessage(MessageHandler handler) {
        handler.handleWorkerStatusMessage(this);
        return null;
    }
}
