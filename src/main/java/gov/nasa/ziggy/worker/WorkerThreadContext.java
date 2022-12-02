package gov.nasa.ziggy.worker;

import java.util.Map;

import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;

/**
 * @author Todd Klaus
 */
public class WorkerThreadContext {
    public enum ThreadState {
        IDLE, PROCESSING
    }

    private WorkerTaskRequest request = null;
    private PipelineInstance pipelineInstance = null;
    private PipelineTask pipelineTask = null;
    private PipelineModule pipelineModule = null;
    private ThreadState state = ThreadState.IDLE;
    private String module = "-";
    private String moduleUow = "-";
    private long processingStartTimeMillis = 0;
    private int minMemoryMegaBytes = 0;
    private TaskLog taskLog = null;
    private long moduleExecTime = 0L;
    private Map<String, Metric> threadMetrics = null;

    public WorkerThreadContext() {
    }

    public WorkerTaskRequest getRequest() {
        return request;
    }

    public void setRequest(WorkerTaskRequest currentRequest) {
        request = currentRequest;
    }

    public PipelineInstance getPipelineInstance() {
        return pipelineInstance;
    }

    public void setPipelineInstance(PipelineInstance currentPipelineInstance) {
        pipelineInstance = currentPipelineInstance;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    public void setPipelineTask(PipelineTask currentPipelineTask) {
        pipelineTask = currentPipelineTask;
    }

    public PipelineModule getPipelineModule() {
        return pipelineModule;
    }

    public void setPipelineModule(PipelineModule currentPipelineModule) {
        pipelineModule = currentPipelineModule;
    }

    public ThreadState getState() {
        return state;
    }

    public void setState(ThreadState currentState) {
        state = currentState;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String currentModule) {
        module = currentModule;
    }

    public String getModuleUow() {
        return moduleUow;
    }

    public void setModuleUow(String currentModuleUow) {
        moduleUow = currentModuleUow;
    }

    public long getProcessingStartTimeMillis() {
        return processingStartTimeMillis;
    }

    public void setProcessingStartTimeMillis(long currentProcessingStartTimeMillis) {
        processingStartTimeMillis = currentProcessingStartTimeMillis;
    }

    public int getMinMemoryMegaBytes() {
        return minMemoryMegaBytes;
    }

    public void setMinMemoryMegaBytes(int currentMinMemoryMegaBytes) {
        minMemoryMegaBytes = currentMinMemoryMegaBytes;
    }

    public TaskLog getTaskLog() {
        return taskLog;
    }

    public void setTaskLog(TaskLog taskLog) {
        this.taskLog = taskLog;
    }

    /**
     * @return the moduleExecTime
     */
    public long getModuleExecTime() {
        return moduleExecTime;
    }

    /**
     * @param moduleExecTime the moduleExecTime to set
     */
    public void setModuleExecTime(long moduleExecTime) {
        this.moduleExecTime = moduleExecTime;
    }

    /**
     * @return the threadMetrics
     */
    public Map<String, Metric> getThreadMetrics() {
        return threadMetrics;
    }

    /**
     * @param threadMetrics the threadMetrics to set
     */
    public void setThreadMetrics(Map<String, Metric> threadMetrics) {
        this.threadMetrics = threadMetrics;
    }
}
