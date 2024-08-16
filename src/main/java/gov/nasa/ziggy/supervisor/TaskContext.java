package gov.nasa.ziggy.supervisor;

import java.util.Map;

import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * @author Todd Klaus
 */
public class TaskContext {
    public enum TaskState {
        IDLE, PROCESSING
    }

    private Long pipelineInstanceId;
    private Long pipelineTaskId;
    private PipelineModule pipelineModule = null;
    private TaskState state = TaskState.IDLE;
    private String module = "-";
    private String moduleUow = "-";
    private long processingStartTimeMillis = 0;
    private long moduleExecTime = 0L;
    private Map<String, Metric> taskMetrics = null;

    public TaskContext() {
    }

    public void setTask(PipelineTask pipelineTask) {
        pipelineTaskId = pipelineTask.getId();
        pipelineInstanceId = pipelineTask.getPipelineInstanceId();
        module = pipelineTask.getModuleName();
        UnitOfWork uow = pipelineTask.uowTaskInstance();
        moduleUow = uow.briefState();
    }

    public Long getPipelineInstanceId() {
        return pipelineInstanceId;
    }

    public void setPipelineInstanceId(Long pipelineInstanceId) {
        this.pipelineInstanceId = pipelineInstanceId;
    }

    public Long getPipelineTaskId() {
        return pipelineTaskId;
    }

    public void setPipelineTaskId(Long pipelineTaskId) {
        this.pipelineTaskId = pipelineTaskId;
    }

    public PipelineModule getPipelineModule() {
        return pipelineModule;
    }

    public void setPipelineModule(PipelineModule currentPipelineModule) {
        pipelineModule = currentPipelineModule;
    }

    public TaskState getState() {
        return state;
    }

    public void setState(TaskState currentState) {
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

    public long getModuleExecTime() {
        return moduleExecTime;
    }

    public void setModuleExecTime(long moduleExecTime) {
        this.moduleExecTime = moduleExecTime;
    }

    public Map<String, Metric> getTaskMetrics() {
        return taskMetrics;
    }

    public void setTaskMetrics(Map<String, Metric> taskMetrics) {
        this.taskMetrics = taskMetrics;
    }
}
