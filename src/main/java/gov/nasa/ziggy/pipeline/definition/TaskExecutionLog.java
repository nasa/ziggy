package gov.nasa.ziggy.pipeline.definition;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.persistence.Embeddable;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;

import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.util.StringUtils;

@Embeddable
public class TaskExecutionLog {
    /** hostname of the worker that processed (or is processing) this step */
    private String workerHost;

    /** worker thread number that processed (or is processing) this step */
    private int workerThread;

    /** Timestamp that processing started on this step */
    private Date startProcessingTime = new Date(0);

    /** Timestamp that processing ended (success or failure) on this step */
    private Date endProcessingTime = new Date(0);

    /** PipelineTask.State at the time this execution iteration started */
    private State initialState = PipelineTask.State.INITIALIZED;

    /** PipelineTask.State at the time this execution iteration ended */
    private State finalState = PipelineTask.State.INITIALIZED;

    /** PipelineTask.ProcessingState at the time this execution iteration started */
    @Enumerated(EnumType.STRING)
    private ProcessingState initialProcessingState = ProcessingState.INITIALIZING;

    /** PipelineTask.ProcessingState at the time this execution iteration ended */
    @Enumerated(EnumType.STRING)
    private ProcessingState finalProcessingState = ProcessingState.INITIALIZING;

    public TaskExecutionLog() {
    }

    public TaskExecutionLog(String workerHost, int workerThread) {
        this.workerHost = workerHost;
        this.workerThread = workerThread;
    }

    public String getWorkerHost() {
        return workerHost;
    }

    public void setWorkerHost(String workerHost) {
        this.workerHost = workerHost;
    }

    public int getWorkerThread() {
        return workerThread;
    }

    public void setWorkerThread(int workerThread) {
        this.workerThread = workerThread;
    }

    public Date getStartProcessingTime() {
        return startProcessingTime;
    }

    public void setStartProcessingTime(Date startProcessingTime) {
        this.startProcessingTime = startProcessingTime;
    }

    public Date getEndProcessingTime() {
        return endProcessingTime;
    }

    public void setEndProcessingTime(Date endProcessingTime) {
        this.endProcessingTime = endProcessingTime;
    }

    public State getInitialState() {
        return initialState;
    }

    public void setInitialState(State initialState) {
        this.initialState = initialState;
    }

    public State getFinalState() {
        return finalState;
    }

    public void setFinalState(State finalState) {
        this.finalState = finalState;
    }

    public ProcessingState getInitialProcessingState() {
        return initialProcessingState;
    }

    public void setInitialProcessingState(ProcessingState initialProcessingState) {
        this.initialProcessingState = initialProcessingState;
    }

    public ProcessingState getFinalProcessingState() {
        return finalProcessingState;
    }

    public void setFinalProcessingState(ProcessingState finalProcessingState) {
        this.finalProcessingState = finalProcessingState;
    }

    @Override
    public String toString() {
        SimpleDateFormat f = new SimpleDateFormat("MMddyy-HH:mm:ss");
        String start = f.format(startProcessingTime);
        String end = f.format(endProcessingTime);

        return "TaskExecutionLog [wh=" + workerHost + ", wt=" + workerThread + ", start=" + start
            + ", end=" + end + ", elapsed="
            + StringUtils.elapsedTime(startProcessingTime, endProcessingTime) + ", Si="
            + initialState + ", Sf=" + finalState + ", PSi=" + initialProcessingState + ", PSf="
            + finalProcessingState + "]";
    }
}
