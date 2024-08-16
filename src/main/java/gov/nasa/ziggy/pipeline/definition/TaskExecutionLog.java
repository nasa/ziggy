package gov.nasa.ziggy.pipeline.definition;

import java.text.SimpleDateFormat;
import java.util.Date;

import gov.nasa.ziggy.util.ZiggyStringUtils;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

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

    /** ProcessingStep at the time this execution iteration started */
    @Enumerated(EnumType.STRING)
    private ProcessingStep initialProcessingStep = ProcessingStep.INITIALIZING;

    /** ProcessingStep at the time this execution iteration ended */
    @Enumerated(EnumType.STRING)
    private ProcessingStep finalProcessingStep = ProcessingStep.INITIALIZING;

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

    public ProcessingStep getInitialProcessingStep() {
        return initialProcessingStep;
    }

    public void setInitialProcessingStep(ProcessingStep initialProcessingStep) {
        this.initialProcessingStep = initialProcessingStep;
    }

    public ProcessingStep getFinalProcessingStep() {
        return finalProcessingStep;
    }

    public void setFinalProcessingStep(ProcessingStep finalProcessingStep) {
        this.finalProcessingStep = finalProcessingStep;
    }

    @Override
    public String toString() {
        SimpleDateFormat f = new SimpleDateFormat("MMddyy-HH:mm:ss");
        String start = f.format(startProcessingTime);
        String end = f.format(endProcessingTime);

        return "TaskExecutionLog [wh=" + workerHost + ", wt=" + workerThread + ", start=" + start
            + ", end=" + end + ", elapsed="
            + ZiggyStringUtils.elapsedTime(startProcessingTime, endProcessingTime) + ", PSi="
            + initialProcessingStep + ", PSf=" + finalProcessingStep + "]";
    }
}
