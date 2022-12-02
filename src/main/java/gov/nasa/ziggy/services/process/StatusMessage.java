package gov.nasa.ziggy.services.process;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Superclass for all status-related messages (messages sent by the {@link StatusMessageBroadcaster}
 *
 * @author Todd Klaus
 */
public abstract class StatusMessage extends PipelineMessage {
    private static final long serialVersionUID = 20210318L;

    private ProcessInfo sourceProcess;
    private int reportIntervalMillis = 0;

    public StatusMessage() {
    }

    public String uniqueKey() {
        return sourceProcess.getKey();
    }

    public String source() {
        return sourceProcess.toString();
    }

    public String briefStatus() {
        return source() + "@" + getTimeSent().toString();
    }

    /**
     * @return the sourceProcess
     */
    public ProcessInfo getSourceProcess() {
        return sourceProcess;
    }

    /**
     * @param sourceProcess the sourceProcess to set
     */
    public void setSourceProcess(ProcessInfo sourceProcess) {
        this.sourceProcess = sourceProcess;
    }

    public int getReportIntervalMillis() {
        return reportIntervalMillis;
    }

    public void setReportIntervalMillis(int reportIntervalMillis) {
        this.reportIntervalMillis = reportIntervalMillis;
    }
}
