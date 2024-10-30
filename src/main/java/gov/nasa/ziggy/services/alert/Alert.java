package gov.nasa.ziggy.services.alert;

import java.io.Serializable;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.messages.AlertMessage;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.ManyToOne;

/**
 * Contains alert data. Shared by {@link Alert} (used to store alert data in the database) and
 * {@link AlertMessage} (used to broadcast alert data).
 *
 * @author Todd Klaus
 */
@Embeddable
public class Alert implements Serializable {

    public enum Severity {
        INFRASTRUCTURE, ERROR, WARNING;
    }

    private static final Logger log = LoggerFactory.getLogger(Alert.class);

    private static final long serialVersionUID = 20240925L;

    private static final int MAX_MESSAGE_LENGTH = 4000;

    private Date timestamp;
    private String sourceComponent;

    /** The source task is null when this object is deserialized. */
    @ManyToOne
    private PipelineTask sourceTask;

    private String processName;
    private String processHost;
    private long processId = -1;

    @Enumerated(EnumType.STRING)
    private Severity severity = Severity.ERROR;

    @Column(nullable = true, length = MAX_MESSAGE_LENGTH)
    private String message;

    // For Hibernate.
    Alert() {
    }

    public Alert(Date timestamp, String sourceComponent, PipelineTask sourceTask,
        String processName, String processHost, long processId, Severity severity, String message) {
        this.timestamp = timestamp;
        this.sourceComponent = sourceComponent;
        this.sourceTask = sourceTask;
        this.processName = processName;
        this.processHost = processHost;
        this.processId = processId;
        this.severity = severity;
        this.message = message;

        validateMessageLength();
    }

    private void validateMessageLength() {
        if (message == null) {
            message = "<Missing>";
            log.warn("Alert message is NULL");
        } else if (message.length() > MAX_MESSAGE_LENGTH) {
            message = message.substring(0, MAX_MESSAGE_LENGTH - 4) + "...";
            log.warn("Alert message length ({}) is too long, max is {}, truncated",
                message.length(), MAX_MESSAGE_LENGTH);
        }
    }

    public long getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getSourceComponent() {
        return sourceComponent;
    }

    public void setSourceComponent(String sourceComponent) {
        this.sourceComponent = sourceComponent;
    }

    public PipelineTask getSourceTask() {
        return sourceTask;
    }

    public void setSourceTask(PipelineTask sourceTask) {
        this.sourceTask = sourceTask;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getProcessHost() {
        return processHost;
    }

    public void setProcessHost(String processHost) {
        this.processHost = processHost;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Severity getSeverity() {
        return severity;
    }

    public void setSeverity(Severity severity) {
        this.severity = severity;
    }
}
