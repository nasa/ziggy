package gov.nasa.ziggy.services.alert;

import java.io.Serializable;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import gov.nasa.ziggy.services.messages.AlertMessage;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

/**
 * Contains alert data. Shared by {@link Alert} (used to store alert data in the database) and
 * {@link AlertMessage} (used to broadcast alert data).
 *
 * @author Todd Klaus
 */
@Embeddable
public class Alert implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(Alert.class);

    private static final long serialVersionUID = 20230511L;

    private static final int MAX_MESSAGE_LENGTH = 4000;

    private Date timestamp;
    private String sourceComponent = null;
    private long sourceTaskId = -1;
    private String processName = null;
    private String processHost = null;
    private long processId = -1;
    private String severity = Level.ERROR.toString();
    @Column(nullable = true, length = MAX_MESSAGE_LENGTH)
    private String message = null;

    public Alert() {
    }

    public Alert(Date timestamp, String sourceComponent, long sourceTaskId, String processName,
        String processHost, long processId, String message) {
        this.timestamp = timestamp;
        this.sourceComponent = sourceComponent;
        this.sourceTaskId = sourceTaskId;
        this.processName = processName;
        this.processHost = processHost;
        this.processId = processId;
        this.message = message;

        validateMessageLength();
    }

    public Alert(Date timestamp, String sourceComponent, long sourceTaskId, String processName,
        String processHost, long processId, String severity, String message) {
        this.timestamp = timestamp;
        this.sourceComponent = sourceComponent;
        this.sourceTaskId = sourceTaskId;
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
            log.warn("Alert message length (" + message.length() + ") is too long, max = "
                + MAX_MESSAGE_LENGTH + ", truncated");
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

    public long getSourceTaskId() {
        return sourceTaskId;
    }

    public void setSourceTaskId(long sourceTaskId) {
        this.sourceTaskId = sourceTaskId;
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

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(Level severity) {
        this.severity = severity.toString();
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }
}
