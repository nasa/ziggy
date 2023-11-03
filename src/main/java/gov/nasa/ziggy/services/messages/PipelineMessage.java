package gov.nasa.ziggy.services.messages;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import gov.nasa.ziggy.util.os.ProcessUtils;

/**
 * Superclass for all classes that are sent over RMI.
 *
 * @author Todd Klaus
 * @author PT
 */
public abstract class PipelineMessage implements Serializable {

    private static final long serialVersionUID = 20230513L;

    private final Date timeSent = new Date();
    private final long senderProcessId = ProcessUtils.getPid();

    public PipelineMessage() {
    }

    /**
     * @return the timeSent
     */
    public Date getTimeSent() {
        return timeSent;
    }

    public long getSenderProcessId() {
        return senderProcessId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(senderProcessId, timeSent, getClass());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineMessage other = (PipelineMessage) obj;
        return senderProcessId == other.senderProcessId && Objects.equals(timeSent, other.timeSent);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ": timeSent=" + timeSent.toString()
            + ", senderProcessId=" + senderProcessId;
    }
}
