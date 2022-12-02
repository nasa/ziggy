package gov.nasa.ziggy.services.messages;

import java.io.Serializable;
import java.util.Date;

import gov.nasa.ziggy.services.messaging.MessageHandler;

/**
 * Superclass for all classes that are sent over RMI.
 *
 * @author Todd Klaus
 */
public abstract class PipelineMessage implements Serializable {
    private static final long serialVersionUID = 20210318L;

    private Date timeSent = new Date();

    public PipelineMessage() {
    }

    /**
     * @return the timeSent
     */
    public Date getTimeSent() {
        return timeSent;
    }

    /**
     * @param timeSent the timeSent to set
     */
    public void setTimeSent(Date timeSent) {
        this.timeSent = timeSent;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PipelineMessage)) {
            return false;
        }

        PipelineMessage other = (PipelineMessage) o;
        return timeSent.equals(other.timeSent);
    }

    @Override
    public int hashCode() {
        return timeSent.hashCode();
    }

    @Override
    public String toString() {
        return "PipelineMessage, timeSent :" + timeSent.toString();
    }

    public abstract Object handleMessage(MessageHandler messageHandler);

}
