package gov.nasa.ziggy.services.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.process.StatusMessage;

/**
 * Provides a signal, with timestamp, from the worker to all clients currently in contact with the
 * worker. This signal is repeated at intervals. If the signals stop, the clients can respond by
 * attempting to re-establish contact with the worker.
 *
 * @author PT
 */
public class WorkerHeartbeatMessage extends StatusMessage {

    private static final long serialVersionUID = 20210218L;

    private static Logger log = LoggerFactory.getLogger(WorkerHeartbeatMessage.class);

    /**
     * Nominal interval between heartbeat messages in seconds.
     */
    private final static long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 1000 * 15L;
    private static long heartbeatIntervalMillis;

    /**
     * Returns the heartbeat interval in milliseconds. This is nominally the default value but can
     * be overridden to a shorter interval for testing purposes.
     */
    public static final long heartbeatIntervalMillis() {
        if (heartbeatIntervalMillis <= 0L) {
            String heartbeatIntervalString = System.getProperty(
                PropertyNames.HEARTBEAT_INTERVAL_PROP_NAME,
                Long.toString(DEFAULT_HEARTBEAT_INTERVAL_MILLIS));
            heartbeatIntervalMillis = Long.parseLong(heartbeatIntervalString);
        }
        return heartbeatIntervalMillis;
    }

    private long heartbeatTimeMillis;

    public WorkerHeartbeatMessage() {
        log.debug("Worker heartbeat message generated");
        heartbeatTimeMillis = System.currentTimeMillis();
    }

    /**
     * Message handler for {@link WorkerHeartbeatMessage}. This sets the timestamp of the current
     * object into the {@link MessageHandler} instance.
     */
    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        messageHandler.setLastHeartbeatTimeMillis(heartbeatTimeMillis);
        return null;
    }

    public long getHeartbeatTimeMillis() {
        return heartbeatTimeMillis;
    }
}
