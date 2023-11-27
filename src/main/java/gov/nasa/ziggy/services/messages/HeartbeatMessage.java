package gov.nasa.ziggy.services.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.process.StatusMessage;
import gov.nasa.ziggy.util.SystemProxy;

/**
 * Provides a signal, with timestamp, from the supervisor to all clients currently in contact with
 * the message server. This signal is repeated at intervals. If the signals stop, the clients can
 * respond by attempting to re-establish contact with the server.
 *
 * @author PT
 */
public class HeartbeatMessage extends StatusMessage {

    private static final long serialVersionUID = 20230513L;

    private static Logger log = LoggerFactory.getLogger(HeartbeatMessage.class);

    /**
     * Nominal interval between heartbeat messages in seconds.
     */
    private final static long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 1000 * 15L;
    private static long heartbeatIntervalMillis;

    // This is protected so that a subclass of this method used for test purposes can
    // set its value.
    protected long heartbeatTimeMillis;

    /**
     * Returns the heartbeat interval in milliseconds. This is nominally the default value but can
     * be overridden to a shorter interval for testing purposes.
     */
    public static synchronized final long heartbeatIntervalMillis() {
        if (heartbeatIntervalMillis <= 0L) {
            heartbeatIntervalMillis = ZiggyConfiguration.getInstance()
                .getLong(PropertyName.HEARTBEAT_INTERVAL.property(),
                    DEFAULT_HEARTBEAT_INTERVAL_MILLIS);
        }
        return heartbeatIntervalMillis;
    }

    public HeartbeatMessage() {
        log.debug("Supervisor heartbeat message generated");
        heartbeatTimeMillis = SystemProxy.currentTimeMillis();
    }

    public long getHeartbeatTimeMillis() {
        return heartbeatTimeMillis;
    }
}
