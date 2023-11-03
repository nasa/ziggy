package gov.nasa.ziggy.services.process;

/**
 * Implemented by classes that wish to broadcast status messages via the
 * {@link StatusMessageBroadcaster}
 *
 * @author Todd Klaus
 */
public interface StatusReporter {
    StatusMessage reportCurrentStatus();
}
