package gov.nasa.ziggy.services.messaging;

import gov.nasa.ziggy.services.alert.AlertMessage;
import gov.nasa.ziggy.services.messages.WorkerShutdownMessage;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;

/**
 * Performs processing of messages received by the client. This allows a separation between the
 * messages and their handleMessage methods and the client.
 *
 * @author PT
 * @author Bill Wohler
 */
public interface ClientMessageHandler {

    void handleAlert(AlertMessage message);

    void handleWorkerStatusMessage(WorkerStatusMessage message);

    void handleShutdownMessage(WorkerShutdownMessage message);

    void handleNoRunningOrQueuedPipelinesMessage();
}
