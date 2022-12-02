package gov.nasa.ziggy.ui.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.alert.AlertMessage;
import gov.nasa.ziggy.services.messages.WorkerShutdownMessage;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messaging.ClientMessageHandler;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.mon.alerts.AlertMessageTableModel;
import gov.nasa.ziggy.ui.mon.master.WorkerStatusPanel;
import gov.nasa.ziggy.ui.ops.instances.OpsInstancesPanel;

/**
 * Performs processing of messages received by the console. This allows a separation between the
 * messages and their handleMessage methods, and the console and its GUI components.
 *
 * @author PT
 */
public class ConsoleMessageDispatcher implements ClientMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(ConsoleMessageDispatcher.class);

    private final AlertMessageTableModel alertMessageTableModel;
    private final WorkerStatusPanel processesStatusPanel;
    private final boolean shutdownEnabled;

    public ConsoleMessageDispatcher(AlertMessageTableModel tableModel,
        WorkerStatusPanel statusPanel, boolean shutdownEnabled) {
        alertMessageTableModel = tableModel;
        processesStatusPanel = statusPanel;
        this.shutdownEnabled = shutdownEnabled;
    }

    // methods that handle message classes

    @Override
    public void handleAlert(AlertMessage message) {
        if (alertMessageTableModel != null) {
            alertMessageTableModel.addAlertMessage(message);
        }
    }

    @Override
    public void handleWorkerStatusMessage(WorkerStatusMessage message) {
        if (processesStatusPanel != null) {
            processesStatusPanel.update(message);
        }
    }

    @Override
    public synchronized void handleShutdownMessage(WorkerShutdownMessage message) {
        log.info("Shutting down due to shutdown signal from worker");
        ZiggyGuiConsole.shutdown();
    }

    @Override
    public void handleNoRunningOrQueuedPipelinesMessage() {
        OpsInstancesPanel.clearInstancesRemaining();
    }
}
