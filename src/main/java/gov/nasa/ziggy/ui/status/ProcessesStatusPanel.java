package gov.nasa.ziggy.ui.status;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messages.HeartbeatCheckMessage;
import gov.nasa.ziggy.services.messages.WorkerResourcesMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiServer;
import gov.nasa.ziggy.ui.ClusterController;
import gov.nasa.ziggy.worker.WorkerResources;

/**
 * Displays the status of the Ziggy processes.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ProcessesStatusPanel extends JPanel {

    private static final long serialVersionUID = 20231126L;
    private static Logger log = LoggerFactory.getLogger(ProcessesStatusPanel.class);

    public static final String RMI_ERROR_MESSAGE = "Unable to establish communication with supervisor";
    public static final String RMI_WARNING_MESSAGE = "Attempting to establish communication with supervisor";
    public static final String SUPERVISOR_ERROR_MESSAGE = "Supervisor process has failed";
    public static final String DATABASE_ERROR_MESSAGE = "Database process has failed";

    private static ProcessesStatusPanel instance;

    private Indicator supervisorIndicator;
    private Indicator databaseIndicator;
    private Indicator messagingIndicator;
    private ClusterController clusterController = new ClusterController(100, 1);

    private LabelValue workerLabel;
    private LabelValue heapSizeLabel;

    public ProcessesStatusPanel() {
        buildComponent();

        ZiggyMessenger.subscribe(WorkerResourcesMessage.class, this::addWorkerDataComponents);
        ZiggyMessenger.subscribe(HeartbeatCheckMessage.class, this::performHeartbeatChecks);
    }

    private void buildComponent() {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();

        supervisorIndicator = createIndicator("Supervisor", "Supervisor is running", null);

        messagingIndicator = createIndicator("Messaging", "Messaging status: good", null);
        messagingIndicator.addDataComponent(new LabelValue("Type", "RMI"));
        messagingIndicator.addDataComponent(new LabelValue("Port", Integer.toString(config
            .getInt(PropertyName.SUPERVISOR_PORT.property(), ZiggyRmiServer.RMI_PORT_DEFAULT))));

        databaseIndicator = createIndicator("Database", "Database is running", null);
        if (monitoringDatabase()) {
            databaseIndicator.addDataComponent(
                new LabelValue("Name", config.getString(PropertyName.DATABASE_NAME.property())));
            databaseIndicator.addDataComponent(new LabelValue("Type",
                config.getString(PropertyName.DATABASE_SOFTWARE.property())));
            databaseIndicator.addDataComponent(
                new LabelValue("Host", config.getString(PropertyName.DATABASE_HOST.property())));
            databaseIndicator.addDataComponent(
                new LabelValue("Port", config.getString(PropertyName.DATABASE_PORT.property())));
            databaseIndicator.addDataComponent(new LabelValue("Connections",
                config.getString(PropertyName.DATABASE_CONNECTIONS.property())));
        } else {
            databaseIndicator.setVisible(false);
        }

        GroupLayout layout = new GroupLayout(this);
        layout.setAutoCreateGaps(true);
        setLayout(layout);

        layout.setHorizontalGroup(layout.createParallelGroup()
            .addComponent(supervisorIndicator, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(messagingIndicator, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(databaseIndicator, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE));

        layout.setVerticalGroup(layout.createSequentialGroup()
            .addComponent(supervisorIndicator, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(messagingIndicator, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(databaseIndicator, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE));
    }

    private Indicator createIndicator(String name, String normalStateToolTipText,
        String idleStateToolTipText) {
        Indicator indicator = new Indicator(name);
        indicator.setNormalStateToolTipText(normalStateToolTipText);
        indicator.setIdleStateToolTipText(idleStateToolTipText);
        indicator.setState(Indicator.State.NORMAL);
        return indicator;
    }

    private void performHeartbeatChecks(HeartbeatCheckMessage message) {

        if (clusterController.isDatabaseAvailable()) {
            databaseIndicator.setState(Indicator.State.NORMAL);
        } else {
            databaseIndicator.setState(Indicator.State.ERROR, DATABASE_ERROR_MESSAGE);
        }
        if (clusterController.isSupervisorRunning()) {
            supervisorIndicator.setState(Indicator.State.NORMAL);
        } else {
            supervisorIndicator.setState(Indicator.State.ERROR, SUPERVISOR_ERROR_MESSAGE);
        }

        if (message.getHeartbeatTime() > 0) {
            log.debug("Setting RMI state to normal");
            messagingIndicator.setState(Indicator.State.NORMAL);
        } else if (message.getHeartbeatTime() == 0) {
            log.warn("Missed supervisor heartbeat message, setting RMI state to warning");
            messagingIndicator.setState(Indicator.State.WARNING, RMI_WARNING_MESSAGE);
        } else {
            log.error("Unable to detect supervisor heartbeat messages");
            messagingIndicator.setState(Indicator.State.ERROR, RMI_ERROR_MESSAGE);
        }
    }

    private static boolean monitoringDatabase() {
        return ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_SOFTWARE.property(), null) != null;
    }

    public void addWorkerDataComponents(WorkerResourcesMessage workerResourcesMessage) {
        if (workerResourcesMessage.getResources() == null) {
            return;
        }
        WorkerResources workerResources = workerResourcesMessage.getResources();
        log.debug("Resource values returned: threads {}, heap size {} MB",
            workerResources.getMaxWorkerCount(), workerResources.getHeapSizeMb());
        SwingUtilities.invokeLater(() -> {
            if (workerLabel != null) {
                supervisorIndicator.removeDataComponent(workerLabel);
            }
            if (heapSizeLabel != null) {
                supervisorIndicator.removeDataComponent(heapSizeLabel);
            }
            workerLabel = new LabelValue("Workers",
                Integer.toString(workerResources.getMaxWorkerCount()));
            heapSizeLabel = new LabelValue("Worker Heap Size",
                workerResources.humanReadableHeapSize().toString());
            supervisorIndicator().addDataComponent(workerLabel);
            supervisorIndicator().addDataComponent(heapSizeLabel);
        });
    }

    public static Indicator supervisorIndicator() {
        return getInstance().supervisorIndicator;
    }

    public static Indicator databaseIndicator() {
        return monitoringDatabase() ? getInstance().databaseIndicator : null;
    }

    public static Indicator messagingIndicator() {
        return getInstance().messagingIndicator;
    }

    public static synchronized ProcessesStatusPanel getInstance() {
        if (instance == null) {
            instance = new ProcessesStatusPanel();
        }
        return instance;
    }
}
