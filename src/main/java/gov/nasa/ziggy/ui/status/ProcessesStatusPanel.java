package gov.nasa.ziggy.ui.status;

import javax.swing.GroupLayout;
import javax.swing.JPanel;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messages.WorkerResources;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiServer;

/**
 * Displays the status of the Ziggy processes.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ProcessesStatusPanel extends JPanel {

    private static final long serialVersionUID = 20230822L;
    private static Logger log = LoggerFactory.getLogger(ProcessesStatusPanel.class);

    private static ProcessesStatusPanel instance;

    private Indicator supervisorIndicator;
    private Indicator databaseIndicator;
    private Indicator messagingIndicator;

    public ProcessesStatusPanel() {
        buildComponent();

        ZiggyMessenger.subscribe(WorkerResources.class, this::addWorkerDataComponents);
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

    private static boolean monitoringDatabase() {
        return ZiggyConfiguration.getInstance()
            .getString(PropertyName.DATABASE_SOFTWARE.property(), null) != null;
    }

    public void addWorkerDataComponents(WorkerResources workerResources) {
        log.info("Resource values returned: threads {}, heap size {} MB",
            workerResources.getMaxWorkerCount(), workerResources.getHeapSizeMb());
        supervisorIndicator().addDataComponent(
            new LabelValue("Workers", Integer.toString(workerResources.getMaxWorkerCount())));
        supervisorIndicator().addDataComponent(
            new LabelValue("Worker Heap Size", workerResources.humanReadableHeapSize().toString()));
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
