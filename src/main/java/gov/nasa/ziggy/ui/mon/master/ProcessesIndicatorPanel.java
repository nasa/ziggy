package gov.nasa.ziggy.ui.mon.master;

import java.util.concurrent.ExecutionException;

import javax.swing.SwingWorker;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messages.WorkerResourceRequest;
import gov.nasa.ziggy.services.messages.WorkerResourceRequest.WorkerResources;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.services.process.StatusMessage;

public class ProcessesIndicatorPanel extends IndicatorPanel {

    private static final long serialVersionUID = 20220614L;

    private static Logger log = LoggerFactory.getLogger(ProcessesIndicatorPanel.class);

    private static ProcessesIndicatorPanel instance;

    private Indicator workerIndicator;
    private Indicator databaseIndicator;
    private Indicator messagingIndicator;

    public ProcessesIndicatorPanel(Indicator parentIndicator) {
        super(parentIndicator);
        initGUI();
    }

    private void initGUI() {
        Configuration config = ZiggyConfiguration.getInstance();
        workerIndicator = newIndicator(this, "Worker", "Worker is running", null);

        // See addWorkerDataComponents(), below.
        add(workerIndicator);
        messagingIndicator = newIndicator(this, "Messaging", "Messaging status: good", null);
        messagingIndicator.addDataComponent(new LabelValue("Type", "RMI"));
        messagingIndicator.addDataComponent(new LabelValue("Port",
            Integer.toString(config.getInt(MessageHandler.RMI_REGISTRY_PORT_PROP,
                MessageHandler.RMI_REGISTRY_PORT_PROP_DEFAULT))));
        add(messagingIndicator);
        if (config.getString(PropertyNames.DATABASE_SOFTWARE_PROP_NAME, null) != null) {
            databaseIndicator = newIndicator(this, "Database", "Database is running", null);
            databaseIndicator.addDataComponent(
                new LabelValue("Name", config.getString(PropertyNames.DATABASE_NAME_PROP_NAME)));
            databaseIndicator.addDataComponent(new LabelValue("Type",
                config.getString(PropertyNames.DATABASE_SOFTWARE_PROP_NAME)));
            databaseIndicator.addDataComponent(
                new LabelValue("Host", config.getString(PropertyNames.DATABASE_HOST_PROP_NAME)));
            databaseIndicator.addDataComponent(
                new LabelValue("Port", config.getString(PropertyNames.DATABASE_PORT_PROP_NAME)));
            databaseIndicator.addDataComponent(new LabelValue("Connections",
                config.getString(PropertyNames.DATABASE_CONNECTIONS_PROP_NAME)));
            add(databaseIndicator);
        }
        setTitle("Pipeline Processes");
    }

    private Indicator newIndicator(IndicatorPanel parent, String name, String greenStateToolTipText,
        String grayStateToolTipText) {
        Indicator indicator = new Indicator(parent, name);
        indicator.setGreenStateToolTipText(greenStateToolTipText);
        indicator.setGrayStateToolTipText(grayStateToolTipText);
        indicator.setState(Indicator.State.GREEN);
        return indicator;
    }

    private static String humanReadableHeapSize(long heapSize) {
        long heapSizeMBytes = heapSize / 1000000;
        if (heapSizeMBytes <= 1000) {
            return Long.toString(heapSizeMBytes) + " MB";
        }
        if (heapSizeMBytes > 1000000) {
            return Long.toString(heapSizeMBytes / 1000000) + " TB";
        }
        return Long.toString(heapSizeMBytes / 1000) + " GB";
    }

    @Override
    public void dismissAll() {
    }

    @Override
    public void update(StatusMessage statusMessage) {
    }

    private static void initializeSingletonInstance() {
        if (instance == null) {
            instance = new ProcessesIndicatorPanel(MasterStatusPanel.processesIndicator());
        }
    }

    public static ProcessesIndicatorPanel processesIndicatorPanel(Indicator processesIndicator) {
        if (instance == null) {
            instance = new ProcessesIndicatorPanel(processesIndicator);
        }
        return instance;
    }

    // There's an ordering constraint on building the GUI components that I haven't been able
    // to resolve. It goes like this:
    // First, the panels (including this panel) must be instantiated.
    // Then, the UiCommunicator can be constructed.
    // Then, we can use the UiCommunicator to get the threads and heap size.
    // When I try to exchange the order of the first 2 items, the GUI comes up blank, but
    // obviously I can't use the UiCommunicator until I'm done constructing it. Thus the
    // worker indicator needs to be instantiated in step 1 but populated in step 3. Thus
    // the need for this method.
    public static void addWorkerDataComponents() {

        SwingWorker<WorkerResources, Void> swingWorker = new SwingWorker<WorkerResources, Void>() {

            @Override
            protected WorkerResources doInBackground() throws Exception {
                WorkerResources resources = (WorkerResources) UiCommunicator
                    .send(new WorkerResourceRequest());
                log.info("Resource values returned: threads " + resources.getWorkerThreads()
                    + ", heap size " + resources.getHeapSize());
                return resources;
            }

            @Override
            protected void done() {
                try {
                    WorkerResources resources = get();
                    log.info("Resource values returned: threads " + resources.getWorkerThreads()
                        + ", heap size " + resources.getHeapSize());
                    int workerThreads = resources.getWorkerThreads();
                    long heapSize = resources.getHeapSize();
                    workerIndicator().addDataComponent(
                        new LabelValue("Threads", Integer.toString(workerThreads)));
                    workerIndicator().addDataComponent(
                        new LabelValue("Heap Size", humanReadableHeapSize(heapSize)));
                } catch (InterruptedException | ExecutionException e) {
                    throw new PipelineException("Unable to obtain worker resources", e);
                }
            }
        };
        swingWorker.execute();

    }

    public static Indicator workerIndicator() {
        initializeSingletonInstance();
        return instance.workerIndicator;
    }

    public static Indicator databaseIndicator() {
        initializeSingletonInstance();
        return instance.databaseIndicator;
    }

    public static Indicator messagingIndicator() {
        initializeSingletonInstance();
        return instance.messagingIndicator;
    }

}
