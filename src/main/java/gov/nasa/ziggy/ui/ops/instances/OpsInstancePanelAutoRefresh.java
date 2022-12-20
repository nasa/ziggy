package gov.nasa.ziggy.ui.ops.instances;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.ui.mon.master.Indicator;
import gov.nasa.ziggy.ui.mon.master.MasterStatusPanel;
import gov.nasa.ziggy.ui.proxy.PipelineOperationsProxy;

/**
 * Performs and manages the automatic refresh of the Ops Instances panel of the console. The class
 * uses a ScheduledThreadPoolExecutor with 1 thread to perform the main action of waiting for
 * specified intervals and then performing the refresh.
 *
 * @author PT
 */
public class OpsInstancePanelAutoRefresh implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OpsInstancePanelAutoRefresh.class);

    private static final long REFRESH_INTERVAL_MILLIS = 2500L;
    private static final String AMBER_MESSAGE = "One or more tasks failed but execution continues";
    private static final String RED_MESSAGE = "One or more tasks failed, execution halted";

    private ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(1);
    private final InstancesTableModel instancesTableModel;
    private final TasksTableModel tasksTableModel;
    private final TaskStatusSummaryPanel taskStatusSummaryPanel;

    // Used to ensure that when an instance completes, a message is sent to the
    // worker looking for queued instances; once that message is sent, it's not sent
    // again until after the next instance completes.
    private boolean instancesCheckMessageSent = false;

    public OpsInstancePanelAutoRefresh(InstancesTableModel instancesTableModel,
        TasksTableModel tasksTableModel, TaskStatusSummaryPanel taskStatusSummaryPanel) {
        this.instancesTableModel = instancesTableModel;
        this.tasksTableModel = tasksTableModel;
        this.taskStatusSummaryPanel = taskStatusSummaryPanel;
    }

    /**
     * Performs the Ops instances panel update. This performs the following steps synchronously:
     * <ol>
     * <li>Refreshes the instances in the instances table model from the database using the
     * CrudProxyExecutor's single thread for its database activity.</li>
     * <li>Refreshes the instances table GUI in the event dispatcher thread using
     * invokeAndWait().</li>
     * <li>Sets the selected instance from the instance table into the tasks table in the event
     * dispatcher thread using invokeAndWait.</li>
     * <li>Refreshes the tasks in the task table model from the database using the
     * CrudProxyExecutor's single thread for its database activity.</li>
     * <li>Refreshes the tasks table GUI in the event dispatcher thread using invokeLater().</li>
     * <li>Updates the selected row in the tasks table in the event dispatcher thread.</li>
     * </ol>
     */
    @Override
    public void run() {

        Thread.currentThread().setName("OpsInstancePanelAutoRefresh");

        // All the rest of the action happens in the SwingWorker
        SwingWorker<Void, Void> swingWorker = new SwingWorker<>() {

            @Override
            protected Void doInBackground() throws Exception {
                updateFromDatabase();
                return null;
            }

            @Override
            protected void done() {
                refreshGui();
            }

        };
        swingWorker.execute();

    }

    /**
     * Starts the thread pool activities by calling scheduleWithFixedDelay() with appropriate
     * arguments.
     */
    public void start() {

        // Send a request to find out whether any pipelines are running or queued. Since we
        // automatically find out about running pipelines when the first refresh fires, this
        // is mainly aimed at finding out about queued instances. But the receiver can't actually
        // tell the difference between queued and running...

        new PipelineOperationsProxy().sendRunningPipelinesCheckRequestMessage();
        pool.scheduleWithFixedDelay(this, 0L, REFRESH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Perform update of the instance and tasks table models from the database. This method is
     * called by the {@link doInBackground} method of the object's {@link AutoRefreshSwingWorker}
     * instance, thus executed in a Swing worker thread.
     */
    private void updateFromDatabase() {
        instancesTableModel.refreshInstancesFromDatabase();
        tasksTableModel.refreshTasksFromDatabase();
    }

    /**
     * Update the GUI, specifically the instances table, the tasks table, and the sub-tasks
     * scoreboard. Called by the {@link done} method of the object's {@link AutoRefreshSwingWorker}
     * instance, thus executed in the event dispatcher thread.
     */
    private void refreshGui() {
        log.debug("Starting GUI auto-refresh");
        instancesTableModel.refreshGui();
        setInstancesStatusLight(instancesTableModel.getStateOfInstanceWithMaxid());
        tasksTableModel.refreshGui();
        taskStatusSummaryPanel.update(tasksTableModel);
        log.debug("Done with GUI auto-refresh");
    }

    /**
     * Shuts down and destroys the thread pool.
     */
    public void stop() {
        pool.shutdownNow();
        pool = null;
    }

    /**
     * Sets the "idiot light" for pipelines based on a PipelineInstance state. A gray indicator
     * indicates that the selected instance was completed; green indicates initialized, processing,
     * or queued; yellow indicates errors running status; red indicates stopped or errors stalled
     * state. When an initialized or processing state is detected, the instancesRemaining member of
     * the OpsInstancesPanel is set.
     *
     * @param instanceState PipelineInstance state to be encoded as the light color.
     */
    private void setInstancesStatusLight(PipelineInstance.State instanceState) {

        boolean instancesRemaining = OpsInstancesPanel.getInstancesRemaining();
        Indicator instancesIndicator = MasterStatusPanel.instancesIndicator();
        switch (instanceState) {
            case COMPLETED:
                if (instancesRemaining) {
                    instancesIndicator.setState(Indicator.State.GREEN);
                    if (!instancesCheckMessageSent) {
                        new PipelineOperationsProxy().sendRunningPipelinesCheckRequestMessage();
                        instancesCheckMessageSent = true;
                    }
                } else {
                    instancesIndicator.setState(Indicator.State.GRAY);
                }
                break;
            case INITIALIZED:
            case PROCESSING:
                instancesIndicator.setState(Indicator.State.GREEN);
                OpsInstancesPanel.setInstancesRemaining();
                instancesCheckMessageSent = false;
                break;
            case ERRORS_RUNNING:
                instancesIndicator.setState(Indicator.State.AMBER, AMBER_MESSAGE);
                break;
            case ERRORS_STALLED:
            case STOPPED:
                instancesIndicator.setState(Indicator.State.RED, RED_MESSAGE);
                break;
            default:
                throw new IllegalStateException(
                    "Unsupported pipeline instance state " + instanceState.toString());
        }
    }

}
