package gov.nasa.ziggy.ui.instances;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.services.messages.TaskCompletionNotification;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.status.Indicator;
import gov.nasa.ziggy.ui.status.StatusPanel;
import gov.nasa.ziggy.ui.util.proxy.PipelineOperationsProxy;

/**
 * Performs and manages the automatic refresh of the Instances panel of the console. The class uses
 * a ScheduledThreadPoolExecutor with 1 thread to perform the main action of waiting for specified
 * intervals and then performing the refresh.
 *
 * @author PT
 */
public class InstancesTasksPanelAutoRefresh implements Runnable {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(InstancesTasksPanelAutoRefresh.class);

    public static final long REFRESH_INTERVAL_MILLIS = 2500L;
    private static final String WARNING_MESSAGE = "One or more tasks failed but execution continues";
    private static final String ERROR_MESSAGE = "One or more tasks failed, execution halted";

    private ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(1);
    private final InstancesTable instancesTable;
    private final TasksTableModel tasksTableModel;
    private final TaskStatusSummaryPanel taskStatusSummaryPanel;

    // Used to ensure that when an instance completes, a message is sent to the
    // worker looking for queued instances; once that message is sent, it's not sent
    // again until after the next instance completes.
    private boolean instancesCheckMessageSent = false;

    public InstancesTasksPanelAutoRefresh(InstancesTable instancesTable,
        TasksTableModel tasksTableModel, TaskStatusSummaryPanel taskStatusSummaryPanel) {
        this.instancesTable = instancesTable;
        this.tasksTableModel = tasksTableModel;
        this.taskStatusSummaryPanel = taskStatusSummaryPanel;

        // Whenever a worker status message comes through, update the display.
        ZiggyMessenger.subscribe(TaskCompletionNotification.class, message -> {
            updatePanel();
        });
    }

    @Override
    public void run() {
        Thread.currentThread().setName("OpsInstancePanelAutoRefresh");
        updatePanel();
    }

    /**
     * Asks the instance and task tables to update themselves. Updates the instance status light and
     * task scoreboard in the event dispatch thread.
     */
    public void updatePanel() {
        instancesTable.loadFromDatabase();
        tasksTableModel.loadFromDatabase();
        SwingUtilities.invokeLater(() -> {
            setInstancesStatusLight(instancesTable.getStateOfInstanceWithMaxid());
            taskStatusSummaryPanel.update(tasksTableModel);
        });
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

        boolean instancesRemaining = InstancesTasksPanel.getInstancesRemaining();
        Indicator instancesIndicator = StatusPanel.ContentItem.PIPELINES.menuItem();
        switch (instanceState) {
            case COMPLETED:
                if (instancesRemaining) {
                    instancesIndicator.setState(Indicator.State.NORMAL);
                    if (!instancesCheckMessageSent) {
                        new PipelineOperationsProxy().sendRunningPipelinesCheckRequestMessage();
                        instancesCheckMessageSent = true;
                    }
                } else {
                    instancesIndicator.setState(Indicator.State.IDLE);
                }
                break;
            case INITIALIZED:
            case PROCESSING:
                instancesIndicator.setState(Indicator.State.NORMAL);
                InstancesTasksPanel.setInstancesRemaining();
                instancesCheckMessageSent = false;
                break;
            case ERRORS_RUNNING:
                instancesIndicator.setState(Indicator.State.WARNING, WARNING_MESSAGE);
                break;
            case ERRORS_STALLED:
            case STOPPED:
                instancesIndicator.setState(Indicator.State.ERROR, ERROR_MESSAGE);
                break;
            default:
                throw new IllegalStateException(
                    "Unsupported pipeline instance state " + instanceState.toString());
        }
    }
}
