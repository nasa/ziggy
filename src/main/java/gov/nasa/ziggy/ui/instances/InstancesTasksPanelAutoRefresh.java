package gov.nasa.ziggy.ui.instances;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.InstanceUpdateMessage;

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

    private ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(1);
    private final InstancesTable instancesTable;
    private final TasksTableModel tasksTableModel;
    private final TaskStatusSummaryPanel taskStatusSummaryPanel;

    // Used to ensure that when an instance completes, a message is sent to the
    // worker looking for queued instances; once that message is sent, it's not sent
    // again until after the next instance completes.
    private boolean instancesCheckMessageSent = false;

    private boolean instancesRemaining = true;
    private State priorInstanceState;
    private boolean priorInstancesRemaining;

    public InstancesTasksPanelAutoRefresh(InstancesTable instancesTable,
        TasksTableModel tasksTableModel, TaskStatusSummaryPanel taskStatusSummaryPanel) {
        this.instancesTable = instancesTable;
        this.tasksTableModel = tasksTableModel;
        this.taskStatusSummaryPanel = taskStatusSummaryPanel;

        // Whenever a worker status message comes through, update the display. This will
        // include both the status message that is sent on worker startup and the one sent
        // on worker shutdown.
        ZiggyMessenger.subscribe(WorkerStatusMessage.class, message -> {
            updatePanel();
        });
    }

    @Override
    public void run() {
        Thread.currentThread().setName("OpsInstancePanelAutoRefresh");
        updatePanel();
    }

    /**
     * Asks the instance and task tables to update themselves. Updates task scoreboard in the event
     * dispatch thread.
     */
    private void updatePanel() {
        instancesTable.loadFromDatabase();
        tasksTableModel.loadFromDatabase();
        updateInstanceState(instancesTable.getStateOfInstanceWithMaxid());
        SwingUtilities.invokeLater(() -> {
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
        ZiggyMessenger.publish(new RunningPipelinesCheckRequest());

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
     * Send a message with the new instance state. Also send a request to find out whether any
     * pipelines are running or queued if needed.
     */
    private void updateInstanceState(PipelineInstance.State instanceState) {
        switch (instanceState) {
            case COMPLETED:
                if (instancesRemaining && !instancesCheckMessageSent) {
                    ZiggyMessenger.publish(new RunningPipelinesCheckRequest());
                    instancesCheckMessageSent = true;
                }
                break;
            case INITIALIZED:
            case PROCESSING:
                instancesRemaining = true;
                instancesCheckMessageSent = false;
                break;
            case ERRORS_RUNNING:
            case ERRORS_STALLED:
                break;
            default:
                throw new IllegalStateException(
                    "Unsupported pipeline instance state " + instanceState.toString());
        }

        if (instanceState != priorInstanceState || instancesRemaining != priorInstancesRemaining) {
            ZiggyMessenger.publish(new InstanceUpdateMessage(instanceState, instancesRemaining),
                false);
            priorInstanceState = instanceState;
            priorInstancesRemaining = instancesRemaining;
        }
    }

    public void clearInstancesRemaining() {
        instancesRemaining = false;
    }
}
