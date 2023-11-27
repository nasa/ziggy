package gov.nasa.ziggy.ui.instances;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.swing.GroupLayout;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.services.messages.KilledTaskMessage;
import gov.nasa.ziggy.services.messages.NoRunningOrQueuedPipelinesMessage;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.proxy.PipelineExecutorProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineTaskCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.ProcessingSummaryOpsProxy;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.Requestor;

/**
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class InstancesTasksPanel extends javax.swing.JPanel implements Requestor {

    private static final Logger log = LoggerFactory.getLogger(InstancesTasksPanel.class);
    private static final long AWAIT_DURATION_MILLIS = 5000L;
    private InstancesPanel instancesPanel;
    private final UUID uuid = UUID.randomUUID();

    // Indices in the tasks table of the selected tasks. Not to be confused with the
    // task IDs of the selected tasks (see below).
    protected List<Integer> selectedTasksIndices = new ArrayList<>();
    private CountDownLatch replyMessagesCountdownLatch;
    private Map<Long, PipelineTask> tasksToHalt;

    private static boolean instancesRemaining = true;

    public InstancesTasksPanel() {

        buildComponent();

        // Subscribe to the NoRunningOrQueuedPipelinesMessage.
        ZiggyMessenger.subscribe(NoRunningOrQueuedPipelinesMessage.class,
            this::clearInstancesRemaining);

        // Subscribe to KilledTaskMessage so we can track whether all the tasks that
        // were marked for death, were actually halted.
        ZiggyMessenger.subscribe(KilledTaskMessage.class, message -> {
            if (isDestination(message)) {
                tasksToHalt.remove(message.getTaskId());
                if (replyMessagesCountdownLatch != null) {
                    replyMessagesCountdownLatch.countDown();
                }
            }
        });
    }

    private void buildComponent() {

        try {
            instancesPanel = new InstancesPanel(this);
            TasksPanel tasksPanel = new TasksPanel(this);

            GroupLayout layout = new GroupLayout(this);
            setLayout(layout);

            layout.setHorizontalGroup(layout.createSequentialGroup()
                .addComponent(instancesPanel)
                .addComponent(tasksPanel));

            layout.setVerticalGroup(
                layout.createParallelGroup().addComponent(instancesPanel).addComponent(tasksPanel));

            new InstancesTasksPanelAutoRefresh(instancesPanel.instancesTable(),
                tasksPanel.tasksTableModel(), tasksPanel.taskStatusSummaryPanel()).start();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    /**
     * Unified restart logic for all-tasks-in-instance restart or selected-tasks restart.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    void restartTasks(Collection<Long> tasksToRestart) {

        try {
            PipelineInstance selectedInstance = instancesPanel.selectedPipelineInstance();
            if (selectedInstance == null) {
                return;
            }

            ProcessingSummaryOpsProxy attrOps = new ProcessingSummaryOpsProxy();
            Map<Long, ProcessingSummary> taskAttrs = attrOps
                .retrieveByInstanceId(selectedInstance.getId());

            // Identify the tasks: for the first step, pull in all the tasks for the instance
            // if we are responding to the instance restart menu item, or the selected tasks
            // if we are responding to the tasks restart menu item.
            PipelineTaskCrudProxy taskCrud = new PipelineTaskCrudProxy();
            List<PipelineTask> selectedTasks = taskCrud.retrieveAll(selectedInstance);
            if (!CollectionUtils.isEmpty(tasksToRestart)) { // selected tasks use case
                selectedTasks = selectedTasks.stream()
                    .filter(s -> tasksToRestart.contains(s.getId()))
                    .collect(Collectors.toList());
            }

            // Filter the tasks to keep only the ones that are in the correct state for the
            // selected restart mode.
            List<PipelineTask> failedTasks = new ArrayList<>();
            for (PipelineTask task : selectedTasks) {
                ProcessingSummary attributes = taskAttrs.get(task.getId());
                if (checkTaskState(task, attributes)) {
                    failedTasks.add(task);
                }
            }

            if (failedTasks.isEmpty()) {
                JOptionPane.showMessageDialog(SwingUtilities.getWindowAncestor(this),
                    "No restart-ready tasks found!", "Restart tasks", JOptionPane.ERROR_MESSAGE);
                return;
            }

            RunMode restartMode = RestartDialog.restartTasks(SwingUtilities.getWindowAncestor(this),
                failedTasks, taskAttrs);

            if (restartMode != null) {
                PipelineExecutorProxy pipelineExecutor = new PipelineExecutorProxy();
                pipelineExecutor.restartTasks(failedTasks, restartMode);
            }
        } catch (Exception e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), "Failed to restart tasks",
                e.getMessage(), e);
        }
    }

    /**
     * Tests whether a given {@link PipelineTask} is in an appropriate state for rerunning. At the
     * moment this is a pretty simple method, but once we add reruns of completed tasks it will get
     * more useful.
     */
    private boolean checkTaskState(PipelineTask task, ProcessingSummary attributes) {
        return task.getState() == PipelineTask.State.ERROR
            || attributes.getProcessingState() == ProcessingState.COMPLETE
                && attributes.getFailedSubtaskCount() > 0;
    }

    /**
     * Unified task-halting logic for all-tasks-in-instance or selected-tasks use-cases.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    void haltTasks(Collection<Long> taskIdsToHalt) {

        try {

            PipelineInstance selectedInstance = instancesPanel.selectedPipelineInstance();
            if (selectedInstance == null) {
                return;
            }

            PipelineTaskCrudProxy taskCrud = new PipelineTaskCrudProxy();

            // collect all the processing pipeline tasks.
            List<PipelineTask> tasksInHaltRequest = taskCrud.retrieveAll(selectedInstance,
                PipelineTask.State.PROCESSING);
            tasksInHaltRequest
                .addAll(taskCrud.retrieveAll(selectedInstance, PipelineTask.State.SUBMITTED));

            if (!CollectionUtils.isEmpty(taskIdsToHalt)) { // selected tasks use-case.
                tasksInHaltRequest = tasksInHaltRequest.stream()
                    .filter(s -> taskIdsToHalt.contains(s.getId()))
                    .collect(Collectors.toList());
            }
            if (CollectionUtils.isEmpty(tasksInHaltRequest)) {
                JOptionPane.showMessageDialog(SwingUtilities.getWindowAncestor(this),
                    "No running tasks found!", "Halt tasks", JOptionPane.ERROR_MESSAGE);
                return;
            }

            // are you sure you want to do this?
            int confirm = JOptionPane.showConfirmDialog(SwingUtilities.getWindowAncestor(this),
                "Confirm halt tasks?", "Confirm halt", JOptionPane.OK_CANCEL_OPTION);
            if (confirm == JOptionPane.OK_OPTION) {

                tasksToHalt = new HashMap<>();
                for (PipelineTask task : tasksInHaltRequest) {
                    tasksToHalt.put(task.getId(), task);
                }
                replyMessagesCountdownLatch = new CountDownLatch(tasksToHalt.size());
                // Halt any local-execution tasks. Note that we are using raw types because that's
                // apparently the only way to get from Set<Long> to Collection<Object>.
                ZiggyMessenger.publish(
                    KillTasksRequest.forTaskIds(InstancesTasksPanel.this, tasksToHalt.keySet()));
                replyMessagesCountdownLatch.await(AWAIT_DURATION_MILLIS, TimeUnit.MILLISECONDS);
                replyMessagesCountdownLatch = null;

                // If any tasks were not halted within the timeout, update the log, generate
                // alerts, and pop up a message dialog.
                if (!tasksToHalt.isEmpty()) {
                    for (PipelineTask task : tasksToHalt.values()) {
                        log.error("Failed to halt task {}", task.getId());
                        AlertService.getInstance()
                            .generateAndBroadcastAlert("PI", task.getId(),
                                AlertService.Severity.ERROR, "Failed to halt task " + task.getId());
                    }
                    JOptionPane.showMessageDialog(SwingUtilities.getWindowAncestor(this),
                        "One or more tasks not halted", "Tasks Not Halted", JOptionPane.OK_OPTION);
                }
            } else {

                // well, never mind then.
                JOptionPane.showMessageDialog(SwingUtilities.getWindowAncestor(this),
                    "Halt tasks canceled", "Halt tasks", JOptionPane.OK_OPTION);
            }
        } catch (Exception e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), "Failed to halt jobs",
                e.getMessage(), e);
        }
    }

    public static boolean getInstancesRemaining() {
        return instancesRemaining;
    }

    public static void setInstancesRemaining() {
        instancesRemaining = true;
    }

    private <T extends PipelineMessage> void clearInstancesRemaining(T message) {
        instancesRemaining = false;
    }

    @Override
    public Object requestorIdentifier() {
        return uuid;
    }
}
