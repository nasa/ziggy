package gov.nasa.ziggy.ui.instances;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.swing.GroupLayout;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import org.apache.commons.collections4.CollectionUtils;

import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.services.messages.NoRunningOrQueuedPipelinesMessage;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.proxy.PipelineExecutorProxy;
import gov.nasa.ziggy.ui.util.proxy.PipelineTaskCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.ProcessingSummaryOpsProxy;

/**
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class InstancesTasksPanel extends javax.swing.JPanel {
    private InstancesPanel instancesPanel;

    // Indices in the tasks table of the selected tasks. Not to be confused with the
    // task IDs of the selected tasks (see below).
    protected List<Integer> selectedTasksIndices = new ArrayList<>();

    private static boolean instancesRemaining = true;

    public InstancesTasksPanel() {

        buildComponent();

        // Subscribe to the NoRunningOrQueuedPipelinesMessage.
        ZiggyMessenger.subscribe(NoRunningOrQueuedPipelinesMessage.class,
            this::clearInstancesRemaining);
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
     * Unified task-killing logic for all-tasks-in-instance or selected-tasks use-cases.
     */
    void killTasks(Collection<Long> taskIdsToKill) {

        try {

            PipelineInstance selectedInstance = instancesPanel.selectedPipelineInstance();
            if (selectedInstance == null) {
                return;
            }

            PipelineTaskCrudProxy taskCrud = new PipelineTaskCrudProxy();

            // collect all the processing pipeline tasks.
            List<PipelineTask> tasksToKill = taskCrud.retrieveAll(selectedInstance,
                PipelineTask.State.PROCESSING);
            tasksToKill
                .addAll(taskCrud.retrieveAll(selectedInstance, PipelineTask.State.SUBMITTED));

            if (!CollectionUtils.isEmpty(taskIdsToKill)) { // selected tasks use-case.
                tasksToKill = tasksToKill.stream()
                    .filter(s -> taskIdsToKill.contains(s.getId()))
                    .collect(Collectors.toList());
            }
            if (CollectionUtils.isEmpty(tasksToKill)) {
                JOptionPane.showMessageDialog(SwingUtilities.getWindowAncestor(this),
                    "No running tasks found!", "Kill tasks", JOptionPane.ERROR_MESSAGE);
                return;
            }

            // are you sure you want to do this?
            int confirm = JOptionPane.showConfirmDialog(SwingUtilities.getWindowAncestor(this),
                "Confirm kill tasks?", "Confirm delete", JOptionPane.OK_CANCEL_OPTION);
            if (confirm == JOptionPane.OK_OPTION) {

                // Delete any local-execution tasks.
                ZiggyMessenger.publish(new KillTasksRequest(tasksToKill));

                // Delete any remote execution tasks.
                QueueCommandManager queueCommandManager = QueueCommandManager.newInstance();
                queueCommandManager.deleteJobsForPipelineTasks(tasksToKill);
            } else {

                // well, never mind then.
                JOptionPane.showMessageDialog(SwingUtilities.getWindowAncestor(this),
                    "Delete tasks canceled", "Delete tasks", JOptionPane.OK_OPTION);
            }
        } catch (Exception e) {
            MessageUtil.showError(SwingUtilities.getWindowAncestor(this), "Failed to kill jobs",
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
}
