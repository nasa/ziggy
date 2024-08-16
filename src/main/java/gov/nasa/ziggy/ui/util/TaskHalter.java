package gov.nasa.ziggy.ui.util;

import java.awt.Window;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.swing.JOptionPane;
import javax.swing.SwingWorker;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.services.messages.KilledTaskMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.instances.InstancesTasksPanel;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.Requestor;

/**
 * Used to halt tasks.
 *
 * @author PT
 * @author Bill Wohler
 */
public class TaskHalter implements Requestor {
    private static final Logger log = LoggerFactory.getLogger(InstancesTasksPanel.class);

    private static final long AWAIT_DURATION_MILLIS = 15000L;

    private final UUID uuid = UUID.randomUUID();

    private CountDownLatch replyMessagesCountdownLatch;
    private Map<Long, PipelineTask> tasksToHalt;

    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    public TaskHalter() {

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

    /**
     * Halts running tasks with diagnostics going to stdout.
     *
     * @param pipelineInstance the pipeline instance containing the running tasks
     * @param taskIdsToHalt the list of tasks to halt; may be null or empty to halt all running
     * tasks in the instance
     */

    public void haltTasks(PipelineInstance pipelineInstance, Collection<Long> taskIdsToHalt) {
        if (pipelineInstance == null) {
            return;
        }

        haltTasks(null, runningTasksInInstance(pipelineInstance, taskIdsToHalt));
    }

    /**
     * Halts all running tasks in the given instance.
     *
     * @param owner If non-null, the window for attaching dialogs; otherwise any warnings will
     * appear on stdout
     * @param pipelineInstance the pipeline instance containing the running tasks
     */
    public void haltTasks(Window owner, PipelineInstance pipelineInstance) {
        haltTasks(owner, pipelineInstance, null);
    }

    /**
     * Halts running tasks.
     *
     * @param owner If non-null, the window for attaching dialogs; otherwise any warnings will
     * appear on stdout
     * @param pipelineInstance the pipeline instance containing the running tasks
     * @param taskIdsToHalt the list of tasks to halt; may be null or empty to halt all running
     * tasks in the instance
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void haltTasks(Window owner, PipelineInstance pipelineInstance,
        Collection<Long> taskIdsToHalt) {

        // Are you sure you want to do this?
        if (pipelineInstance == null
            || owner != null && JOptionPane.showConfirmDialog(owner, "Confirm halt tasks?",
                "Confirm halt", JOptionPane.OK_CANCEL_OPTION) != JOptionPane.OK_OPTION) {
            return;
        }

        new SwingWorker<List<PipelineTask>, Void>() {
            @Override
            protected List<PipelineTask> doInBackground() throws Exception {
                return runningTasksInInstance(pipelineInstance, taskIdsToHalt);
            }

            @Override
            protected void done() {
                try {
                    haltTasks(owner, get());
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Could not obtain task IDs to halt", e);
                }
            }
        }.execute();
    }

    private List<PipelineTask> runningTasksInInstance(PipelineInstance pipelineInstance,
        Collection<Long> taskIdsToHalt) {
        // Collect all the processing pipeline tasks.
        List<PipelineTask> incompleteTasksInSelectedInstance = pipelineTaskOperations()
            .pipelineTasks(pipelineInstance, ProcessingStep.processingSteps());

        // Check for selected tasks in list of running tasks in instance.
        List<PipelineTask> taskIdsInSelectedInstance = incompleteTasksInSelectedInstance;
        if (!CollectionUtils.isEmpty(taskIdsToHalt)) {
            taskIdsInSelectedInstance = incompleteTasksInSelectedInstance.stream()
                .filter(s -> taskIdsToHalt.contains(s.getId()))
                .collect(Collectors.toList());
        }

        return taskIdsInSelectedInstance;
    }

    private void haltTasks(Window owner, List<PipelineTask> runningTasksInInstance) {
        if (CollectionUtils.isEmpty(runningTasksInInstance)) {
            MessageUtils.showError(owner, "No running tasks found!");
            return;
        }

        // Attempt to halt the tasks.
        tasksToHalt = new HashMap<>();
        for (PipelineTask task : runningTasksInInstance) {
            tasksToHalt.put(task.getId(), task);
        }
        replyMessagesCountdownLatch = new CountDownLatch(tasksToHalt.size());
        log.info("Halting tasks {}", new TreeSet<>(tasksToHalt.keySet()));
        ZiggyMessenger.publish(KillTasksRequest.forTaskIds(this, tasksToHalt.keySet()));
        try {
            replyMessagesCountdownLatch.await(AWAIT_DURATION_MILLIS, TimeUnit.MILLISECONDS);
            log.info("Halting tasks...done");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        replyMessagesCountdownLatch = null;

        // If any tasks were not halted within the timeout, update the log, generate
        // alerts, and let the user know.
        if (!tasksToHalt.isEmpty()) {
            for (PipelineTask task : tasksToHalt.values()) {
                String message = "Task " + task.getId() + " not halted after "
                    + AWAIT_DURATION_MILLIS / 1000L + " seconds";
                log.warn(message);
                AlertService.getInstance()
                    .generateAndBroadcastAlert("PI", task.getId(), AlertService.Severity.WARNING,
                        message);
            }
            String message = "The " + (tasksToHalt.size() == 1 ? "task is" : "tasks are")
                + " still running, halting will continue in the background";
            MessageUtils.showError(owner, message);
        }
    }

    @Override
    public Object requestorIdentifier() {
        return uuid;
    }

    private PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }
}
