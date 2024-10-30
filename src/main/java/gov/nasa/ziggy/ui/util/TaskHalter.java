package gov.nasa.ziggy.ui.util;

import java.awt.Window;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.messages.HaltTasksRequest;
import gov.nasa.ziggy.services.messages.TaskHaltedMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.instances.InstancesTasksPanel;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Used to halt tasks.
 *
 * @author PT
 * @author Bill Wohler
 */
public class TaskHalter {
    private static final Logger log = LoggerFactory.getLogger(InstancesTasksPanel.class);

    private static final long AWAIT_DURATION_MILLIS = 15000L;

    private CountDownLatch replyMessagesCountdownLatch;
    private Set<PipelineTask> tasksToHalt;

    private final PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    public TaskHalter() {

        // Subscribe to TaskHaltedMessage so we can track whether all the tasks that
        // were marked for death, were actually halted.
        ZiggyMessenger.subscribe(TaskHaltedMessage.class, message -> {
            if (tasksToHalt.contains(message.getPipelineTask())) {
                tasksToHalt.remove(message.getPipelineTask());
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
     * @param tasksToHalt the list of tasks to halt; may be null or empty to halt all running tasks
     * in the instance
     */

    public void haltTasks(PipelineInstance pipelineInstance, Collection<PipelineTask> tasksToHalt) {
        if (pipelineInstance == null) {
            return;
        }

        haltTasks((Window) null, runningTasksInInstance(pipelineInstance, tasksToHalt));
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
     * @param tasksToHalt the list of tasks to halt; may be null or empty to halt all running tasks
     * in the instance
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void haltTasks(Window owner, PipelineInstance pipelineInstance,
        Collection<PipelineTask> tasksToHalt) {

        // Are you sure you want to do this?
        if (pipelineInstance == null
            || owner != null && JOptionPane.showConfirmDialog(owner, "Confirm halt tasks?",
                "Confirm halt", JOptionPane.OK_CANCEL_OPTION) != JOptionPane.OK_OPTION) {
            return;
        }

        new SwingWorker<List<PipelineTask>, Void>() {
            @Override
            protected List<PipelineTask> doInBackground() throws Exception {
                return runningTasksInInstance(pipelineInstance, tasksToHalt);
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
        Collection<PipelineTask> tasksToHalt) {
        // Collect all the processing pipeline tasks.
        List<PipelineTask> incompleteTasksInSelectedInstance = pipelineTaskDataOperations()
            .pipelineTasks(pipelineInstance, ProcessingStep.processingSteps());

        // Check for selected tasks in list of running tasks in instance.
        List<PipelineTask> tasksInSelectedInstance = incompleteTasksInSelectedInstance;
        if (!CollectionUtils.isEmpty(tasksToHalt)) {
            tasksInSelectedInstance = incompleteTasksInSelectedInstance.stream()
                .filter(t -> tasksToHalt.contains(t))
                .collect(Collectors.toList());
        }

        return tasksInSelectedInstance;
    }

    private void haltTasks(Window owner, List<PipelineTask> runningTasksInInstance) {
        if (CollectionUtils.isEmpty(runningTasksInInstance)) {
            MessageUtils.showError(owner, "No running tasks found!");
            return;
        }

        // Attempt to halt the tasks.
        tasksToHalt = new TreeSet<>(runningTasksInInstance);
        replyMessagesCountdownLatch = new CountDownLatch(tasksToHalt.size());
        log.info("Halting tasks {}", tasksToHalt);
        for (PipelineTask pipelineTask : tasksToHalt) {
            pipelineTaskDataOperations().setHaltRequested(pipelineTask, true);
        }
        ZiggyMessenger.publish(new HaltTasksRequest(tasksToHalt));
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
            for (PipelineTask pipelineTask : tasksToHalt) {
                String message = "Task " + pipelineTask + " not halted after "
                    + AWAIT_DURATION_MILLIS / 1000L + " seconds";
                log.warn(message);
                AlertService.getInstance()
                    .generateAndBroadcastAlert("PI", pipelineTask, Severity.WARNING, message);
            }
            String message = "The " + (tasksToHalt.size() == 1 ? "task is" : "tasks are")
                + " still running, halting will continue in the background";
            MessageUtils.showError(owner, message);
        }
    }

    private PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }
}
