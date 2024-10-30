package gov.nasa.ziggy.ui.util;

import java.awt.Window;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.swing.SwingWorker;

import org.apache.commons.collections4.CollectionUtils;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.messages.RestartTasksRequest;
import gov.nasa.ziggy.services.messages.StartMemdroneRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.instances.RestartDialog;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Used to restart tasks.
 *
 * @author PT
 * @author Bill Wohler
 */
public class TaskRestarter {

    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private final PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
    private final PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    /**
     * Restarts stopped tasks with diagnostics going to stdout.
     *
     * @param pipelineInstance the pipeline instance containing the stopped tasks
     * @param tasksToRestart the list of tasks to restart; may be null or empty to restart all
     * stopped tasks in the instance
     * @param runMode the run mode to use when starting the tasks; if null, the user is prompted for
     * the mode using the non-null {@code owner}
     * @param messageSentLatch if non-null, the latch is decremented twice after the required
     * messages are published.
     */
    public void restartTasks(PipelineInstance pipelineInstance,
        Collection<PipelineTask> tasksToRestart, RunMode runMode, CountDownLatch messageSentLatch) {

        if (pipelineInstance == null) {
            return;
        }

        restartTasks(null, runMode, messageSentLatch,
            restartableTasksInInstance(pipelineInstance, tasksToRestart));
    }

    /**
     * Restarts all stopped tasks in the given instance.
     *
     * @param owner If non-null, the window for attaching dialogs; otherwise any warnings will
     * appear on stdout
     * @param pipelineInstance the pipeline instance containing the stopped tasks
     */
    public void restartTasks(Window owner, PipelineInstance pipelineInstance) {
        restartTasks(owner, pipelineInstance, null, null, null);
    }

    /**
     * Restarts stopped tasks.
     *
     * @param owner If non-null, the window for attaching dialogs; otherwise any warnings will
     * appear on stdout
     * @param pipelineInstance the pipeline instance containing the stopped tasks
     * @param tasksToRestart the list of tasks to restart; may be null or empty to restart all
     * stopped tasks in the instance
     */
    public void restartTasks(Window owner, PipelineInstance pipelineInstance,
        Collection<PipelineTask> tasksToRestart) {
        restartTasks(owner, pipelineInstance, tasksToRestart, null, null);
    }

    /**
     * Restarts stopped tasks.
     *
     * @param owner If non-null, the window for attaching dialogs; otherwise any warnings will
     * appear on stdout
     * @param pipelineInstance the pipeline instance containing the stopped tasks
     * @param tasksToRestart the list of tasks to restart; may be null or empty to restart all
     * stopped tasks in the instance
     * @param runMode the run mode to use when starting the tasks; if null and {@code owner} is
     * non-null, the user is prompted for the mode; otherwise,
     * {@link RunMode#RESTART_FROM_BEGINNING} is used
     * @param messageSentLatch if non-null, the latch is decremented twice after the required
     * messages are published.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void restartTasks(Window owner, PipelineInstance pipelineInstance,
        Collection<PipelineTask> tasksToRestart, RunMode runMode, CountDownLatch messageSentLatch) {

        if (pipelineInstance == null) {
            return;
        }

        new SwingWorker<Map<PipelineTaskDisplayData, List<RunMode>>, Void>() {
            @Override
            protected Map<PipelineTaskDisplayData, List<RunMode>> doInBackground()
                throws Exception {
                return restartableTasksInInstance(pipelineInstance, tasksToRestart);
            }

            @Override
            protected void done() {
                try {
                    restartTasks(owner, runMode, messageSentLatch, get());
                } catch (InterruptedException | ExecutionException e) {
                    MessageUtils.showError(owner, "Failed to restart tasks; see console log", e);
                }
            }
        }.execute();
    }

    private Map<PipelineTaskDisplayData, List<RunMode>> restartableTasksInInstance(
        PipelineInstance pipelineInstance, Collection<PipelineTask> tasksToRestart) {

        // Collect all of the instance's tasks.
        List<PipelineTask> allTasksInSelectedInstance = pipelineTaskOperations()
            .pipelineTasks(pipelineInstance);

        // Check for selected tasks in list of all tasks in instance.
        List<PipelineTask> tasksInSelectedInstance = allTasksInSelectedInstance;
        if (!CollectionUtils.isEmpty(tasksToRestart)) {
            tasksInSelectedInstance = allTasksInSelectedInstance.stream()
                .filter(task -> tasksToRestart.contains(task))
                .collect(Collectors.toList());
        }

        // Filter the tasks to keep only the ones that are in the correct state for the
        // selected restart mode.
        List<PipelineTaskDisplayData> pipelineTaskDisplayData = pipelineTaskDisplayDataOperations()
            .pipelineTaskDisplayData(tasksInSelectedInstance);
        List<PipelineTaskDisplayData> failedTasks = new ArrayList<>();
        for (PipelineTaskDisplayData task : pipelineTaskDisplayData) {
            if (checkTaskState(task)) {
                failedTasks.add(task);
            }
        }
        if (failedTasks.isEmpty()) {
            return null;
        }

        Map<PipelineTask, List<RunMode>> supportedRunModesByPipelineTask = pipelineTaskOperations()
            .supportedRunModesByPipelineTask(
                failedTasks.stream().map(PipelineTaskDisplayData::getPipelineTask).toList());
        return failedTasks.stream()
            .collect(Collectors.toMap(x -> x,
                x -> supportedRunModesByPipelineTask.get(x.getPipelineTask())));
    }

    /**
     * Tests whether a given {@link PipelineTask} is in an appropriate state for rerunning. At the
     * moment this is a pretty simple method, but once we add reruns of completed tasks it will get
     * more useful.
     */
    private boolean checkTaskState(PipelineTaskDisplayData task) {
        return task.isError() || task.getProcessingStep() == ProcessingStep.COMPLETE
            && task.getFailedSubtaskCount() > 0;
    }

    private void restartTasks(Window owner, RunMode runMode, CountDownLatch messageSentLatch,
        Map<PipelineTaskDisplayData, List<RunMode>> supportedRunModesByPipelineTaskDisplayData) {

        if (supportedRunModesByPipelineTaskDisplayData == null) {
            MessageUtils.showError(owner, "No restart-ready tasks found!", null);
            return;
        }

        RunMode restartMode = runMode != null ? runMode
            : owner != null
                ? RestartDialog.restartTasks(owner, supportedRunModesByPipelineTaskDisplayData)
                : RunMode.RESTART_FROM_BEGINNING;
        if (restartMode == null) {
            return;
        }

        List<PipelineTask> failedTasks = supportedRunModesByPipelineTaskDisplayData.keySet()
            .stream()
            .map(
                (Function<? super PipelineTaskDisplayData, ? extends PipelineTask>) PipelineTaskDisplayData::getPipelineTask)
            .collect(Collectors.toList());
        for (PipelineTask pipelineTask : failedTasks) {
            pipelineTaskDataOperations().setHaltRequested(pipelineTask, false);
        }
        ZiggyMessenger.publish(new RestartTasksRequest(failedTasks, false, restartMode),
            messageSentLatch);
        ZiggyMessenger.publish(new StartMemdroneRequest(failedTasks.get(0).getModuleName(),
            failedTasks.get(0).getPipelineInstanceId()), messageSentLatch);
    }

    private PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    private PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }
}
