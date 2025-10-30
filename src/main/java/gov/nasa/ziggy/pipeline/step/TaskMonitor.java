package gov.nasa.ziggy.pipeline.step;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.AlgorithmStateFiles.AlgorithmState;
import gov.nasa.ziggy.pipeline.step.AlgorithmStateFiles.SubtaskStateCounts;
import gov.nasa.ziggy.pipeline.step.TimestampFile.Event;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskUtils;
import gov.nasa.ziggy.services.messages.AllJobsFinishedMessage;
import gov.nasa.ziggy.services.messages.HaltTasksRequest;
import gov.nasa.ziggy.services.messages.TaskProcessingCompleteMessage;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.util.ZiggyUtils;

/**
 * Provides tools to manage a task's state file.
 * <p>
 * The main function of the class is to, upon request, walk through the subtask directories for a
 * given task and count the number of failed and completed subtasks. This allows the subtask counts
 * in the database to be updated.
 * <p>
 * The TaskMonitor also detects whether all subtasks for a task are either completed or failed, and
 * notifies the {@link AlgorithmMonitor} of this fact via an instance of
 * {@link TaskProcessingCompleteMessage}. Conversely, the TaskMonitor also responds to
 * {@link AllJobsFinishedMessage} instances sent from the {@link AlgorithmMonitor} by shutting down
 * the monitoring for the given task and performing a final count of subtask states. The
 * {@link AllJobsFinishedMessage} means that all remote jobs for the task have finished or been
 * deleted, ergo no further processing will occur regardless of how many subtasks have not yet been
 * processed.
 *
 * @author PT
 * @author Bill Wohler
 */

public class TaskMonitor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TaskMonitor.class);

    private static final long PROCESSING_MESSAGE_MAX_WAIT_MILLIS = 5000L;
    private static final long FILE_SYSTEM_LAG_DELAY_MILLIS = 5000L;
    private static final long FILE_SYSTEM_CHECK_INTERVAL_MILLIS = 100L;
    private static final int FILE_SYSTEM_CHECKS_COUNT = (int) (FILE_SYSTEM_LAG_DELAY_MILLIS
        / FILE_SYSTEM_CHECK_INTERVAL_MILLIS);

    private final File taskDir;
    private final List<Path> subtaskDirectories;
    private final ScheduledThreadPoolExecutor monitoringThread = new ScheduledThreadPoolExecutor(1);
    final long pollIntervalMilliseconds;
    final AlgorithmStateFiles taskAlgorithmStateFile;
    private final PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private final PipelineTask pipelineTask;
    private int totalSubtasks;
    private boolean monitoringEnabled = true;
    private boolean finishFileDetected;
    private boolean incompleteSubtasksPossible;
    private boolean waitedForSubtaskUpdates;
    private boolean allSubtasksProcessed;

    public TaskMonitor(PipelineTask pipelineTask, File taskDir, long pollIntervalMilliseconds) {
        subtaskDirectories = SubtaskUtils.subtaskDirectories(taskDir.toPath());
        this.taskDir = taskDir;
        this.pollIntervalMilliseconds = pollIntervalMilliseconds;
        taskAlgorithmStateFile = new AlgorithmStateFiles(taskDir);
        this.pipelineTask = pipelineTask;
    }

    public void startMonitoring() {
        if (pollIntervalMilliseconds > 0) {
            monitoringThread.scheduleWithFixedDelay(this, 0, pollIntervalMilliseconds,
                TimeUnit.MILLISECONDS);
            ZiggyShutdownHook.addShutdownHook(() -> {
                monitoringThread.shutdownNow();
            });
        }

        // If the worker for this task has sent a final message, perform a final
        // update and shut down the monitoring thread.
        ZiggyMessenger.subscribe(WorkerStatusMessage.class, message -> {
            handleWorkerStatusMessage(message);
        });

        // If all remote jobs for this task have exited, perform a final update and
        // shut down the monitoring thread.
        ZiggyMessenger.subscribe(AllJobsFinishedMessage.class, message -> {
            handleAllJobsFinishedMessage(message);
        });

        // If the task has been halted, perform a final update and shut down the
        // monitoring thread.
        ZiggyMessenger.subscribe(HaltTasksRequest.class, message -> {
            handleHaltTasksRequest(message);
        });
    }

    @Override
    public void run() {
        update();
    }

    void handleWorkerStatusMessage(WorkerStatusMessage message) {

        // We only care about the worker message if we're doing local processing; otherwise, the
        // worker's exit is irrelevant because processing has already been handed over to PBS.
        if (message.isLastMessageFromWorker() && message.getPipelineTask().equals(pipelineTask)
            && !pipelineTaskOperations.executionResources(pipelineTask)
                .isRemoteExecutionEnabled()) {
            update(true);
        }
    }

    void handleAllJobsFinishedMessage(AllJobsFinishedMessage message) {
        if (!pipelineTask.equals(message.getPipelineTask())) {
            return;
        }
        incompleteSubtasksPossible = true;
        update(true);
    }

    void handleHaltTasksRequest(HaltTasksRequest request) {
        if (request.getPipelineTasks().contains(pipelineTask)) {
            incompleteSubtasksPossible = true;
            update(true);
        }
    }

    private SubtaskStateCounts countSubtaskStates() {

        // If this is the first time we're counting states, make sure that the total subtask
        // count is set correctly.
        if (totalSubtasks == 0) {
            totalSubtasks = pipelineTaskDataOperations.subtaskCounts(pipelineTask)
                .getTotalSubtaskCount();
        }

        SubtaskStateCounts stateCounts = new SubtaskStateCounts();
        if (subtaskDirectories.isEmpty()) {
            log.warn("No subtask directories found in {}", taskDir);
        }

        for (Path subtaskDir : subtaskDirectories) {
            AlgorithmStateFiles currentSubtaskStateFile = new AlgorithmStateFiles(
                subtaskDir.toFile());
            AlgorithmState currentSubtaskState = currentSubtaskStateFile.currentAlgorithmState();

            if (currentSubtaskState == null) {
                // no algorithm state file exists yet
                continue;
            }
            currentSubtaskState.updateStateCounts(stateCounts);
        }

        return stateCounts;
    }

    public boolean allSubtasksProcessed() {
        return allSubtasksProcessed(countSubtaskStates());
    }

    /**
     * Determines whether all subtasks have been processed: specifically, this means that all the
     * subtasks are in either the completed or failed states, and none are currently processing or
     * waiting to be processed.
     *
     * @return true if all subtasks have been processed.
     */
    public boolean allSubtasksProcessed(SubtaskStateCounts stateCounts) {
        return stateCounts.getCompletedSubtasks()
            + stateCounts.getFailedSubtasks() == totalSubtasks;
    }

    /**
     * Makes a single pass through all of the subtask directories and updates the database based on
     * the {@link AlgorithmStateFiles} instances. If the task has started processing, the update
     * will detect the .PROCESSING file in the task directory and set the task step to EXECUTING.
     */
    public void update() {
        update(false);
    }

    /**
     * Performs a task status update. If argument finalUpdate is true, the {@link TaskMonitor}
     * performs an orderly shutdown of monitoring; this occurs in response to messages received by
     * the task monitor, as all such messages signal to the monitor that processing has ended. The
     * method is synchronized in order to prevent the monitoring loop and the message-induced update
     * from interfering with one another.
     */
    private synchronized void update(boolean finalUpdate) {

        // If we're no longer monitoring, it means we don't need this update.
        if (!monitoringEnabled) {
            return;
        }

        if (subtaskDirectories.isEmpty()) {
            log.warn("No subtask dirs found in {}", taskDir);
        }

        SubtaskStateCounts stateCounts = updateSubtaskStateCounts();

        if (taskAlgorithmStateFile.isProcessing()
            && pipelineTaskDataOperations.processingStep(pipelineTask).isPreExecutionStep()) {
            pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.EXECUTING);
        }

        allSubtasksProcessed = allSubtasksProcessed(stateCounts);

        // If this was a run-of-the-mill update, we're done.
        if (!allSubtasksProcessed && !finalUpdate) {
            return;
        }

        if (finalUpdate) {
            log.debug("Final update for task {}", pipelineTask.getId());
        }
        if (allSubtasksProcessed) {
            log.debug("All subtasks processed for task {}", pipelineTask.getId());
        }

        // If we got this far, then all subsequent calls to this method should return
        // without taking any action.
        monitoringEnabled = false;
        checkForFinishFile();

        if (!allSubtasksProcessed) {
            if (!incompleteSubtasksPossible) {

                // Wait and see if file-system lag has prevented the subtask counts
                // from showing all subtasks processed.
                waitForSubtaskCountUpdates();
            }
        }

        // Regardles of anything else, do one last update of the subtask counts. This time,
        // verify that the database update was performed.
        finalSubtaskUpdate();

        publishTaskProcessingCompleteMessage(allSubtasksProcessed ? new CountDownLatch(1) : null);

        // It is now safe to shut down the monitoring loop.
        shutdown();
    }

    private SubtaskStateCounts updateSubtaskStateCounts() {
        SubtaskStateCounts stateCounts = countSubtaskStates();
        pipelineTaskDataOperations.updateSubtaskCounts(pipelineTask, -1,
            stateCounts.getCompletedSubtasks(), stateCounts.getFailedSubtasks());
        return stateCounts;
    }

    /**
     * Checks for the FINISH timestamp file in the task directory. This is created by the
     * {@link ComputeNodeMaster} when it exits and is needed by
     * {@link AlgorithmPipelineStepExecutor} when it persists results. Because the subtask
     * .COMPLETED files can appear before the compute node FINISH file, and because there are file
     * system lags on network file systems, we need to do a repetitive check for the file rather
     * than just a one-and-done.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void checkForFinishFile() {
        try {
            ZiggyUtils.tryPatiently("Wait for FINISH file for task " + pipelineTask.getId(),
                fileSystemChecksCount(), fileSystemCheckIntervalMillis(), () -> {
                    if (!TimestampFile.exists(taskDir, Event.FINISH)) {
                        throw new Exception();
                    }
                    finishFileDetected = true;
                    return null;
                });
        } catch (PipelineException e) {
            log.error("FINISH file never created in task directory {}", taskDir.toString());
            finishFileDetected = false;
        }
    }

    private void finalSubtaskUpdate() {
        try {
            ZiggyUtils.tryPatiently(
                "Wait for final subtask count update for task " + pipelineTask.getId(),
                fileSystemChecksCount(), fileSystemCheckIntervalMillis(), () -> {
                    SubtaskStateCounts stateCounts = countSubtaskStates();
                    log.debug("Pipeline task {}: {} completed, {} failed", pipelineTask.getId(),
                        stateCounts.getCompletedSubtasks(), stateCounts.getFailedSubtasks());
                    pipelineTaskDataOperations.updateSubtaskCounts(pipelineTask, -1,
                        stateCounts.getCompletedSubtasks(), stateCounts.getFailedSubtasks());
                    SubtaskCounts databaseStateCounts = pipelineTaskDataOperations
                        .subtaskCounts(pipelineTask);
                    log.debug("Pipeline task {} database: {} completed, {} failed",
                        pipelineTask.getId(), databaseStateCounts.getCompletedSubtaskCount(),
                        databaseStateCounts.getFailedSubtaskCount());
                    if (stateCounts.getCompletedSubtasks() != databaseStateCounts
                        .getCompletedSubtaskCount()
                        || stateCounts.getFailedSubtasks() != databaseStateCounts
                            .getFailedSubtaskCount()) {
                        throw new Exception();
                    }
                    return null;
                });
        } catch (PipelineException e) {
            log.error("Subtask count update never accepted by database for task {}",
                pipelineTask.getId());
        }
    }

    /**
     * Wait to see if additional subtasks get their .COMPLETE flags set. This is intended to address
     * file system lags in the creation of the zero-length files that Ziggy relies upon to monitor
     * status.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void waitForSubtaskCountUpdates() {
        waitedForSubtaskUpdates = true;
        try {
            ZiggyUtils.tryPatiently("Wait for all subtasks to report done", fileSystemChecksCount(),
                fileSystemCheckIntervalMillis(), () -> {
                    if (!allSubtasksProcessed()) {
                        throw new Exception();
                    }
                    allSubtasksProcessed = allSubtasksProcessed();
                    return null;
                });
        } catch (PipelineException e) {
            log.error("Incomplete subtasks remain for task {}", taskDir.toString());
        }
    }

    public void shutdown() {
        monitoringThread.shutdown();
    }

    void publishTaskProcessingCompleteMessage(CountDownLatch processingCompleteMessageLatch) {
        ZiggyMessenger.publish(new TaskProcessingCompleteMessage(pipelineTask), false,
            processingCompleteMessageLatch);
        if (processingCompleteMessageLatch != null) {
            try {
                processingCompleteMessageLatch.await(PROCESSING_MESSAGE_MAX_WAIT_MILLIS,
                    TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    List<Path> getSubtaskDirectories() {
        return subtaskDirectories;
    }

    Path getTaskDir() {
        return taskDir.toPath();
    }

    AlgorithmStateFiles getTaskAlgorithmStateFile() {
        return taskAlgorithmStateFile;
    }

    long fileSystemCheckIntervalMillis() {
        return FILE_SYSTEM_CHECK_INTERVAL_MILLIS;
    }

    int fileSystemChecksCount() {
        return FILE_SYSTEM_CHECKS_COUNT;
    }

    boolean isFinishFileDetected() {
        return finishFileDetected;
    }

    void resetFinishFileDetection() {
        finishFileDetected = false;
    }

    void resetMonitoringEnabled() {
        monitoringEnabled = true;
    }

    boolean iswaitForSubtaskCountUpdates() {
        return waitedForSubtaskUpdates;
    }

    void resetWaitedForSubtaskCountUpdates() {
        waitedForSubtaskUpdates = false;
    }

    void resetIncompleteSubtasksPossible() {
        incompleteSubtasksPossible = false;
    }

    boolean isAllSubtasksProcessed() {
        return allSubtasksProcessed;
    }

    void resetAllSubtasksProcessed() {
        allSubtasksProcessed = false;
    }
}
