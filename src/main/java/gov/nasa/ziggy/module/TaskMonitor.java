package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmStateFiles.SubtaskState;
import gov.nasa.ziggy.module.AlgorithmStateFiles.SubtaskStateCounts;
import gov.nasa.ziggy.util.io.LockManager;

/**
 * Provides tools to manage a task's state file.
 * <p>
 * The main function of the class is to, upon request, walk through the subtask directories for a
 * given task and count the number of failed and completed subtasks. This information is used in two
 * ways:
 * <ol>
 * <li>It allows the compute nodes to determine whether all subtasks are through with processing,
 * which is important to execution decisions made by {@link ComputeNodeMaster}.
 * <li>It allows the state file's subtask counts to be updated, and if necessary it allows the state
 * to be updated to reflect that errors have occurred.
 * </ol>
 * In addition, once the {@link ComputeNodeMaster} has completed its post-processing, the class
 * allows the state file to be marked as complete.
 * <p>
 *
 * @author PT
 */

public class TaskMonitor {
    private static final Logger log = LoggerFactory.getLogger(TaskMonitor.class);

    private final StateFile stateFile;
    private final File stateFileDir;
    private final File taskDir;
    private final File lockFile;
    private final List<File> subtaskDirectories;

    public TaskMonitor(TaskConfigurationManager inputsHandler, StateFile stateFile,
        File stateFileDir, File taskDir) {
        subtaskDirectories = inputsHandler.allSubTaskDirectories();
        this.stateFile = stateFile;
        this.stateFileDir = stateFileDir;
        this.taskDir = taskDir;
        lockFile = new File(taskDir, StateFile.LOCK_FILE_NAME);
    }

    private SubtaskStateCounts countSubtaskStates() {
        SubtaskStateCounts stateCounts = new SubtaskStateCounts();
        if (subtaskDirectories.isEmpty()) {
            log.warn("No subtask directories found in: " + taskDir);
        }

        for (File subtaskDir : subtaskDirectories) {
            AlgorithmStateFiles currentSubtaskStateFile = new AlgorithmStateFiles(subtaskDir);
            SubtaskState currentSubtaskState = currentSubtaskStateFile.currentSubtaskState();

            if (currentSubtaskState == null) {
                // no algorithm state file exists yet
                continue;
            }
            currentSubtaskState.updateStateCounts(stateCounts);
        }

        return stateCounts;
    }

    /**
     * Determines whether all subtasks have been processed: specifically, this means that all the
     * subtasks are in either the completed or failed states, and none are currently processing or
     * waiting to be processed.
     *
     * @return true if all subtasks have been processed.
     */
    public boolean allSubtasksProcessed() {
        SubtaskStateCounts stateCounts = countSubtaskStates();
        return stateCounts.getCompletedSubtasks() + stateCounts.getFailedSubtasks() == stateFile
            .getNumTotal();
    }

    /**
     * Notifies the caller that the task in question has been marked for deletion.
     * <p>
     * Generally, this indicates that the pipeline worker has set the state in the state file on
     * disk to DELETED, and that the {@link TaskMonitor} has detected this change and applied it to
     * the {@link StateFile} instance in the {@link TaskMonitor}. This allows the caller (which is
     * generally an instance of {@link ComputeNodeMaster}) that processing of this task must end.
     */
    public boolean isDeleted() {
        return stateFile.getState().equals(StateFile.State.DELETED);
    }

    /**
     * Makes a single pass through all of the subtask directories and updates the {@link StateFile}
     * based on the {@link AlgorithmStateFiles}s. This method does not update the status to COMPLETE
     * when all subtasks are done to allow the caller to do any post processing before the state
     * file is updated. The state file should be marked COMPLETE with the markStateFileDone()
     * method.
     */
    public void updateState() throws IOException {
        try {
            LockManager.getWriteLockOrBlock(lockFile);
            StateFile previousStateFile = StateFile
                .newStateFileFromDiskFile(new File(stateFileDir, stateFile.name()), true, true);

            if (subtaskDirectories.isEmpty()) {
                log.warn("No subtask dirs found in: " + taskDir);
            }

            SubtaskStateCounts stateCounts = countSubtaskStates();
            stateFile.setNumComplete(stateCounts.getCompletedSubtasks());
            stateFile.setNumFailed(stateCounts.getFailedSubtasks());
            if (previousStateFile.getState().equals(StateFile.State.DELETED)) {
                stateFile.setState(StateFile.State.DELETED);
            }
            updateStateFile(previousStateFile);
        } finally {
            LockManager.releaseWriteLock(lockFile);
        }

    }

    /**
     * Move the {@link StateFile} into the completed state if all subtasks are complete, or into the
     * failed state if some subtasks failed or were never processed.
     */
    public void markStateFileDone() throws IOException {

        try {
            LockManager.getWriteLockOrBlock(lockFile);

            StateFile previousStateFile = StateFile
                .newStateFileFromDiskFile(new File(stateFileDir, stateFile.name()), true);

            if (stateFile.getNumComplete() + stateFile.getNumFailed() == stateFile.getNumTotal()) {
                log.info("All subtasks complete or errored, marking state file COMPLETE");
            } else {
                // If there is a shortfall, consider the missing sub-tasks failed
                int missing = stateFile.getNumTotal()
                    - (stateFile.getNumComplete() + stateFile.getNumFailed());

                log.info("Missing subtasks, forcing state to FAILED, missing=" + missing);
                stateFile.setNumFailed(stateFile.getNumFailed() + missing);
            }
            stateFile.setState(StateFile.State.COMPLETE);

            updateStateFile(previousStateFile);
        } finally {
            LockManager.releaseWriteLock(lockFile);
        }
    }

    private void updateStateFile(StateFile previousStateFile) {
        if (!previousStateFile.equals(stateFile)) {
            log.info("Updating state: " + previousStateFile + " -> " + stateFile);

            if (!StateFile.updateStateFile(previousStateFile, stateFile, stateFileDir)) {
                log.error("Failed to update state file: " + previousStateFile);
            }
        }
    }
}
