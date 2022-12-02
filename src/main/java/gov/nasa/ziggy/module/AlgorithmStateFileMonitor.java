package gov.nasa.ziggy.module;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmStateFiles.SubtaskState;
import gov.nasa.ziggy.module.StateFile.State;

/**
 * This class updates the task {@link StateFile} files based on a roll up of the sub-task
 * {@link AlgorithmStateFiles}s.
 * <p>
 * This file-based mechanism is used to communicate state between the worker process and a remote
 * CPU cluster.
 * <p>
 * This code performs a single pass through the current state files.
 *
 * @author Todd Klaus
 */
public class AlgorithmStateFileMonitor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AlgorithmStateFileMonitor.class);

    /**
     * Indicates that the task is currently executing on the Pleiades back-end A task is not
     * considered complete until this file has been deleted
     */
    private static final String IN_PROGRESS = ".IN_PROGRESS";

    /** How often we update state */
    private static final int POLL_INTERVAL_MILLIS = 10 * 1000; // 10 secs

    private final File stateFileDir;
    private final File taskRootDir;

    public AlgorithmStateFileMonitor(File stateFileDir, File taskRootDir) {
        this.stateFileDir = stateFileDir;
        this.taskRootDir = taskRootDir;
    }

    @Override
    public void run() {
        log.info("Remote monitor starting");
        log.info("Updating state for existing tasks...");
        while (true) {
            try {
                updateStateForRunningTasks();
            } catch (Exception e) {
                log.error("updateStateForRunningTasks failed, caught e = " + e, e);
            }

            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
            }
        }
    }

    private void updateStateForRunningTasks() throws Exception {
        List<State> stateFilters = new ArrayList<>();
        stateFilters.add(StateFile.State.PROCESSING);

        List<StateFile> runningTasks = StateFile.fromDirectory(stateFileDir, stateFilters);

        // for each PROCESSING and ERRORSRUNNING task, scan the sub-task dirs
        // to update the counts and overall state, if necessary
        for (StateFile stateFile : runningTasks) {
            String taskDirName = stateFile.taskDirName();
            File taskDir = new File(taskRootDir, taskDirName);
            File[] subTaskDirs = taskDir.listFiles();
            int numTotal = stateFile.getNumTotal();
            int numComplete = 0;
            int numFailed = 0;

            if (subTaskDirs != null) {
                for (File subTaskDir : subTaskDirs) {
                    AlgorithmStateFiles currentSubTaskStateFile = new AlgorithmStateFiles(
                        subTaskDir);
                    SubtaskState currentSubTaskState = currentSubTaskStateFile
                        .currentSubtaskState();

                    if (currentSubTaskState == null) {
                        // no algorithm state file exists yet
                        continue;
                    }

                    switch (currentSubTaskState) {
                        case COMPLETE:
                            numComplete++;
                            break;
                        case FAILED:
                            numFailed++;
                            break;
                        case PROCESSING:
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "Unexpected type: " + currentSubTaskState);
                    }
                }
            }

            File inProgressFile = new File(taskDir, IN_PROGRESS);
            boolean inProgress = inProgressFile.exists();

            StateFile newStateFile = new StateFile(stateFile);

            newStateFile.setNumTotal(numTotal);
            newStateFile.setNumComplete(numComplete);
            newStateFile.setNumFailed(numFailed);

            if (numComplete + numFailed == numTotal && !inProgress) {
                // done!
                newStateFile.setState(StateFile.State.COMPLETE);
            }

            if (!newStateFile.equals(stateFile)) {
                log.info("Updating state: " + stateFile + " -> " + newStateFile);
                // update the state file
                if (!StateFile.updateStateFile(stateFile, newStateFile, stateFileDir)) {
                    throw new PipelineException("Failed to update state file: " + stateFile);
                }
            } else {
                log.debug("No changes for: " + stateFile);
            }
        }
    }
}
