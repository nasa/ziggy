package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * This class manages zero-length files whose names are used to represent the state of an executing
 * algorithm. These files are stored in the subtask working directory.
 *
 * @author Todd Klaus
 * @author PT
 */
public class AlgorithmStateFiles {
    private static final Logger log = LoggerFactory.getLogger(AlgorithmStateFiles.class);

    private static final String HAS_RESULTS = "HAS_RESULTS";

    public enum SubtaskState {
        // State in which no AlgorithmStateFile is present. Rather than return an actual
        // null, when queried about the subtask state we can return SubtaskState.NULL .
        NULL {
            @Override
            public void updateStateCounts(SubtaskStateCounts stateCounts) {
                // nothing to do in this case
            }
        },
        PROCESSING {
            @Override
            public void updateStateCounts(SubtaskStateCounts stateCounts) {
                // Nothing to do in this case.
            }
        },
        FAILED {
            @Override
            public void updateStateCounts(SubtaskStateCounts stateCounts) {
                stateCounts.incrementFailedSubtasks();
            }
        },
        COMPLETE {
            @Override
            public void updateStateCounts(SubtaskStateCounts stateCounts) {
                stateCounts.incrementCompletedSubtasks();
            }
        };

        public abstract void updateStateCounts(SubtaskStateCounts stateCounts);
    }

    private final File processingFlag;
    private final File completeFlag;
    private final File failedFlag;
    private final File resultsFlag;

    public AlgorithmStateFiles(File workingDir) {
        processingFlag = new File(workingDir, "." + SubtaskState.PROCESSING.toString());
        completeFlag = new File(workingDir, "." + SubtaskState.COMPLETE.toString());
        failedFlag = new File(workingDir, "." + SubtaskState.FAILED.toString());
        resultsFlag = new File(workingDir, "." + HAS_RESULTS);
    }

    public static boolean isComplete(File workingDir) {
        return new AlgorithmStateFiles(workingDir).isComplete();
    }

    public static boolean hasResults(File workingDir) {
        return new AlgorithmStateFiles(workingDir).resultsFlag.exists();
    }

    public void clearState() {
        processingFlag.delete();
        completeFlag.delete();
        failedFlag.delete();
    }

    /**
     * Removes any "stale" state flags. A stale state flag is one from a previous processing attempt
     * that will cause the pipeline to either miscount the task/ sub-task, or do the wrong thing
     * with it. COMPLETED states are never stale, because they finished and don't need to be
     * restarted. FAILED and PROCESSING flags are stale, because they indicate incomplete prior
     * execution but can prevent the current execution attempt from starting.
     * <p>
     * If the subtask state is not COMPLETE, any results flag in the subdirectory is also deleted in
     * order to ensure that any results flag present after rerunning is due to the re-run and not
     * the preceding run.
     */
    public void clearStaleState() {
        if (!currentSubtaskState().equals(SubtaskState.COMPLETE)) {
            resultsFlag.delete();
        }
        processingFlag.delete();
        failedFlag.delete();
    }

    public void updateCurrentState(SubtaskState newState) throws IOException {
        clearState();

        switch (newState) {
            case COMPLETE:
                completeFlag.createNewFile();
                break;

            case PROCESSING:
                processingFlag.createNewFile();
                break;

            case FAILED:
                failedFlag.createNewFile();
                break;

            default:
                throw new IllegalStateException("Unknown state: " + newState);
        }
    }

    public void setResultsFlag() throws IOException {
        resultsFlag.createNewFile();
    }

    public SubtaskState currentSubtaskState() {
        SubtaskState current = SubtaskState.NULL;

        if (robustExists(processingFlag)) {
            current = SubtaskState.PROCESSING;
        }
        if (robustExists(completeFlag)) {
            if (current != SubtaskState.NULL) {
                log.warn("Duplicate algorithm state files found!");
                return null;
            }
            current = SubtaskState.COMPLETE;
        }
        if (robustExists(failedFlag)) {
            if (current != SubtaskState.NULL) {
                log.warn("Duplicate algorithm state files found!");
                return null;
            }
            current = SubtaskState.FAILED;
        }
        log.debug("current state: " + current);
        return current;
    }

    /**
     * Returns true if a subtask state file exists, false otherwise
     *
     * @return
     */
    public boolean subtaskStateExists() {
        return robustExists(completeFlag) || robustExists(processingFlag)
            || robustExists(failedFlag);
    }

    /**
     * Verifies that a file exists by attempting to read it instead of just using File.exists() in
     * order to bypass the NFS client cache
     *
     * @param f
     * @return
     */
    private boolean robustExists(File f) {
        try {
            Files.toByteArray(f);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean isProcessing() {
        return currentSubtaskState() == SubtaskState.PROCESSING;
    }

    public boolean isComplete() {
        return currentSubtaskState() == SubtaskState.COMPLETE;
    }

    public boolean isFailed() {
        return currentSubtaskState() == SubtaskState.FAILED;
    }

    // For testing only.
    AlgorithmStateFiles() {
        this(new File("dummy"));
    }

    /**
     * Simple class that holds the number of failed and completed subtasks detected by checking the
     * {@link AlgorithmStateFiles} contents of each subtask directory.
     *
     * @author PT
     */
    public static class SubtaskStateCounts {

        private int failedSubtasks = 0;
        private int completedSubtasks = 0;

        public void incrementFailedSubtasks() {
            failedSubtasks++;
        }

        public void incrementCompletedSubtasks() {
            completedSubtasks++;
        }

        public int getFailedSubtasks() {
            return failedSubtasks;
        }

        public int getCompletedSubtasks() {
            return completedSubtasks;
        }
    }

}
