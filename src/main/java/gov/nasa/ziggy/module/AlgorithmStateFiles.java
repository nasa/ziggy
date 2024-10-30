package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * This class manages zero-length files whose names are used to represent the state of an executing
 * algorithm. These files are stored in the task and subtask working directories.
 *
 * @author Todd Klaus
 * @author PT
 */
public class AlgorithmStateFiles {
    private static final Logger log = LoggerFactory.getLogger(AlgorithmStateFiles.class);

    private static final String HAS_OUTPUTS = "HAS_OUTPUTS";

    public enum AlgorithmState {
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
    private final File outputsFlag;

    public AlgorithmStateFiles(File workingDir) {
        processingFlag = new File(workingDir, "." + AlgorithmState.PROCESSING.toString());
        completeFlag = new File(workingDir, "." + AlgorithmState.COMPLETE.toString());
        failedFlag = new File(workingDir, "." + AlgorithmState.FAILED.toString());
        outputsFlag = new File(workingDir, "." + HAS_OUTPUTS);
    }

    public static boolean isComplete(File workingDir) {
        return new AlgorithmStateFiles(workingDir).isComplete();
    }

    public static boolean hasOutputs(File workingDir) {
        return new AlgorithmStateFiles(workingDir).outputsFlag.exists();
    }

    public void clearState() {
        processingFlag.delete();
        completeFlag.delete();
        failedFlag.delete();
    }

    /**
     * Removes any "stale" state flags. A stale state flag is one from a previous processing attempt
     * that will cause the pipeline to either miscount the task/ subtask, or do the wrong thing
     * with it. COMPLETED states are never stale, because they finished and don't need to be
     * restarted. FAILED and PROCESSING flags are stale, because they indicate incomplete prior
     * execution but can prevent the current execution attempt from starting.
     * <p>
     * If the subtask state is not COMPLETE, any results flag in the subdirectory is also deleted in
     * order to ensure that any results flag present after rerunning is due to the re-run and not
     * the preceding run.
     */
    public void clearStaleState() {
        if (!currentAlgorithmState().equals(AlgorithmState.COMPLETE)) {
            outputsFlag.delete();
        }
        processingFlag.delete();
        failedFlag.delete();
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void updateCurrentState(AlgorithmState newState) {
        clearState();

        try {
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

                case NULL:
                    throw new IllegalArgumentException(
                        "Unable to create new algorithm state file for state " + newState);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create new algorithm state file", e);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void setOutputsFlag() {
        try {
            outputsFlag.createNewFile();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create new file " + outputsFlag.toString(),
                e);
        }
    }

    public AlgorithmState currentAlgorithmState() {
        AlgorithmState current = AlgorithmState.NULL;

        if (processingFlag.exists()) {
            current = AlgorithmState.PROCESSING;
        }
        if (completeFlag.exists()) {
            if (current != AlgorithmState.NULL) {
                log.warn("Duplicate algorithm state files found!");
                return null;
            }
            current = AlgorithmState.COMPLETE;
        }
        if (failedFlag.exists()) {
            if (current != AlgorithmState.NULL) {
                log.warn("Duplicate algorithm state files found!");
                return null;
            }
            current = AlgorithmState.FAILED;
        }
        log.debug("current={}", current);
        return current;
    }

    /**
     * Returns true if a subtask state file exists, false otherwise
     *
     * @return
     */
    public boolean stateExists() {
        return completeFlag.exists() || processingFlag.exists() || failedFlag.exists();
    }

    public boolean isProcessing() {
        return currentAlgorithmState() == AlgorithmState.PROCESSING;
    }

    public boolean isComplete() {
        return currentAlgorithmState() == AlgorithmState.COMPLETE;
    }

    public boolean isFailed() {
        return currentAlgorithmState() == AlgorithmState.FAILED;
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
