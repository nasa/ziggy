package gov.nasa.ziggy.pipeline.definition;

import java.util.List;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;

/**
 * Provides methods used by {@link PipelineModule} subclasses that make use of the
 * {@link ProcessingState} enumerated type. The methods allow the definition of valid processing
 * states, a mechanism to advance the processing state from one valid state to the next, and perform
 * database operations that manage the state as processing progresses.
 *
 * @author PT
 */
public interface ProcessingStatePipelineModule {

    /**
     * Defines the subset of {@link ProcessingState} enumerations that are valid, and the order in
     * which they are stepped through.
     */
    List<ProcessingState> processingStates();

    /**
     * Returns the ID of the {@link PipelineTask}.
     */
    long pipelineTaskId();

    /**
     * Returns the next processing state for the current pipeline module.
     *
     * @param currentProcessingState Current processing state of the module.
     * @return ProcessingState that is next in the list of states. If the currentProcessingState is
     * not in the list or is the last state in the list, a PipelineException is thrown.
     */
    default ProcessingState nextProcessingState(ProcessingState currentProcessingState) {

        int processingStateIndex = processingStates().indexOf(currentProcessingState);
        if (processingStateIndex == -1) {
            throw new PipelineException("Unable to locate next processing state after "
                + currentProcessingState.toString());
        }
        if (processingStateIndex == processingStates().size() - 1) {
            throw new PipelineException();
        }
        return processingStates().get(++processingStateIndex);
    }

    /**
     * Increments the processing state of a {@link PipelineTask} in the database from its current
     * state in the database.
     */
    default void incrementDatabaseProcessingState() {
        ProcessingState nextState = nextProcessingState(databaseProcessingState());
        processingSummaryOperations().updateProcessingState(pipelineTaskId(), nextState);
    }

    default ProcessingSummaryOperations processingSummaryOperations() {
        return new ProcessingSummaryOperations();
    }

    /**
     * Returns the current processing state in the database.
     *
     * @return current processing state.
     */
    default ProcessingState databaseProcessingState() {
        return processingSummaryOperations().processingSummary(pipelineTaskId())
            .getProcessingState();
    }

    /**
     * Performs the stepping through the processing states and executes the appropriate action at
     * each one.
     */
    void processingMainLoop();

    /**
     * Defines the task action to be taken when the task state is
     * {@link ProcessingState#INITIALIZING}.
     */
    void initializingTaskAction();

    /**
     * Defines the task action to be taken when the task state is
     * {@link ProcessingState#MARSHALING}.
     */
    void marshalingTaskAction();

    /**
     * Defines the task action to be taken when the task state is
     * {@link ProcessingState#ALGORITHM_SUBMITTING}.
     */
    void submittingTaskAction();

    /**
     * Defines the task action to be taken when the task state is
     * {@link ProcessingState#ALGORITHM_QUEUED}.
     */
    void queuedTaskAction();

    /**
     * Defines the task action to be taken when the task state is
     * {@link ProcessingState#ALGORITHM_EXECUTING}.
     */
    void executingTaskAction();

    /**
     * Defines the task action to be taken when the task state is
     * {@link ProcessingState#ALGORITHM_COMPLETE}.
     */
    void algorithmCompleteTaskAction();

    /**
     * Defines the task action to be taken when the task state is {@link ProcessingState#STORING}.
     */
    void storingTaskAction();

    /**
     * Defines the task action to be taken when the task state is {@link ProcessingState#COMPLETE}.
     */
    void processingCompleteTaskAction();
}
