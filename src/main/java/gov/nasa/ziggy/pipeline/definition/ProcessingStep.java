package gov.nasa.ziggy.pipeline.definition;

import java.io.Serializable;
import java.util.Set;
import java.util.function.Consumer;

import gov.nasa.ziggy.pipeline.definition.TaskCounts.Counts;

/**
 * Current processing step. The enumerations also provide support for actions that are to be
 * performed in each step on the given {@link PipelineStepExecutor} or to be counted in a
 * {@link TaskCounts} object.
 * <p>
 * The {@link #INITIALIZING}, {@link #WAITING_TO_RUN}, and {@link #COMPLETE} steps are performed by
 * the infrastructure (see {@link #isInfrastructureStep()}) Therefore, the first step that pipeline
 * authors will define is {@link #MARSHALING}, but only if applicable.
 *
 * @author PT
 * @author Bill Wohler
 */
public enum ProcessingStep implements Serializable {

    INITIALIZING(PipelineStepExecutor::initializingTaskAction,
        Counts::incrementInitializingTaskCount),
    WAITING_TO_RUN(PipelineStepExecutor::waitingToRunTaskAction,
        Counts::incrementWaitingToRunTaskCount),
    MARSHALING(PipelineStepExecutor::marshalingTaskAction, Counts::incrementMarshallingTaskCount),
    SUBMITTING(PipelineStepExecutor::submittingTaskAction, Counts::incrementSubmittingTaskCount),
    QUEUED(PipelineStepExecutor::queuedTaskAction, Counts::incrementQueuedTaskCount),
    EXECUTING(PipelineStepExecutor::executingTaskAction, Counts::incrementExecutingTaskCount),
    WAITING_TO_STORE(PipelineStepExecutor::waitingToStoreTaskAction,
        Counts::incrementWaitingToStoreTaskCount),
    STORING(PipelineStepExecutor::storingTaskAction, Counts::incrementStoringTaskCount),
    COMPLETE(PipelineStepExecutor::processingCompleteTaskAction,
        Counts::incrementCompleteTaskCount);

    private Consumer<PipelineStepExecutor> taskAction;
    private Consumer<Counts> countsAction;

    ProcessingStep(Consumer<PipelineStepExecutor> taskAction, Consumer<Counts> countsAction) {
        this.taskAction = taskAction;
        this.countsAction = countsAction;
    }

    public void taskAction(PipelineStepExecutor pipelineStepExecutor) {
        taskAction.accept(pipelineStepExecutor);
    }

    public void incrementTaskCount(Counts counts) {
        countsAction.accept(counts);
    }

    public boolean isInfrastructureStep() {
        return this == INITIALIZING || this == WAITING_TO_RUN || this == COMPLETE;
    }

    public boolean isPreExecutionStep() {
        return this == INITIALIZING || this == WAITING_TO_RUN || this == MARSHALING
            || this == QUEUED || this == SUBMITTING;
    }

    public static Set<ProcessingStep> processingSteps() {
        return Set.of(WAITING_TO_RUN, MARSHALING, SUBMITTING, QUEUED, EXECUTING, WAITING_TO_STORE,
            STORING);
    }
}
