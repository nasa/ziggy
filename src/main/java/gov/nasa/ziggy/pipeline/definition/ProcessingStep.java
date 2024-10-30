package gov.nasa.ziggy.pipeline.definition;

import java.io.Serializable;
import java.util.Set;
import java.util.function.Consumer;

import gov.nasa.ziggy.pipeline.definition.TaskCounts.Counts;

/**
 * Current processing step. The enumerations also provide support for actions that are to be
 * performed in each step on the given {@link PipelineModule} or to be counted in a
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

    INITIALIZING(PipelineModule::initializingTaskAction, Counts::incrementInitializingTaskCount),
    WAITING_TO_RUN(PipelineModule::waitingToRunTaskAction, Counts::incrementWaitingToRunTaskCount),
    MARSHALING(PipelineModule::marshalingTaskAction, Counts::incrementMarshallingTaskCount),
    SUBMITTING(PipelineModule::submittingTaskAction, Counts::incrementSubmittingTaskCount),
    QUEUED(PipelineModule::queuedTaskAction, Counts::incrementQueuedTaskCount),
    EXECUTING(PipelineModule::executingTaskAction, Counts::incrementExecutingTaskCount),
    WAITING_TO_STORE(PipelineModule::waitingToStoreTaskAction,
        Counts::incrementWaitingToStoreTaskCount),
    STORING(PipelineModule::storingTaskAction, Counts::incrementStoringTaskCount),
    COMPLETE(PipelineModule::processingCompleteTaskAction, Counts::incrementCompleteTaskCount);

    private Consumer<PipelineModule> taskAction;
    private Consumer<Counts> countsAction;

    ProcessingStep(Consumer<PipelineModule> taskAction, Consumer<Counts> countsAction) {
        this.taskAction = taskAction;
        this.countsAction = countsAction;
    }

    public void taskAction(PipelineModule pipelineModule) {
        taskAction.accept(pipelineModule);
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
