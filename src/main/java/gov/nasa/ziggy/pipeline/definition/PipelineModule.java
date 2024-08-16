package gov.nasa.ziggy.pipeline.definition;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * This is the super-class for all pipeline modules.
 * <p>
 * It defines the entry point called by the pipeline infrastructure when a task arrives for this
 * module (processTask()).
 * <p>
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 * @author PT
 * @author Bill Wohler
 */
public abstract class PipelineModule {

    private static final Logger log = LoggerFactory.getLogger(PipelineModule.class);

    protected PipelineTask pipelineTask;
    protected RunMode runMode;

    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    /**
     * Standard constructor. Stores the {@link PipelineTask} and {@link RunMode} for the instance.
     */
    public PipelineModule(PipelineTask pipelineTask, RunMode runMode) {
        this.pipelineTask = checkNotNull(pipelineTask, "pipelineTask");
        this.runMode = checkNotNull(runMode, "runMode");
    }

    public final long taskId() {
        return pipelineTask.getId();
    }

    /**
     * Defines the subset of {@link ProcessingStep} enumerations that are valid, and the order in
     * which they are stepped through.
     */
    public List<ProcessingStep> processingSteps() {
        return List.of(ProcessingStep.EXECUTING);
    }

    /**
     * Returns the next processing step for the current pipeline module.
     *
     * @param currentProcessingStep current processing step of the module
     * @return ProcessingStep that is next in the list of steps
     * @throw PipelineException if the currentProcessingStep is not in the list or is the last step
     * in the list, a PipelineException is thrown
     */
    public ProcessingStep nextProcessingStep(ProcessingStep currentProcessingStep) {

        int processingStepIndex = processingSteps().indexOf(currentProcessingStep);
        if (processingStepIndex == -1) {
            throw new PipelineException("Processing step " + currentProcessingStep.toString()
                + " not valid for this pipeline module");
        }
        if (processingStepIndex == processingSteps().size() - 1) {
            throw new PipelineException(
                "No more processing steps after " + currentProcessingStep.toString());
        }
        return processingSteps().get(++processingStepIndex);
    }

    /**
     * Increments the processing step of a {@link PipelineTask} in the database from its current
     * step in the database.
     */
    public void incrementProcessingStep() {
        pipelineTaskOperations().updateProcessingStep(taskId(),
            nextProcessingStep(currentProcessingStep()));
    }

    /**
     * Returns the current processing step in the database.
     *
     * @return current processing step
     */
    public ProcessingStep currentProcessingStep() {
        return pipelineTaskOperations().pipelineTask(taskId()).getProcessingStep();
    }

    /**
     * Defines the task action to be taken when the task step is
     * {@link ProcessingStep#INITIALIZING}. This action is performed by the infrastructure so
     * pipeline module authors are not allowed to define their own.
     */
    public final void initializingTaskAction() {
    }

    /**
     * Defines the task action to be taken when the task step is
     * {@link ProcessingStep#WAITING_TO_RUN}. This action is performed by the infrastructure so
     * pipeline module authors are not allowed to define their own.
     */
    public final void waitingToRunTaskAction() {
    }

    /**
     * Defines the task action to be taken when the task step is {@link ProcessingStep#MARSHALING}.
     * The default action is to do nothing.
     */
    public void marshalingTaskAction() {
    }

    /**
     * Defines the task action to be taken when the task step is {@link ProcessingStep#SUBMITTING}.
     * The default action is to do nothing.
     */
    public void submittingTaskAction() {
    }

    /**
     * Defines the task action to be taken when the task step is {@link ProcessingStep#QUEUED}. The
     * default action is to do nothing.
     */
    public void queuedTaskAction() {
    }

    /**
     * Defines the task action to be taken when the task step is {@link ProcessingStep#EXECUTING}.
     * The default action is to do nothing.
     */
    public void executingTaskAction() {
    }

    /**
     * Defines the task action to be taken when the task step is
     * {@link ProcessingStep#WAITING_TO_STORE}. The default action is to do nothing.
     */
    public void waitingToStoreTaskAction() {
    }

    /**
     * Defines the task action to be taken when the task step is {@link ProcessingStep#STORING}. The
     * default action is to do nothing.
     */
    public void storingTaskAction() {
    }

    /**
     * Defines the task action to be taken when the task step is {@link ProcessingStep#COMPLETE}.
     * This action is performed by the infrastructure so pipeline module authors are not allowed to
     * define their own.
     */
    public final void processingCompleteTaskAction() {
    }

    /**
     * Modules should subclass this or in some cases generateInputs and processOutputs(). This is
     * how they perform the work for a pipeline task.
     *
     * @return true if task was completed.
     * @throws Exception if an exception occurs in a concrete implementation. Because the
     * implementation can throw practically anything, we are forced to be prepared for practically
     * anything. Hence, {@link Exception} rather than a subclass of {@link Exception}.
     */
    public abstract boolean processTask() throws Exception;

    /**
     * Update the PipelineTask.summaryMetrics.
     * <p>
     * This default implementation adds a single category ("ALL") with the overall execution time.
     * <p>
     * Subclasses can override this method to provide module-specific categories.
     *
     * @param pipelineTask
     */
    public void updateMetrics(PipelineTask pipelineTask, Map<String, Metric> threadMetrics,
        long overallExecTimeMillis) {
        List<PipelineTaskMetrics> taskMetrics = new ArrayList<>();
        PipelineTaskMetrics m = new PipelineTaskMetrics("All", overallExecTimeMillis, Units.TIME);
        taskMetrics.add(m);
        pipelineTask.setSummaryMetrics(taskMetrics);
    }

    public abstract String getModuleName();

    /**
     * Returns the supported restart modes for a given concrete class. The method is guaranteed to
     * return at least one restart method, RESTART_FROM_BEGINNING, if the class otherwise provides
     * no information about restart modes. Also, the STANDARD run mode will be removed if the
     * concrete class erroneously includes that as a restart mode.
     */

    public final List<RunMode> supportedRestartModes() {
        Set<RunMode> supportedRestartModes = new LinkedHashSet<>(defaultRestartModes());
        List<RunMode> restartModes = restartModes();
        if (restartModes != null && !restartModes.isEmpty()) {
            supportedRestartModes.addAll(restartModes);
        }
        supportedRestartModes.remove(RunMode.STANDARD);
        return new ArrayList<>(supportedRestartModes);
    }

    /**
     * Ensures that all {@link PipelineModule} concrete classes will support restart from beginning
     * as a restart mode, even if the developer who wrote the class ignored the entire issue of
     * restart modes. This method can be overridden if restarting from the beginning is
     * inappropriate, which may be the case if the pipeline removes the inputs for example.
     */
    protected List<RunMode> defaultRestartModes() {
        return List.of(RunMode.RESTART_FROM_BEGINNING);
    }

    /**
     * The specific restart modes that a subclass supports.
     */
    protected abstract List<RunMode> restartModes();

    protected void logTaskInfo(PipelineInstance instance, PipelineTask task) {
        log.debug("[" + getModuleName() + "]instance id = " + instance.getId());
        log.debug("[" + getModuleName() + "]instance node id = " + task.getId());
        log.debug(
            "[" + getModuleName() + "]instance node uow = " + task.uowTaskInstance().briefState());
    }

    // TODO Make protected so it's not in the public API
    // The unit test that uses this method can define its own PipelineModule to mock this field.
    public PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    // Run modes:
    //
    // The RunMode enum, below, must have all of the restart modes that can be used by any
    // subclass of PipelineModule, plus STANDARD. Each run mode has a run() method that, in
    // turn, calls one of the abstract void methods below. This is a bit of a kludge, but it
    // reasonably assures that (a) all restart modes have module-specific methods they can execute,
    // but (b) otherwise we get all the virtues of enum-with-behavior, which is safer than using
    // an enum in a switch statement.

    /** Defines what it means to restart from the beginning. The default action is to do nothing. */
    protected void restartFromBeginning() {
    }

    /**
     * Defines what it means to resume from the current step. The default action is to do nothing.
     */
    protected void resumeCurrentStep() {
    }

    /** Defines what it means to resubmit the task. The default action is to do nothing. */
    protected void resubmit() {
    }

    /** Defines what it means to resume monitoring. The default action is to do nothing. */
    protected void resumeMonitoring() {
    }

    /**
     * Defines the standard run mode. The default action is to do nothing, so you'll definitely want
     * to override this method if you want your module to do something useful.
     */
    protected void runStandard() {
    }

    public enum RunMode {
        RESTART_FROM_BEGINNING(PipelineModule::restartFromBeginning),
        RESUME_CURRENT_STEP(PipelineModule::resumeCurrentStep),
        RESUBMIT(PipelineModule::resubmit),
        RESUME_MONITORING(PipelineModule::resumeMonitoring),
        STANDARD(PipelineModule::runStandard);

        private Consumer<PipelineModule> taskAction;

        RunMode(Consumer<PipelineModule> taskAction) {
            this.taskAction = taskAction;
        }

        public void run(PipelineModule pipelineModule) {
            taskAction.accept(pipelineModule);
        }

        @Override
        public String toString() {
            return this == STANDARD ? "-"
                : ZiggyStringUtils.constantToSentenceWithSpaces(super.toString());
        }
    }
}
