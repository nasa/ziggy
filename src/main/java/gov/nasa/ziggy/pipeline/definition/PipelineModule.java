package gov.nasa.ziggy.pipeline.definition;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;

/**
 * This is the super-class for all pipeline modules.
 * <p>
 * It defines the entry point called by the pipeline infrastructure when a task arrives for this
 * module (processTask()).
 * <p>
 * Important note related to task deletion:
 * <p>
 * Ziggy provides the capability to halt task execution, both prior to the start of execution (when
 * a task is submitted but not yet processing) and during execution. However: because Java's model
 * of thread interruption is "cooperative," {@link PipelineModule} subclasses need to provide
 * support for task deletion if they are expected to allow a task to be deleted once
 * {@link #processTask()} has been called. In particular, the module must check for thread
 * interruption using {@link Thread#isInterrupted()} on the current thread and returning from
 * {@link #processTask()} if an interruption is detected.
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 */
public abstract class PipelineModule {

    private static final Logger log = LoggerFactory.getLogger(PipelineModule.class);

    protected PipelineTask pipelineTask;
    protected RunMode runMode;

    /**
     * If set to true by a sub-class, the current task will be committed when the task completes,
     * but the transition logic will not be run.
     */
    private boolean haltPipelineOnTaskCompletion = false;

    /**
     * Used by sub-classes to indicate that only a subset of the unit of work was processed
     * successfully. In this case, the transaction will be committed and the transition logic will
     * be executed, but the {@link PipelineTask} state will be set to PARTIAL instead of COMPLETE.
     */
    private boolean partialSuccess = false;

    /**
     * Standard constructor. Stores the {@link PipelineTask} and {@link RunMode} for the instance.
     *
     * @param pipelineTask
     * @param runMode
     */
    public PipelineModule(PipelineTask pipelineTask, RunMode runMode) {
        this.pipelineTask = checkNotNull(pipelineTask, "pipelineTask");
        this.runMode = checkNotNull(runMode, "runMode");
    }

    public final long taskId() {
        return pipelineTask.getId();
    }

    /**
     * Indicates whether the {@link processTask} method of a given subclass must be executed within
     * a database transaction. Override to set to false if this is not the case.
     */
    public boolean processTaskRequiresDatabaseTransaction() {
        return true;
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
        Set<RunMode> supportedRestartModes = new HashSet<>(defaultRestartModes());
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
     * restart modes.
     */
    protected List<RunMode> defaultRestartModes() {
        return Arrays.asList(RunMode.RESTART_FROM_BEGINNING);
    }

    /**
     * The specific restart modes that a subclass supports.
     */
    protected abstract List<RunMode> restartModes();

    /**
     * Returns the text strings for the supported restart modes.
     */
    public List<String> supportedRestartModeStrings() {
        return supportedRestartModes().stream().map(RunMode::toString).collect(Collectors.toList());
    }

    protected void logTaskInfo(PipelineInstance instance, PipelineTask task) {
        log.debug("[" + getModuleName() + "]instance id = " + instance.getId());
        log.debug("[" + getModuleName() + "]instance node id = " + task.getId());
        log.debug(
            "[" + getModuleName() + "]instance node uow = " + task.uowTaskInstance().briefState());
    }

    public boolean isHaltPipelineOnTaskCompletion() {
        return haltPipelineOnTaskCompletion;
    }

    public void setHaltPipelineOnTaskCompletion(boolean haltPipelineOnTaskCompletion) {
        this.haltPipelineOnTaskCompletion = haltPipelineOnTaskCompletion;
    }

    public boolean isPartialSuccess() {
        return partialSuccess;
    }

    public void setPartialSuccess(boolean partialSuccess) {
        this.partialSuccess = partialSuccess;
    }

    // Run modes:
    //
    // The RunMode enum, below, must have all of the restart modes that can be used by any
    // subclass of PipelineModule, plus STANDARD. Each run mode has a run() method that, in
    // turn, calls one of the abstract void methods below. This is a bit of a kludge, but it
    // reasonably assures that (a) all restart modes have module-specific methods they can execute,
    // but (b) otherwise we get all the virtues of enum-with-behavior, which is safer than using
    // an enum in a switch statement.

    protected abstract void restartFromBeginning();

    protected abstract void resumeCurrentStep();

    protected abstract void resubmit();

    protected abstract void resumeMonitoring();

    protected abstract void runStandard();

    public enum RunMode {
        RESTART_FROM_BEGINNING {
            @Override
            public void run(PipelineModule pipelineModule) {
                pipelineModule.restartFromBeginning();
            }

            @Override
            public String toString() {
                return "Restart from beginning";
            }
        },
        RESUME_CURRENT_STEP {
            @Override
            public void run(PipelineModule pipelineModule) {
                pipelineModule.resumeCurrentStep();
            }

            @Override
            public String toString() {
                return "Resume current step";
            }
        },
        RESUBMIT {
            @Override
            public void run(PipelineModule pipelineModule) {
                pipelineModule.resubmit();
            }

            @Override
            public String toString() {
                return "Resubmit";
            }
        },

        RESUME_MONITORING {
            @Override
            public void run(PipelineModule pipelineModule) {
                pipelineModule.resumeMonitoring();
            }

            @Override
            public String toString() {
                return "Resume monitoring";
            }
        },

        STANDARD {
            @Override
            public void run(PipelineModule pipelineModule) {
                pipelineModule.runStandard();
            }

            @Override
            public String toString() {
                return "-";
            }
        };

        public abstract void run(PipelineModule pipelineModule);

        @Override
        public abstract String toString();
    }
}
