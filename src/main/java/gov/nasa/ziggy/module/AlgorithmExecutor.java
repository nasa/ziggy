package gov.nasa.ziggy.module;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.SupportedRemoteClusters;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;;

/**
 * Superclass for algorithm execution. This includes local execution via the
 * {@link LocalAlgorithmExecutor}, as well as HPC / cloud execution.
 *
 * @author PT
 */

public abstract class AlgorithmExecutor {

    private static final Logger log = LoggerFactory.getLogger(AlgorithmExecutor.class);

    public static final String ZIGGY_PROGRAM = "ziggy";

    protected static final String NODE_MASTER_NAME = "compute-node-master";

    protected final PipelineTask pipelineTask;
    private ParameterSetCrud parameterSetCrud;
    private PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud;
    private ProcessingSummaryOperations processingSummaryOperations;

    private StateFile stateFile;

    /**
     * Returns a new instance of the appropriate {@link AlgorithmExecutor} subclass.
     */
    public static final AlgorithmExecutor newInstance(PipelineTask pipelineTask) {
        return newInstance(pipelineTask, new ParameterSetCrud(), new PipelineDefinitionNodeCrud(),
            new ProcessingSummaryOperations());
    }

    /**
     * Version of {@link #newInstance(PipelineTask)} that accepts user-supplied
     * {@link ParameterSetCrud} and {@link ProcessingSummaryOperations} instances. Allows these
     * classes to be mocked for testing.
     */
    static final AlgorithmExecutor newInstance(PipelineTask pipelineTask,
        ParameterSetCrud parameterSetCrud, PipelineDefinitionNodeCrud defNodeCrud,
        ProcessingSummaryOperations processingSummaryOperations) {

        if (pipelineTask == null) {
            log.debug("Pipeline task is null, returning LocalAlgorithmExecutor instance");
            return new LocalAlgorithmExecutor(pipelineTask);
        }
        PipelineDefinitionNodeExecutionResources remoteParams = defNodeCrud
            .retrieveExecutionResources(pipelineTask.pipelineDefinitionNode());

        if (remoteParams == null) {
            log.debug("Remote parameters null, returning LocalAlgorithmExecutor instance");
            return new LocalAlgorithmExecutor(pipelineTask);
        }
        if (!remoteParams.isRemoteExecutionEnabled()) {
            log.debug("Remote execution not selected, returning LocalAlgorithmExecutor instance");
            return new LocalAlgorithmExecutor(pipelineTask);
        }
        ProcessingSummary processingState = processingSummaryOperations
            .processingSummary(pipelineTask.getId());
        log.debug("Total subtasks " + processingState.getTotalSubtaskCount());
        log.debug("Completed subtasks " + processingState.getCompletedSubtaskCount());
        int subtasksToRun = processingState.getTotalSubtaskCount()
            - processingState.getCompletedSubtaskCount();
        if (subtasksToRun < remoteParams.getMinSubtasksForRemoteExecution()) {
            log.info("Number subtasks to run (" + subtasksToRun
                + ") less than min subtasks for remote execution ("
                + remoteParams.getMinSubtasksForRemoteExecution() + ")");
            log.info("Executing task " + pipelineTask.getId() + " locally");
            return new LocalAlgorithmExecutor(pipelineTask);
        }
        return newRemoteInstance(pipelineTask);
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public static final AlgorithmExecutor newRemoteInstance(PipelineTask pipelineTask) {
        Constructor<? extends AlgorithmExecutor> ctor;
        Class<? extends AlgorithmExecutor> implementationClass = SupportedRemoteClusters
            .remoteCluster()
            .getRemoteExecutorClass();
        try {
            ctor = implementationClass.getDeclaredConstructor(PipelineTask.class);
            ctor.setAccessible(true);
            return ctor.newInstance(pipelineTask);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException
            | IllegalArgumentException | InvocationTargetException e) {
            // This can never occur. By construction the desired constructor signature
            // must exist.
            throw new AssertionError(e);
        }
    }

    protected AlgorithmExecutor(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    /**
     * Submits the {@link PipelineTask} for execution. This follows a somewhat different code path
     * depending on whether the submission is the original submission or a resubmission. In the
     * event of a resubmission, there is no {@link TaskConfiguration} argument required because
     * subtask counts can be obtained from the database.
     *
     * @param inputsHandler Will be null for resubmission.
     */
    public void submitAlgorithm(TaskConfiguration inputsHandler) {

        prepareToSubmitAlgorithm(inputsHandler);

        IntervalMetric.measure(PipelineMetrics.SEND_METRIC, () -> {
            log.info("Submitting task for execution (taskId=" + pipelineTask.getId() + ")");

            Files.createDirectories(algorithmLogDir());
            Files.createDirectories(DirectoryProperties.stateFilesDir());
            Files.createDirectories(taskDataDir());
            SubtaskUtils.clearStaleAlgorithmStates(
                new TaskDirectoryManager(pipelineTask).taskDir().toFile());

            log.info("Start remote monitoring (taskId=" + pipelineTask.getId() + ")");
            submitForExecution(stateFile);
            return null;
        });
    }

    private void prepareToSubmitAlgorithm(TaskConfiguration inputsHandler) {
        // execute the external process on a remote host
        int numSubtasks;
        PbsParameters pbsParameters = null;

        PipelineDefinitionNodeExecutionResources executionResources = (PipelineDefinitionNodeExecutionResources) DatabaseTransactionFactory
            .performTransaction(() -> pipelineDefinitionNodeCrud()
                .retrieveExecutionResources(pipelineTask.pipelineDefinitionNode()));

        // Initial submission: this is indicated by a non-null task configuration manager
        if (inputsHandler != null) { // indicates initial submission
            log.info("Processing initial submission of task " + pipelineTask.getId());
            numSubtasks = inputsHandler.getSubtaskCount();

            pbsParameters = generatePbsParameters(executionResources, numSubtasks);

            // Resubmission: this is indicated by a null task configuration manager, which
            // means that subtask counts are available in the database
        } else

        {
            log.info("Processing resubmission of task " + pipelineTask.getId());
            ProcessingSummary processingState = processingSummaryOperations()
                .processingSummary(pipelineTask.getId());
            numSubtasks = processingState.getTotalSubtaskCount();
            int numCompletedSubtasks = processingState.getCompletedSubtaskCount();

            // Scale the total subtasks to get to the number that still need to be processed
            double subtaskCountScaleFactor = (double) (numSubtasks - numCompletedSubtasks)
                / (double) numSubtasks;

            // Get the current remote parameters
            pbsParameters = generatePbsParameters(executionResources,
                (int) (numSubtasks * subtaskCountScaleFactor));
        }

        stateFile = StateFile.generateStateFile(pipelineTask, pbsParameters, numSubtasks);
    }

    /**
     * Resubmit the pipeline task to the appropriate {@link AlgorithmMonitor}. This is used in the
     * case where the supervisor has been stopped and restarted but tasks are still running (usually
     * remotely). This notifies the monitor that there are tasks that it should be looking out for.
     */
    public void resumeMonitoring() {
        prepareToSubmitAlgorithm(null);
        addToMonitor(stateFile);
    }

    protected abstract void addToMonitor(StateFile stateFile);

    protected abstract void submitForExecution(StateFile stateFile);

    /**
     * Generates an updated instance of {@link PbsParameters}. The method is abstract because each
     * implementation of {@link AlgorithmExecutor} has specific needs for its PBS command, hence
     * each needs its own implementation of this method.
     */
    public abstract PbsParameters generatePbsParameters(
        PipelineDefinitionNodeExecutionResources executionResources, int totalSubtaskCount);

    protected Path algorithmLogDir() {
        return DirectoryProperties.algorithmLogsDir();
    }

    protected Path taskDataDir() {
        return DirectoryProperties.taskDataDir();
    }

    protected Path workingDir() {
        return taskDataDir().resolve(pipelineTask.taskBaseName());
    }

    public abstract AlgorithmType algorithmType();

    // For testing purposes we need to be able to insert a mocked ParameterSetCrud instance,
    // so we will use the standard pattern of a package-private setter and a protected getter
    // for the object.
    protected ParameterSetCrud parameterSetCrud() {
        if (parameterSetCrud == null) {
            parameterSetCrud = new ParameterSetCrud();
        }
        return parameterSetCrud;
    }

    protected void setParameterSetCrud(ParameterSetCrud parameterSetCrud) {
        this.parameterSetCrud = parameterSetCrud;
    }

    protected PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud() {
        if (pipelineDefinitionNodeCrud == null) {
            pipelineDefinitionNodeCrud = new PipelineDefinitionNodeCrud();
        }
        return pipelineDefinitionNodeCrud;
    }

    protected ProcessingSummaryOperations processingSummaryOperations() {
        if (processingSummaryOperations == null) {
            processingSummaryOperations = new ProcessingSummaryOperations();
        }
        return processingSummaryOperations;
    }

    protected void setProcessingSummaryOperations(
        ProcessingSummaryOperations processingSummaryOperations) {
        this.processingSummaryOperations = processingSummaryOperations;
    }

    public StateFile getStateFile() {
        return stateFile;
    }

    public enum AlgorithmType {
        /** local execution only */
        LOCAL,

        /** Pleiades execution (database server is inside NAS enclave) */
        REMOTE

    }
}
