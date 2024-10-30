package gov.nasa.ziggy.module;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
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
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;;

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
    public static final String ACTIVE_CORES_FILE_NAME = ".activeCoresPerNode";
    public static final String WALL_TIME_FILE_NAME = ".requestedWallTimeSeconds";

    protected final PipelineTask pipelineTask;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    private PbsParameters pbsParameters;

    /**
     * Returns a new instance of the appropriate {@link AlgorithmExecutor} subclass.
     */
    public static final AlgorithmExecutor newInstance(PipelineTask pipelineTask) {
        return newInstance(pipelineTask, new PipelineTaskOperations(),
            new PipelineTaskDataOperations());
    }

    /**
     * Version of {@link #newInstance(PipelineTask)} that accepts a user-supplied
     * {@link PipelineTaskOperations} instances. Allows these classes to be mocked for testing.
     *
     * @param pipelineTaskDataOperations2
     */
    static final AlgorithmExecutor newInstance(PipelineTask pipelineTask,
        PipelineTaskOperations pipelineTaskOperations,
        PipelineTaskDataOperations pipelineTaskDataOperations) {

        if (pipelineTask == null) {
            log.debug("Pipeline task is null, returning LocalAlgorithmExecutor instance");
            return new LocalAlgorithmExecutor(pipelineTask);
        }
        PipelineDefinitionNodeExecutionResources remoteParams = pipelineTaskOperations
            .executionResources(pipelineTask);

        if (remoteParams == null) {
            log.debug("Remote parameters null, returning LocalAlgorithmExecutor instance");
            return new LocalAlgorithmExecutor(pipelineTask);
        }
        if (!remoteParams.isRemoteExecutionEnabled()) {
            log.debug("Remote execution not selected, returning LocalAlgorithmExecutor instance");
            return new LocalAlgorithmExecutor(pipelineTask);
        }
        SubtaskCounts subtaskCounts = pipelineTaskDataOperations.subtaskCounts(pipelineTask);
        log.debug("Total subtasks {}", subtaskCounts.getTotalSubtaskCount());
        log.debug("Completed subtasks {}", subtaskCounts.getCompletedSubtaskCount());
        int subtasksToRun = subtaskCounts.getTotalSubtaskCount()
            - subtaskCounts.getCompletedSubtaskCount();
        if (subtasksToRun < remoteParams.getMinSubtasksForRemoteExecution()) {
            log.info("Number subtasks to run ({}) less than min subtasks for remote execution ({})",
                subtasksToRun, remoteParams.getMinSubtasksForRemoteExecution());
            log.info("Executing task {} locally", pipelineTask);
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
        writeActiveCoresFile();
        writeWallTimeFile();

        IntervalMetric.measure(PipelineMetrics.SEND_METRIC, () -> {
            log.info("Submitting task for execution (taskId={})", pipelineTask);

            Files.createDirectories(algorithmLogDir());
            Files.createDirectories(taskDataDir());
            SubtaskUtils.clearStaleAlgorithmStates(
                new TaskDirectoryManager(pipelineTask).taskDir().toFile());

            log.info("Start remote monitoring (taskId={})", pipelineTask);
            submitForExecution();
            writeQueuedTimestampFile();
            return null;
        });
    }

    private void prepareToSubmitAlgorithm(TaskConfiguration inputsHandler) {

        PipelineDefinitionNodeExecutionResources executionResources = pipelineTaskOperations()
            .executionResources(pipelineTask);
        int numSubtasks;
        // Initial submission: this is indicated by a non-null task configuration manager
        if (inputsHandler != null) { // indicates initial submission
            log.info("Processing initial submission of task {}", pipelineTask);
            numSubtasks = inputsHandler.getSubtaskCount();
            pipelineTaskDataOperations().updateSubtaskCounts(pipelineTask, numSubtasks, -1, -1);

            pbsParameters = generatePbsParameters(executionResources, numSubtasks);

            // Resubmission: this is indicated by a null task configuration manager, which
            // means that subtask counts are available in the database
        } else {
            log.info("Processing resubmission of task {}", pipelineTask);
            SubtaskCounts subtaskCounts = pipelineTaskDataOperations().subtaskCounts(pipelineTask);
            numSubtasks = subtaskCounts.getTotalSubtaskCount();
            int numCompletedSubtasks = subtaskCounts.getCompletedSubtaskCount();

            // Scale the total subtasks to get to the number that still need to be processed
            double subtaskCountScaleFactor = (double) (numSubtasks - numCompletedSubtasks)
                / (double) numSubtasks;

            // Get the current remote parameters
            pbsParameters = generatePbsParameters(executionResources,
                (int) (numSubtasks * subtaskCountScaleFactor));
        }
    }

    /** Writes the number of active cores per node to a file in the task directory. */
    private void writeActiveCoresFile() {
        writeActiveCoresFile(workingDir(), activeCores());
    }

    private void writeWallTimeFile() {
        writeWallTimeFile(workingDir(), wallTime());
    }

    protected abstract void addToMonitor();

    protected abstract void submitForExecution();

    protected abstract String activeCores();

    protected abstract String wallTime();

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

    protected void writeQueuedTimestampFile() {
        TimestampFile.create(workingDir().toFile(), TimestampFile.Event.QUEUED);
    }

    public abstract AlgorithmType algorithmType();

    protected PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    public PbsParameters getPbsParameters() {
        return pbsParameters;
    }

    protected PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    // Broken out for use in ComputeNodeMaster unit tests.
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    static void writeActiveCoresFile(Path taskDir, String activeCoresPerNode) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(taskDir.resolve(ACTIVE_CORES_FILE_NAME).toFile()),
            ZiggyFileUtils.ZIGGY_CHARSET))) {
            writer.write(activeCoresPerNode);
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Broken out for use in ComputeNodeMaster unit tests.
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    static void writeWallTimeFile(Path taskDir, String wallTime) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(taskDir.resolve(WALL_TIME_FILE_NAME).toFile()),
            ZiggyFileUtils.ZIGGY_CHARSET))) {
            writer.write(wallTime);
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
