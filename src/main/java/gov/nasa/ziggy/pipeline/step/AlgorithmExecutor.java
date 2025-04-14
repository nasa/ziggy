package gov.nasa.ziggy.pipeline.step;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.remote.RemoteAlgorithmExecutor;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskUtils;
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

    public static final String NODE_MASTER_NAME = "compute-node-master";
    public static final String ACTIVE_CORES_FILE_NAME = ".activeCoresPerNode";
    public static final String WALL_TIME_FILE_NAME = ".requestedWallTimeSeconds";

    protected final PipelineTask pipelineTask;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    public AlgorithmExecutor(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

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
        PipelineNodeExecutionResources remoteParams = pipelineTaskOperations
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
        return new RemoteAlgorithmExecutor(pipelineTask);
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

        if (inputsHandler != null) { // indicates initial submission
            log.info("Processing initial submission of task {}", pipelineTask);
            int numSubtasks = inputsHandler.getSubtaskCount();
            pipelineTaskDataOperations().updateSubtaskCounts(pipelineTask, numSubtasks, -1, -1);
        }

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
            pipelineTaskDataOperations().incrementTaskLogIndex(pipelineTask);
            return null;
        });
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

    protected PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
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
