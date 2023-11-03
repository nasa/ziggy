package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.remote.TimestampFile;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.logging.TaskLog;

/**
 * Unit tests for the {@link ComputeNodeMaster} class.
 *
 * @author PT
 */
public class ComputeNodeMasterTest {

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule resultsDirRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        directoryRule);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule).around(resultsDirRule);

    private final int SUBTASK_COUNT = 5;
    private final int CORES_PER_NODE = 3;
    private final long INSTANCE_ID = 10;
    private final long TASK_ID = 20;
    private final String MODULE_NAME = "dummy";
    private final String TASK_DIR_NAME = Long.toString(INSTANCE_ID) + "-" + Long.toString(TASK_ID)
        + "-" + MODULE_NAME;

    private PipelineTask pipelineTask;
    private TaskConfigurationManager inputsHandler;
    private SubtaskServer subtaskServer;
    private ExecutorService subtaskMasterThreadPool;
    private TaskLog taskLog;
    private ComputeNodeMaster computeNodeMaster;
    private StateFile stateFile;
    private Path taskDir;
    private List<File> subtaskDirFiles;

    @Before
    public void setUp() throws Exception {

        // Construct a task directory inside the unit test's directory,
        // then construct the subtask directories below that.
        taskDir = DirectoryProperties.taskDataDir().resolve(TASK_DIR_NAME);
        Files.createDirectories(taskDir);
        subtaskDirFiles = new ArrayList<>();
        for (int subtask = 0; subtask < SUBTASK_COUNT; subtask++) {
            Path subtaskDir = taskDir.resolve("st-" + subtask);
            subtaskDirFiles.add(subtaskDir.toFile());
            Files.createDirectories(subtaskDir);
        }

        // Create and populate the state file directory.
        Files.createDirectories(DirectoryProperties.stateFilesDir());
        pipelineTask = mock(PipelineTask.class);
        when(pipelineTask.getModuleName()).thenReturn(MODULE_NAME);
        when(pipelineTask.exeTimeoutSeconds()).thenReturn(3_600_000);
        when(pipelineTask.getId()).thenReturn(TASK_ID);
        when(pipelineTask.pipelineInstanceId()).thenReturn(INSTANCE_ID);
        stateFile = StateFile.generateStateFile(pipelineTask, null, SUBTASK_COUNT);
        stateFile.setActiveCoresPerNode(CORES_PER_NODE);
        stateFile.persist();

        // Create the state file lock file
        stateFile.lockFile().createNewFile();

        // Create the algorithm log directory and the TaskLog instance
        Files.createDirectories(DirectoryProperties.algorithmLogsDir());
        taskLog = new TaskLog(
            DirectoryProperties.algorithmLogsDir().resolve(TASK_DIR_NAME + ".log").toString());

        // Create mocked instances
        inputsHandler = mock(TaskConfigurationManager.class);
        when(inputsHandler.allSubTaskDirectories()).thenReturn(subtaskDirFiles);
        subtaskServer = mock(SubtaskServer.class);
        subtaskMasterThreadPool = mock(ExecutorService.class);

        // Create the ComputeNodeMaster. To be precise, create an instance of the
        // class that is a Mockito spy.
        computeNodeMaster = Mockito.spy(new ComputeNodeMaster(taskDir.toString(), taskLog));
        doReturn(inputsHandler).when(computeNodeMaster).getInputsHandler();
        doReturn(subtaskServer).when(computeNodeMaster).subtaskServer();
        doReturn(subtaskMasterThreadPool).when(computeNodeMaster).subtaskMasterThreadPool();
        doReturn(true).when(computeNodeMaster)
            .getWriteLockWithoutBlocking(ArgumentMatchers.any(File.class));
        doNothing().when(computeNodeMaster).releaseWriteLock(ArgumentMatchers.any(File.class));
    }

    /**
     * Tests an {@link ComputeNodeMaster#initialize()} call that has to update the state file.
     */
    @Test
    public void testInitializeAndUpdateState()
        throws ConfigurationException, IllegalStateException, IOException, InterruptedException {

        // The initial state of the state file should be QUEUED.
        assertEquals(StateFile.State.QUEUED, stateFile.newStateFileFromDiskFile().getState());

        // Initialize the instance.
        computeNodeMaster.initialize();

        // The state file on disk should now be PROCESSING
        assertEquals(StateFile.State.PROCESSING, stateFile.newStateFileFromDiskFile().getState());

        // The state file in the ComputeNodeMaster should have correct counts
        assertEquals(5, computeNodeMaster.getStateFileNumTotal());
        assertEquals(0, computeNodeMaster.getStateFileNumComplete());
        assertEquals(0, computeNodeMaster.getStateFileNumFailed());

        // The SubtaskServer should have started
        verify(subtaskServer).start();

        // There should be 3 SubtaskMaster instances that were submitted to the ExecutorService.
        assertEquals(3, computeNodeMaster.subtaskMastersCount());
        verify(subtaskMasterThreadPool, times(3)).submit(ArgumentMatchers.any(SubtaskMaster.class),
            ArgumentMatchers.any(ThreadFactory.class));
        assertEquals(0, computeNodeMaster.getSemaphorePermits());

        // There should be timestamp files in the task directory.
        assertTrue(TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.ARRIVE_PFE) > 0);
        assertTrue(
            TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.PBS_JOB_START) > 0);
        assertEquals(-1L,
            TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.QUEUED_PBS));
    }

    /**
     * Tests an {@link ComputeNodeMaster#initialize()} call that does not update the state file.
     */
    @Test
    public void testInitializeWithoutUpdatingState()
        throws IOException, ConfigurationException, IllegalStateException, InterruptedException {

        doReturn(false).when(computeNodeMaster)
            .getWriteLockWithoutBlocking(ArgumentMatchers.any(File.class));

        // Initialize the instance.
        computeNodeMaster.initialize();

        // The state file on disk should still be QUEUED
        assertEquals(StateFile.State.QUEUED, stateFile.newStateFileFromDiskFile().getState());

        // The state file in the ComputeNodeMaster should have correct counts
        assertEquals(5, computeNodeMaster.getStateFileNumTotal());
        assertEquals(0, computeNodeMaster.getStateFileNumComplete());
        assertEquals(0, computeNodeMaster.getStateFileNumFailed());

        // The SubtaskServer should have started
        verify(subtaskServer).start();

        // There should be 3 SubtaskMaster instances that were submitted to the ExecutorService.
        assertEquals(3, computeNodeMaster.subtaskMastersCount());
        verify(subtaskMasterThreadPool, times(3)).submit(ArgumentMatchers.any(SubtaskMaster.class),
            ArgumentMatchers.any(ThreadFactory.class));
        assertEquals(0, computeNodeMaster.getSemaphorePermits());

        // There should be timestamp files in the task directory.
        assertTrue(TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.ARRIVE_PFE) > 0);
        assertTrue(
            TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.PBS_JOB_START) > 0);
        assertEquals(-1L,
            TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.QUEUED_PBS));
    }

    /**
     * Tests an {@link ComputeNodeMaster#initialize()} call that runs after all subtasks have
     * already been completed.
     */
    @Test
    public void testInitializeAllSubtasksDone()
        throws IOException, ConfigurationException, IllegalStateException, InterruptedException {

        // Set all subtasks to be either FAILED or COMPLETE.
        for (int subtask = 0; subtask < SUBTASK_COUNT; subtask++) {
            File subtaskDirFile = taskDir.resolve("st-" + subtask).toFile();
            AlgorithmStateFiles.SubtaskState subtaskState = subtask == 0
                ? AlgorithmStateFiles.SubtaskState.FAILED
                : AlgorithmStateFiles.SubtaskState.COMPLETE;
            new AlgorithmStateFiles(subtaskDirFile).updateCurrentState(subtaskState);
        }

        // Initialize the instance.
        computeNodeMaster.initialize();

        // The state file on disk should still be QUEUED
        assertEquals(StateFile.State.QUEUED, stateFile.newStateFileFromDiskFile().getState());

        // The SubtaskServer should not have started
        verify(subtaskServer, times(0)).start();

        // There should be no SubtaskMaster instances.
        assertEquals(0, computeNodeMaster.subtaskMastersCount());
        verify(subtaskMasterThreadPool, times(0)).submit(ArgumentMatchers.any(SubtaskMaster.class),
            ArgumentMatchers.any(ThreadFactory.class));
        assertEquals(-1, computeNodeMaster.getSemaphorePermits());
    }

    /**
     * Tests the performance of the monitoring process when there are subtasks that remain that
     * require processing.
     */
    @Test
    public void testMonitoringWhenSubtasksRemain()
        throws ConfigurationException, IllegalStateException, IOException, InterruptedException {

        when(subtaskServer.isListenerRunning()).thenReturn(true);

        // Initialize the instance.
        computeNodeMaster.initialize();

        // Run the monitoring process once.
        computeNodeMaster.run();

        // The state file in the ComputeNodeMaster should have correct counts
        assertEquals(5, computeNodeMaster.getStateFileNumTotal());
        assertEquals(0, computeNodeMaster.getStateFileNumComplete());
        assertEquals(0, computeNodeMaster.getStateFileNumFailed());

        // The countdown latch should still be waiting.
        assertEquals(1, computeNodeMaster.getCountDownLatchCount());

        // All of the semaphore permits should still be in use.
        assertEquals(0, computeNodeMaster.getSemaphorePermits());

        // Mark a subtask as complete, another as processing, another as failed.
        new AlgorithmStateFiles(taskDir.resolve("st-0").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        new AlgorithmStateFiles(taskDir.resolve("st-1").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
        new AlgorithmStateFiles(taskDir.resolve("st-2").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.PROCESSING);

        // Run the monitoring process once.
        computeNodeMaster.run();

        // The state file in the ComputeNodeMaster should have correct counts
        assertEquals(5, computeNodeMaster.getStateFileNumTotal());
        assertEquals(1, computeNodeMaster.getStateFileNumComplete());
        assertEquals(1, computeNodeMaster.getStateFileNumFailed());

        // The countdown latch should still be waiting.
        assertEquals(1, computeNodeMaster.getCountDownLatchCount());

        // All of the semaphore permits should still be in use.
        assertEquals(0, computeNodeMaster.getSemaphorePermits());

    }

    /**
     * Tests the performance of the monitoring process when the {@link SubtaskServer} listener
     * thread fails.
     */
    @Test
    public void testMonitoringWhenServerFails()
        throws ConfigurationException, IllegalStateException, IOException, InterruptedException {

        // Initialize the instance.
        computeNodeMaster.initialize();

        // Run the monitoring process once.
        computeNodeMaster.run();

        // The state file in the ComputeNodeMaster should have correct counts
        assertEquals(5, computeNodeMaster.getStateFileNumTotal());
        assertEquals(0, computeNodeMaster.getStateFileNumComplete());
        assertEquals(0, computeNodeMaster.getStateFileNumFailed());

        // The countdown latch should no longer be waiting.
        assertEquals(0, computeNodeMaster.getCountDownLatchCount());

        // All of the semaphore permits should still be in use.
        assertEquals(0, computeNodeMaster.getSemaphorePermits());

        // Mark a subtask as complete, another as processing, another as failed.
        new AlgorithmStateFiles(taskDir.resolve("st-0").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        new AlgorithmStateFiles(taskDir.resolve("st-1").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
        new AlgorithmStateFiles(taskDir.resolve("st-2").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.PROCESSING);

        // Run the monitoring process once.
        computeNodeMaster.run();

        // The state counts should not reflect the changes because the
        // monitoring system doesn't update the ComputeNodeMaster state
        // when the countdown latch has been released.
        assertEquals(5, computeNodeMaster.getStateFileNumTotal());
        assertEquals(0, computeNodeMaster.getStateFileNumComplete());
        assertEquals(0, computeNodeMaster.getStateFileNumFailed());

        // The countdown latch should no longer be waiting.
        assertEquals(0, computeNodeMaster.getCountDownLatchCount());

        // All of the semaphore permits should still be in use.
        assertEquals(0, computeNodeMaster.getSemaphorePermits());
    }

    /**
     * Tests the performance of the monitoring process when all the subtasks are completed.
     */
    @Test
    public void testMonitoringCompletedTask()
        throws ConfigurationException, IllegalStateException, IOException, InterruptedException {

        when(subtaskServer.isListenerRunning()).thenReturn(true);

        // Initialize the instance.
        computeNodeMaster.initialize();

        // Mark subtasks as complete or failed.
        new AlgorithmStateFiles(taskDir.resolve("st-0").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        new AlgorithmStateFiles(taskDir.resolve("st-1").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
        new AlgorithmStateFiles(taskDir.resolve("st-2").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        new AlgorithmStateFiles(taskDir.resolve("st-3").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
        new AlgorithmStateFiles(taskDir.resolve("st-4").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);

        // Run the monitoring process once.
        computeNodeMaster.run();

        // The countdown latch should no longer be waiting.
        assertEquals(0, computeNodeMaster.getCountDownLatchCount());

        // All of the semaphore permits should still be in use.
        assertEquals(0, computeNodeMaster.getSemaphorePermits());

        // The state of the state file should still be processing.
        assertEquals(StateFile.State.PROCESSING, stateFile.newStateFileFromDiskFile().getState());

        // The state file in the ComputeNodeMaster should have correct counts
        assertEquals(5, computeNodeMaster.getStateFileNumTotal());
        assertEquals(3, computeNodeMaster.getStateFileNumComplete());
        assertEquals(2, computeNodeMaster.getStateFileNumFailed());

    }

    /**
     * Tests the performance of the montioring process when all of the SubtaskMaster instances have
     * completed.
     *
     * @throws InterruptedException
     */
    @Test
    public void testMonitoringSubtaskMastersDone()
        throws ConfigurationException, IllegalStateException, IOException, InterruptedException {

        when(subtaskServer.isListenerRunning()).thenReturn(true);

        // Initialize the instance.
        computeNodeMaster.initialize();

        // Replace the allPermitsAvailable() method with a mockery.
        doReturn(true).when(computeNodeMaster).allPermitsAvailable();

        // Run the monitoring process once.
        computeNodeMaster.run();

        // The countdown latch should no longer be waiting.
        assertEquals(0, computeNodeMaster.getCountDownLatchCount());

    }

    @Test
    public void testFinish()
        throws ConfigurationException, IllegalStateException, IOException, InterruptedException {

        // Initialize the instance.
        computeNodeMaster.initialize();

        // If the subtasks aren't all done, then all we should get is a timestamp file.
        computeNodeMaster.finish();

        // The state file on disk should now be PROCESSING
        assertEquals(StateFile.State.PROCESSING, stateFile.newStateFileFromDiskFile().getState());

        // The job finish timestamp should be present.
        assertTrue(
            TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.PBS_JOB_FINISH) > 0);

        // Mark subtasks as complete or failed.
        new AlgorithmStateFiles(taskDir.resolve("st-0").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        new AlgorithmStateFiles(taskDir.resolve("st-1").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
        new AlgorithmStateFiles(taskDir.resolve("st-2").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        new AlgorithmStateFiles(taskDir.resolve("st-3").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
        new AlgorithmStateFiles(taskDir.resolve("st-4").toFile())
            .updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);

        // This time, the state file should be updated.
        computeNodeMaster.finish();

        assertEquals(StateFile.State.COMPLETE, stateFile.newStateFileFromDiskFile().getState());
    }
}
