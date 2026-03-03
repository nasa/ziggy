package gov.nasa.ziggy.pipeline.step;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskMaster;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskServer;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.BuildInfo;
import gov.nasa.ziggy.util.BuildInfo.BuildType;
import gov.nasa.ziggy.util.os.MemInfo;

/**
 * Unit tests for the {@link ComputeNodeMaster} class.
 *
 * @author PT
 */
public class ComputeNodeMasterTest {

    private static final String NODE_NAME = "dummy";

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule resultsDirRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        directoryRule);

    public ZiggyPropertyRule homeDirRule = new ZiggyPropertyRule(PropertyName.ZIGGY_HOME_DIR,
        directoryRule);

    public ZiggyPropertyRule pipelineHomeDirRule = new ZiggyPropertyRule(
        PropertyName.PIPELINE_HOME_DIR, directoryRule);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(resultsDirRule)
        .around(homeDirRule)
        .around(pipelineHomeDirRule);

    @Rule
    public ZiggyPropertyRule executableNameRule = new ZiggyPropertyRule(
        PropertyName.ZIGGY_ALGORITHM_NAME, NODE_NAME);

    private final int SUBTASK_COUNT = 5;
    private final int CORES_PER_NODE = 3;
    private final long INSTANCE_ID = 10;
    private final long TASK_ID = 20;
    private final String TASK_DIR_NAME = Long.toString(INSTANCE_ID) + "-" + Long.toString(TASK_ID)
        + "-" + NODE_NAME;
    private final int WALL_TIME_REQUEST = 1800;

    private TaskConfiguration taskConfiguration;
    private SubtaskServer subtaskServer;
    private ExecutorService subtaskMasterThreadPool;
    private ScheduledThreadPoolExecutor memoryMonitorExecutor;
    private ComputeNodeMaster computeNodeMaster;
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

        // Create the algorithm log directory and the TaskLog instance
        Files.createDirectories(DirectoryProperties.algorithmLogsDir());

        // Create mocked instances
        taskConfiguration = mock(TaskConfiguration.class);
        subtaskServer = mock(SubtaskServer.class);
        subtaskMasterThreadPool = mock(ExecutorService.class);
        memoryMonitorExecutor = mock(ScheduledThreadPoolExecutor.class);

        // Create the ComputeNodeMaster. To be precise, create an instance of the
        // class that is a Mockito spy.
        computeNodeMaster = Mockito.spy(new ComputeNodeMaster(taskDir.toString()));
        doReturn(taskConfiguration).when(computeNodeMaster).getTaskConfiguration();
        doReturn(subtaskServer).when(computeNodeMaster).subtaskServer();
        doReturn(subtaskMasterThreadPool).when(computeNodeMaster).subtaskMasterThreadPool();
        doReturn("1234567").when(computeNodeMaster).getJobId();
        doReturn(true).when(computeNodeMaster).isBatchSystem();
        doReturn(memoryMonitorExecutor).when(computeNodeMaster).memoryMonitorExecutor();
        Mockito.when(taskConfiguration.getRequestedTimeSeconds()).thenReturn(WALL_TIME_REQUEST);
        Mockito.when(taskConfiguration.getActiveCores()).thenReturn(CORES_PER_NODE);

        // Create the version information properties file.
        new BuildInfo(BuildType.ZIGGY).writeBuildFile();
    }

    /**
     * Tests an {@link ComputeNodeMaster#initialize()} call.
     */
    @Test
    public void testInitializeWithBatchSystem() {
        testInitialize();
        // The memory monitoring executor should be constructed.
        verify(computeNodeMaster).memoryMonitorExecutor();
    }

    @Test
    public void testInitializeNoBatchSystem() {
        doReturn(false).when(computeNodeMaster).isBatchSystem();
        testInitialize();
        // The memory monitoring executor should NOT be constructed.
        verify(computeNodeMaster, times(0)).memoryMonitorExecutor();
    }

    // Do all initialize() tests except for checking the memory monitor startup.
    private void testInitialize() {

        // Initialize the instance.
        computeNodeMaster.initialize();

        // The SubtaskServer should have started
        verify(subtaskServer).start();

        // There should be 3 SubtaskMaster instances that were submitted to the ExecutorService.
        assertEquals(3, computeNodeMaster.subtaskMastersCount());
        verify(subtaskMasterThreadPool, times(3)).submit(ArgumentMatchers.any(SubtaskMaster.class),
            ArgumentMatchers.any(ThreadFactory.class));

        // There should be timestamp files in the task directory.
        assertTrue(
            TimestampFile.exists(taskDir.toFile(), TimestampFile.Event.ARRIVE_COMPUTE_NODES));
        assertTrue(TimestampFile.exists(taskDir.toFile(), TimestampFile.Event.START));
    }

    /**
     * Tests an {@link ComputeNodeMaster#initialize()} call that does not update the state file.
     */
    @Test
    public void testInitializeWithoutUpdatingState() {

        // Initialize the instance.
        computeNodeMaster.initialize();

        // The SubtaskServer should have started
        verify(subtaskServer).start();

        // There should be 3 SubtaskMaster instances that were submitted to the ExecutorService.
        assertEquals(3, computeNodeMaster.subtaskMastersCount());
        verify(subtaskMasterThreadPool, times(3)).submit(ArgumentMatchers.any(SubtaskMaster.class),
            ArgumentMatchers.any(ThreadFactory.class));

        // There should be timestamp files in the task directory.
        assertTrue(
            TimestampFile.exists(taskDir.toFile(), TimestampFile.Event.ARRIVE_COMPUTE_NODES));
        assertTrue(TimestampFile.exists(taskDir.toFile(), TimestampFile.Event.START));
    }

    @Test
    public void testFinish() {

        // Initialize the instance.
        computeNodeMaster.initialize();

        // If the subtasks aren't all done, then all we should get is a timestamp file.
        computeNodeMaster.finish();

        // The job finish timestamp should be present.
        assertTrue(TimestampFile.exists(taskDir.toFile(), TimestampFile.Event.FINISH));

        // Mark subtasks as complete or failed.
        new AlgorithmStateFiles(taskDir.resolve("st-0").toFile())
            .updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        new AlgorithmStateFiles(taskDir.resolve("st-1").toFile())
            .updateCurrentState(AlgorithmStateFiles.AlgorithmState.FAILED);
        new AlgorithmStateFiles(taskDir.resolve("st-2").toFile())
            .updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        new AlgorithmStateFiles(taskDir.resolve("st-3").toFile())
            .updateCurrentState(AlgorithmStateFiles.AlgorithmState.FAILED);
        new AlgorithmStateFiles(taskDir.resolve("st-4").toFile())
            .updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
    }

    @Test
    public void testCheckMemory() {
        MemInfo memInfo = mock(MemInfo.class);
        doReturn(memInfo).when(computeNodeMaster).memInfo();
        Mockito.when(memInfo.getTotalMemoryKB()).thenReturn(100_000_000L);
        String memoryWarningFileName = ComputeNodeMaster.FREE_MEMORY_WARNING_FILE_NAME_PREFIX
            + "1234567";

        // No warning when we're below the threshold for the warning.
        Mockito.when(memInfo.getFreeMemoryKB()).thenReturn(90_000_000L);
        computeNodeMaster.checkMemory();
        verify(computeNodeMaster, times(0)).issueMemoryWarning(ArgumentMatchers.anyLong(),
            ArgumentMatchers.anyLong());
        assertFalse(Files.exists(taskDir.resolve(memoryWarningFileName)));

        // Warning is issued, and status file created, when we dip below the threshold.
        Mockito.when(memInfo.getFreeMemoryKB()).thenReturn(4_999_999L);
        computeNodeMaster.checkMemory();
        verify(computeNodeMaster).issueMemoryWarning(4_999_999L, 100_000_000L);
        assertTrue(Files.exists(taskDir.resolve(memoryWarningFileName)));

        // When we're still below the threshold, we nonetheless do not issue an additional warning.
        Mockito.when(memInfo.getFreeMemoryKB()).thenReturn(4_999_998L);
        computeNodeMaster.checkMemory();
        verify(computeNodeMaster, times(1)).issueMemoryWarning(ArgumentMatchers.anyLong(),
            ArgumentMatchers.anyLong());
        assertTrue(Files.exists(taskDir.resolve(memoryWarningFileName)));

        // When we rise above the threshold, the status file is still present.
        Mockito.when(memInfo.getFreeMemoryKB()).thenReturn(90_000_000L);
        computeNodeMaster.checkMemory();
        verify(computeNodeMaster, times(1)).issueMemoryWarning(ArgumentMatchers.anyLong(),
            ArgumentMatchers.anyLong());
        assertTrue(Files.exists(taskDir.resolve(memoryWarningFileName)));
    }
}
