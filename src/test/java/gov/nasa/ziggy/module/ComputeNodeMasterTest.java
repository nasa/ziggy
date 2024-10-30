package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.BuildInfo;
import gov.nasa.ziggy.util.BuildInfo.BuildType;

/**
 * Unit tests for the {@link ComputeNodeMaster} class.
 *
 * @author PT
 */
public class ComputeNodeMasterTest {

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

    private final String MODULE_NAME = "dummy";

    @Rule
    public ZiggyPropertyRule executableNameRule = new ZiggyPropertyRule(
        PropertyName.ZIGGY_ALGORITHM_NAME, MODULE_NAME);

    private final int SUBTASK_COUNT = 5;
    private final int CORES_PER_NODE = 3;
    private final long INSTANCE_ID = 10;
    private final long TASK_ID = 20;
    private final String TASK_DIR_NAME = Long.toString(INSTANCE_ID) + "-" + Long.toString(TASK_ID)
        + "-" + MODULE_NAME;
    private final long WALL_TIME_REQUEST = 1800L;

    private TaskConfiguration inputsHandler;
    private SubtaskServer subtaskServer;
    private ExecutorService subtaskMasterThreadPool;
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
        inputsHandler = mock(TaskConfiguration.class);
        subtaskServer = mock(SubtaskServer.class);
        subtaskMasterThreadPool = mock(ExecutorService.class);

        // Create the wall time and active cores files.
        AlgorithmExecutor.writeActiveCoresFile(taskDir, Integer.toString(CORES_PER_NODE));
        AlgorithmExecutor.writeWallTimeFile(taskDir, Long.toString(WALL_TIME_REQUEST));

        // Create the ComputeNodeMaster. To be precise, create an instance of the
        // class that is a Mockito spy.
        computeNodeMaster = Mockito.spy(new ComputeNodeMaster(taskDir.toString()));
        doReturn(inputsHandler).when(computeNodeMaster).getTaskConfiguration();
        doReturn(subtaskServer).when(computeNodeMaster).subtaskServer();
        doReturn(subtaskMasterThreadPool).when(computeNodeMaster).subtaskMasterThreadPool();

        // Create the version information properties file.
        new BuildInfo(BuildType.ZIGGY).writeBuildFile();
    }

    /**
     * Tests an {@link ComputeNodeMaster#initialize()} call.
     */
    @Test
    public void testInitialize()
        throws ConfigurationException, IllegalStateException, IOException, InterruptedException {

        // Initialize the instance.
        computeNodeMaster.initialize();

        // The SubtaskServer should have started
        verify(subtaskServer).start();

        // There should be 3 SubtaskMaster instances that were submitted to the ExecutorService.
        assertEquals(3, computeNodeMaster.subtaskMastersCount());
        verify(subtaskMasterThreadPool, times(3)).submit(ArgumentMatchers.any(SubtaskMaster.class),
            ArgumentMatchers.any(ThreadFactory.class));

        // There should be timestamp files in the task directory.
        assertTrue(TimestampFile.timestamp(taskDir.toFile(),
            TimestampFile.Event.ARRIVE_COMPUTE_NODES) > 0);
        assertTrue(TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.START) > 0);
    }

    /**
     * Tests an {@link ComputeNodeMaster#initialize()} call that does not update the state file.
     */
    @Test
    public void testInitializeWithoutUpdatingState()
        throws IOException, ConfigurationException, IllegalStateException, InterruptedException {

        // Initialize the instance.
        computeNodeMaster.initialize();

        // The SubtaskServer should have started
        verify(subtaskServer).start();

        // There should be 3 SubtaskMaster instances that were submitted to the ExecutorService.
        assertEquals(3, computeNodeMaster.subtaskMastersCount());
        verify(subtaskMasterThreadPool, times(3)).submit(ArgumentMatchers.any(SubtaskMaster.class),
            ArgumentMatchers.any(ThreadFactory.class));

        // There should be timestamp files in the task directory.
        assertTrue(TimestampFile.timestamp(taskDir.toFile(),
            TimestampFile.Event.ARRIVE_COMPUTE_NODES) > 0);
        assertTrue(TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.START) > 0);
    }

    @Test
    public void testFinish()
        throws ConfigurationException, IllegalStateException, IOException, InterruptedException {

        // Initialize the instance.
        computeNodeMaster.initialize();

        // If the subtasks aren't all done, then all we should get is a timestamp file.
        computeNodeMaster.finish();

        // The job finish timestamp should be present.
        assertTrue(TimestampFile.timestamp(taskDir.toFile(), TimestampFile.Event.FINISH) > 0);

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
}
