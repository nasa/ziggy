package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.SubtaskServer.ResponseType;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.io.LockManager;

/**
 * Unit tests for {@link SubtaskMaster} class. Amusingly, {@link SubtaskMaster} has only one public
 * method, {@link SubtaskMaster#run()}, so all tests will execute that method with assorted
 * different conditions introduced to ensure that {@link SubtaskMaster} does the right thing.
 */
public class SubtaskMasterTest {

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule resultsDirRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        directoryRule);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule).around(resultsDirRule);

    private SubtaskMaster subtaskMaster;
    private SubtaskClient subtaskClient;
    private SubtaskExecutor subtaskExecutor;
    private Semaphore completionCounter;
    private AlgorithmStateFiles algorithmStateFiles;

    private final int THREAD_NUMBER = 5;
    private final String NODE = "dummy";
    private final String BINARY_NAME = "dummy";
    private final String TASK_DIR = "dummy";
    private final int TIMEOUT = 3_600_000;
    private final int SUBTASK_INDEX = 50;

    @Before
    public void setUp() throws IOException, InterruptedException {

        // Set up mocked instances as needed.
        subtaskClient = mock(SubtaskClient.class);
        subtaskExecutor = mock(SubtaskExecutor.class);
        algorithmStateFiles = mock(AlgorithmStateFiles.class);

        // Set up the object for test.
        completionCounter = new Semaphore(THREAD_NUMBER);
        completionCounter.acquire();
        subtaskMaster = spy(new SubtaskMaster(THREAD_NUMBER, NODE, completionCounter, BINARY_NAME,
            DirectoryProperties.taskDataDir().resolve(TASK_DIR).toString(), TIMEOUT));
        doReturn(subtaskClient).when(subtaskMaster).subtaskClient();
        doReturn(subtaskExecutor).when(subtaskMaster).subtaskExecutor(ArgumentMatchers.anyInt());
        doReturn(algorithmStateFiles).when(subtaskMaster)
            .algorithmStateFiles(ArgumentMatchers.any(File.class));
        Mockito.doNothing().when(subtaskMaster).releaseWriteLock(ArgumentMatchers.any(File.class));
    }

    @After
    public void TearDown() throws IOException {
        LockManager.releaseAllLocks();
    }

    /**
     * Test normal execution: a subtask is obtained, it is ready to be processed, its execution runs
     * without errors.
     */
    @Test
    public void testNormalExecution() throws InterruptedException, IOException {

        standardSetUp();

        // Execute the run() method.
        subtaskMaster.run();

        // Check all the executions that were supposed to happen.
        verify(subtaskExecutor).execAlgorithm();
        verify(subtaskMaster).releaseWriteLock(DirectoryProperties.taskDataDir()
            .resolve(TASK_DIR)
            .resolve("st-" + SUBTASK_INDEX)
            .resolve(TaskConfigurationManager.LOCK_FILE_NAME)
            .toFile());
        verify(subtaskMaster, times(0)).logException(ArgumentMatchers.any(Integer.class),
            ArgumentMatchers.any(Exception.class));
        assertEquals(5, completionCounter.availablePermits());
    }

    /**
     * Test an execution in which the algorithm fails.
     */
    @Test
    public void testAlgorithmFailure() throws InterruptedException, IOException {

        standardSetUp();

        // The SubtaskExecutor should have a nonzero return.
        when(subtaskExecutor.execAlgorithm()).thenReturn(1);

        // Execute the run() method.
        subtaskMaster.run();

        // Check all the executions that were supposed to happen. Notably,
        // the logException() method should run.
        verify(subtaskExecutor).execAlgorithm();
        verify(subtaskMaster).releaseWriteLock(DirectoryProperties.taskDataDir()
            .resolve(TASK_DIR)
            .resolve("st-" + SUBTASK_INDEX)
            .resolve(TaskConfigurationManager.LOCK_FILE_NAME)
            .toFile());
        verify(subtaskMaster).logException(ArgumentMatchers.eq(SUBTASK_INDEX),
            ArgumentMatchers.any(ModuleFatalProcessingException.class));
        assertEquals(5, completionCounter.availablePermits());
    }

    /**
     * Test an execution in which the subtask that is offered to the {@link SubtaskMaster} was
     * already completed.
     */
    @Test
    public void testSubtaskAlreadyComplete() throws InterruptedException, IOException {

        standardSetUp();

        // The subtask should have a prior algorithm state file, one that indicates completion.
        when(algorithmStateFiles.subtaskStateExists()).thenReturn(true);
        when(algorithmStateFiles.isComplete()).thenReturn(true);

        // Execute the run() method.
        subtaskMaster.run();

        // Check all the executions that were supposed to happen. Notably, the
        // execAlgorithm() method should not run.
        verify(subtaskExecutor, times(0)).execAlgorithm();
        verify(subtaskMaster).releaseWriteLock(DirectoryProperties.taskDataDir()
            .resolve(TASK_DIR)
            .resolve("st-" + SUBTASK_INDEX)
            .resolve(TaskConfigurationManager.LOCK_FILE_NAME)
            .toFile());
        verify(subtaskMaster, times(0)).logException(ArgumentMatchers.any(Integer.class),
            ArgumentMatchers.any(Exception.class));
        assertEquals(5, completionCounter.availablePermits());
    }

    /**
     * Tests an execution in which the subtask that is offered to the {@link SubtaskMaster} is
     * marked as failed.
     */
    @Test
    public void testSubtaskAlreadyFailed() throws InterruptedException, IOException {

        standardSetUp();

        // The subtask should have a prior algorithm state file, one that indicates completion.
        when(algorithmStateFiles.subtaskStateExists()).thenReturn(true);
        when(algorithmStateFiles.isFailed()).thenReturn(true);

        // Execute the run() method.
        subtaskMaster.run();

        // Check all the executions that were supposed to happen. Notably, the
        // execAlgorithm() method should not run.
        verify(subtaskExecutor, times(0)).execAlgorithm();
        verify(subtaskMaster).releaseWriteLock(DirectoryProperties.taskDataDir()
            .resolve(TASK_DIR)
            .resolve("st-" + SUBTASK_INDEX)
            .resolve(TaskConfigurationManager.LOCK_FILE_NAME)
            .toFile());
        verify(subtaskMaster, times(0)).logException(ArgumentMatchers.any(Integer.class),
            ArgumentMatchers.any(Exception.class));
        assertEquals(5, completionCounter.availablePermits());
    }

    /**
     * Tests an execution in which the subtask that is offered to the {@link SubtaskMaster} is
     * marked as processing.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testSubtaskAlreadyProcessing() throws InterruptedException, IOException {

        standardSetUp();

        // The subtask should have a prior algorithm state file, one that indicates completion.
        when(algorithmStateFiles.subtaskStateExists()).thenReturn(true);
        when(algorithmStateFiles.isProcessing()).thenReturn(true);

        // Execute the run() method.
        subtaskMaster.run();

        // Check all the executions that were supposed to happen. Notably, the
        // execAlgorithm() method should not run.
        verify(subtaskExecutor, times(0)).execAlgorithm();
        verify(subtaskMaster).releaseWriteLock(DirectoryProperties.taskDataDir()
            .resolve(TASK_DIR)
            .resolve("st-" + SUBTASK_INDEX)
            .resolve(TaskConfigurationManager.LOCK_FILE_NAME)
            .toFile());
        verify(subtaskMaster, times(0)).logException(ArgumentMatchers.any(Integer.class),
            ArgumentMatchers.any(Exception.class));
        assertEquals(5, completionCounter.availablePermits());
    }

    /**
     * Tests an execution in which the subtask offered to the {@link SubtaskMaster} is locked by
     * another {@link SubtaskMaster}.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testUnableToObtainFileLock() throws InterruptedException, IOException {

        standardSetUp();

        // The locking method should return false, not true.
        doReturn(false).when(subtaskMaster)
            .getWriteLockWithoutBlocking(DirectoryProperties.taskDataDir()
                .resolve(TASK_DIR)
                .resolve("st-" + SUBTASK_INDEX)
                .resolve(TaskConfigurationManager.LOCK_FILE_NAME)
                .toFile());

        // Execute the run() method.
        subtaskMaster.run();

        // Check all the executions that were supposed to happen. Notably, the
        // execAlgorithm() method should not run and the write lock should not be
        // released (since it was never obtained).
        verify(subtaskExecutor, times(0)).execAlgorithm();
        verify(subtaskMaster, times(1)).releaseWriteLock(DirectoryProperties.taskDataDir()
            .resolve(TASK_DIR)
            .resolve("st-" + SUBTASK_INDEX)
            .resolve(TaskConfigurationManager.LOCK_FILE_NAME)
            .toFile());
        verify(subtaskMaster, times(0)).logException(ArgumentMatchers.any(Integer.class),
            ArgumentMatchers.any(Exception.class));
        assertEquals(5, completionCounter.availablePermits());
    }

    /**
     * Tests an execution in which the subtask write lock is not obtained and an {@link IOException}
     * occurs.
     */
    @Test
    public void testIOException() throws InterruptedException, IOException {

        standardSetUp();

        // The locking method should cause an exception.
        doThrow(new PipelineException("dummy")).when(subtaskMaster)
            .getWriteLockWithoutBlocking(DirectoryProperties.taskDataDir()
                .resolve(TASK_DIR)
                .resolve("st-" + SUBTASK_INDEX)
                .resolve(TaskConfigurationManager.LOCK_FILE_NAME)
                .toFile());

        // Execute the run() method.
        subtaskMaster.run();

        // Check all the executions that were supposed to happen. Notably, the
        // execAlgorithm() method should not run, the write lock should not be
        // released (since it was never obtained), and the IOException should be logged.
        verify(subtaskExecutor, times(0)).execAlgorithm();
        verify(subtaskMaster, times(0)).releaseWriteLock(
            Paths.get(TASK_DIR, "st-" + SUBTASK_INDEX, TaskConfigurationManager.LOCK_FILE_NAME)
                .toFile());
        verify(subtaskMaster).logException(ArgumentMatchers.eq(SUBTASK_INDEX),
            ArgumentMatchers.any(PipelineException.class));
        assertEquals(5, completionCounter.availablePermits());
    }

    /**
     * Sets up standard execution. In standard execution, the {@link SubtaskMaster} is offered a
     * subtask, and subsequently informed that all subtasks are done; there is no algorithm state
     * file in the subtask directory; the subtask lock file is successfully obtained; and the
     * algorithm runs and returns a value of 0. This is a good starting point for most unit test
     * setups, which allows the user to first run {@link #standardSetUp()} and then to override the
     * small number of setup items that need to be changed for the given test.
     */
    private void standardSetUp() throws InterruptedException, IOException {

        // Set up the SubtaskClient to return a response with a subtask index, followed by
        // a NO_MORE response.
        when(subtaskClient.nextSubtask())
            .thenReturn(new SubtaskServer.Response(ResponseType.OK, SUBTASK_INDEX))
            .thenReturn(new SubtaskServer.Response(ResponseType.NO_MORE));

        // Mock a successful lock of the subtask lock file.
        doReturn(true).when(subtaskMaster)
            .getWriteLockWithoutBlocking(
                Paths.get(TASK_DIR, "st-" + SUBTASK_INDEX, TaskConfigurationManager.LOCK_FILE_NAME)
                    .toFile());

        // The subtask should have no prior algorithm state file.
        when(algorithmStateFiles.subtaskStateExists()).thenReturn(false);
        when(algorithmStateFiles.isProcessing()).thenReturn(false);
        when(algorithmStateFiles.isFailed()).thenReturn(false);
        when(algorithmStateFiles.isComplete()).thenReturn(false);

        // The SubtaskExecutor should return zero.
        when(subtaskExecutor.execAlgorithm()).thenReturn(0);

    }
}
