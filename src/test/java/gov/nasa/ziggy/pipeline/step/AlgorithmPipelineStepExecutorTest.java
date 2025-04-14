package gov.nasa.ziggy.pipeline.step;

import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HALT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager.InputFiles;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineOutputsSample;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerOperations;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.TimestampFile.Event;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.util.PipelineException;

/**
 * Unit test class for {@link AlgorithmPipelineStepExecutor}.
 *
 * @author PT
 */
public class AlgorithmPipelineStepExecutorTest {

    private PipelineTask pipelineTask;
    private PipelineInstance pipelineInstance;
    private TaskConfiguration taskConfiguration;
    private AlgorithmLifecycleManager taskAlgorithmLifecycle;
    private File taskDir;
    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineStep pipelineStep;
    private AlgorithmExecutor algorithmExecutor;
    private AlgorithmPipelineStepExecutor pipelineStepExecutor;
    private TaskDirectoryManager taskDirManager;
    private DatastoreFileManager datastoreFileManager;
    private DatastoreProducerConsumerOperations datastoreProducerConsumerOperations;
    private PipelineTaskOperations pipelineTaskOperations;
    private PipelineTaskDataOperations pipelineTaskDataOperations;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, "/dev/null");
    @Rule
    public ZiggyPropertyRule pipelineResultsRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        "/dev/null");

    @Rule
    public ZiggyPropertyRule piProcessingHaltStepPropertyRule = new ZiggyPropertyRule(PIPELINE_HALT,
        (String) null);

    @Rule
    public ZiggyPropertyRule piWorkerAllowPartialTasksPropertyRule = new ZiggyPropertyRule(
        PropertyName.ALLOW_PARTIAL_TASKS, (String) null);

    @Before
    public void setup() {
        pipelineTask = mock(PipelineTask.class);
        pipelineInstance = mock(PipelineInstance.class);
        pipelineInstanceNode = mock(PipelineInstanceNode.class);
        pipelineStep = mock(PipelineStep.class);
        algorithmExecutor = mock(AlgorithmExecutor.class);
        when(pipelineTask.getId()).thenReturn(100L);
        pipelineTaskOperations = Mockito.mock(PipelineTaskOperations.class);
        when(pipelineTaskOperations.pipelineInstance(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineInstance);
        when(pipelineTaskOperations.pipelineInstanceNode(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineInstanceNode);
        when(pipelineTaskOperations.pipelineStep(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineStep);
        pipelineTaskDataOperations = Mockito.mock(PipelineTaskDataOperations.class);

        when(pipelineTask.taskBaseName()).thenReturn("50-100-test");
        when(pipelineStep.getInputsClass())
            .thenReturn(new ClassWrapper<>(PipelineInputsSample.class));
        when(pipelineStep.getOutputsClass())
            .thenReturn(new ClassWrapper<>(PipelineOutputsSample.class));
        when(pipelineInstance.getId()).thenReturn(50L);
        when(pipelineTask.getPipelineInstanceId()).thenReturn(50L);
        datastoreProducerConsumerOperations = mock(DatastoreProducerConsumerOperations.class);
        taskConfiguration = mock(TaskConfiguration.class);
        taskDir = directoryRule.directory().toFile();
        taskDir.mkdirs();
        taskAlgorithmLifecycle = mock(AlgorithmLifecycleManager.class);
        when(taskAlgorithmLifecycle.getTaskDir(true)).thenReturn(taskDir);
        when(taskAlgorithmLifecycle.getTaskDir(false)).thenReturn(taskDir);
        when(taskAlgorithmLifecycle.getExecutor()).thenReturn(algorithmExecutor);
        taskDirManager = mock(TaskDirectoryManager.class);
        when(taskDirManager.taskDir()).thenReturn(directoryRule.directory());

        datastoreFileManager = mock(DatastoreFileManager.class);
        when(datastoreFileManager.inputFilesByOutputStatus())
            .thenReturn(new InputFiles(new HashSet<>(), new HashSet<>()));

        configurePipelineStepExecutor(RunMode.STANDARD);

        // By default, mock 5 subtasks.
        when(taskConfiguration.getSubtaskCount()).thenReturn(5);
    }

    /** Sets up a pipeline step executor with a specified run mode. */
    private void configurePipelineStepExecutor(RunMode runMode) {
        pipelineStepExecutor = spy(new AlgorithmPipelineStepExecutor(pipelineTask, runMode));
        doReturn(datastoreProducerConsumerOperations).when(pipelineStepExecutor)
            .datastoreProducerConsumerOperations();
        doReturn(pipelineTaskOperations).when(pipelineStepExecutor).pipelineTaskOperations();
        doReturn(pipelineTaskDataOperations).when(pipelineStepExecutor)
            .pipelineTaskDataOperations();
        doReturn(pipelineTask).when(pipelineStepExecutor).pipelineTask();
        doReturn(taskConfiguration).when(pipelineStepExecutor).taskConfiguration();
        doReturn(datastoreFileManager).when(pipelineStepExecutor).datastoreFileManager();
        doReturn(new HashSet<>()).when(pipelineStepExecutor)
            .datastorePathsToRelative(ArgumentMatchers.anySet());
        doReturn(new HashSet<>()).when(pipelineStepExecutor)
            .datastorePathsToNames(ArgumentMatchers.anySet());
        doReturn(taskAlgorithmLifecycle).when(pipelineStepExecutor).algorithmManager();
        doReturn(taskDirManager).when(pipelineStepExecutor).taskDirManager();

        // Return the database processing steps in the correct order.
        configureDatabaseProcessingSteps();
    }

    @After
    public void teardown() throws IOException {
        DatabaseService.reset();
    }

    /**
     * Tests the ability to determine the next processing step given a current processing step.
     */
    @Test
    public void testNextProcessingStep() {

        ProcessingStep processingStep = pipelineStepExecutor.processingSteps().get(0);
        assertEquals(ProcessingStep.MARSHALING, processingStep);
        processingStep = pipelineStepExecutor.nextProcessingStep(processingStep);
        assertEquals(ProcessingStep.SUBMITTING, processingStep);
        processingStep = pipelineStepExecutor.nextProcessingStep(processingStep);
        assertEquals(ProcessingStep.QUEUED, processingStep);
        processingStep = pipelineStepExecutor.nextProcessingStep(processingStep);
        assertEquals(ProcessingStep.EXECUTING, processingStep);
        processingStep = pipelineStepExecutor.nextProcessingStep(processingStep);
        assertEquals(ProcessingStep.WAITING_TO_STORE, processingStep);
        processingStep = pipelineStepExecutor.nextProcessingStep(processingStep);
        assertEquals(ProcessingStep.STORING, processingStep);
    }

    /**
     * Tests that attempting to get the next processing step after the final one will throw the
     * correct exception.
     */
    @Test(expected = PipelineException.class)
    public void testExceptionFinalStep() {
        pipelineStepExecutor.nextProcessingStep(ProcessingStep.COMPLETE);
    }

    /**
     * Tests the AlgorithmPipelineStepExecutor constructor.
     */
    @Test
    public void testConstructor() {
        assertEquals(100L, pipelineStepExecutor.pipelineTask().getId().longValue());
        assertEquals(50L, pipelineStepExecutor.instanceId());
        assertNotNull(pipelineStepExecutor.algorithmManager());
        assertTrue(pipelineStepExecutor.pipelineInputs() instanceof PipelineInputsSample);
        assertTrue(pipelineStepExecutor.pipelineOutputs() instanceof PipelineOutputsSample);
    }

    /**
     * Tests that the method that processes a task in MARSHALING step performs correctly for local
     * processing tasks.
     */
    @Test
    public void testProcessMarshalingLocal() throws Exception {

        doReturn(ProcessingStep.MARSHALING).when(pipelineStepExecutor).currentProcessingStep();
        pipelineStepExecutor.marshalingTaskAction();

        verify(pipelineStepExecutor).copyFilesToTaskDirectory(eq(taskConfiguration), eq(taskDir));
        verify(taskConfiguration).serialize(eq(taskDir));
        verify(pipelineStepExecutor).incrementProcessingStep();
        assertFalse(pipelineStepExecutor.getDoneLooping());
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
    }

    /**
     * Tests that the method that processes a task in MARSHALING step performs correctly when no
     * inputs are generated.
     */
    @Test
    public void testProcessMarshalingNoInputs() throws Exception {

        doReturn(ProcessingStep.MARSHALING).when(pipelineStepExecutor).currentProcessingStep();
        when(taskConfiguration.getSubtaskCount()).thenReturn(0);
        pipelineStepExecutor.marshalingTaskAction();

        verify(pipelineStepExecutor).copyFilesToTaskDirectory(eq(taskConfiguration), eq(taskDir));
        verify(taskConfiguration, never()).serialize(eq(taskDir));
        verify(pipelineStepExecutor, never()).incrementProcessingStep();
        assertTrue(pipelineStepExecutor.getDoneLooping());
        assertTrue(pipelineStepExecutor.isProcessingSuccessful());
    }

    /**
     * Tests that the method that processes a task in EXECUTING step performs the correct actions.
     */
    @Test
    public void testProcessAlgorithmExecuting() {

        // remote processing
        doReturn(ProcessingStep.EXECUTING).when(pipelineStepExecutor).currentProcessingStep();
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(true);
        pipelineStepExecutor.executingTaskAction();

        verify(taskAlgorithmLifecycle).executeAlgorithm(null);
        verify(pipelineStepExecutor, never()).incrementProcessingStep();
        assertTrue(pipelineStepExecutor.getDoneLooping());
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());

        // local execution
        configurePipelineStepExecutor(RunMode.STANDARD);
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(false);
        pipelineStepExecutor.executingTaskAction();

        verify(taskAlgorithmLifecycle, times(2)).executeAlgorithm(null);
        verify(pipelineStepExecutor, never()).incrementProcessingStep();
        assertTrue(pipelineStepExecutor.getDoneLooping());
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
    }

    /**
     * Tests that the method that processes a task in WAITING_TO_STORE step performs the correct
     * actions.
     */
    @Test
    public void testProcessAlgorithmCompleted() {

        doReturn(ProcessingStep.WAITING_TO_STORE).when(pipelineStepExecutor)
            .currentProcessingStep();
        pipelineStepExecutor.waitingToStoreTaskAction();
        verify(pipelineStepExecutor).incrementProcessingStep();

        when(taskAlgorithmLifecycle.isRemote()).thenReturn(true);
        pipelineStepExecutor.waitingToStoreTaskAction();
        verify(pipelineStepExecutor, times(2)).incrementProcessingStep();
    }

    /**
     * Tests that the method that processes a task in STORING step performs the correct actions.
     */
    @Test
    public void testProcessStoring() {

        doReturn(ProcessingStep.STORING).when(pipelineStepExecutor).currentProcessingStep();
        doReturn(0L).when(pipelineStepExecutor)
            .timestampFileTimestamp(ArgumentMatchers.any(Event.class));
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(true);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineStepExecutor)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(failureSummary).when(pipelineStepExecutor).processingFailureSummary();

        // the local version performs relatively limited activities
        pipelineStepExecutor.storingTaskAction();
        verify(pipelineStepExecutor, never()).timestampFileElapsedTimeMillis(any(Event.class),
            any(Event.class));
        verify(pipelineStepExecutor, never()).timestampFileTimestamp(any(Event.class));
        verify(pipelineStepExecutor, never()).valueMetricAddValue(any(String.class),
            any(long.class));
        verify(pipelineStepExecutor).processingFailureSummary();
        verify(pipelineStepExecutor).persistResultsAndUpdateConsumers();

        // the remote version does somewhat more
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(true);
        pipelineStepExecutor.storingTaskAction();
        verify(pipelineStepExecutor, times(3)).timestampFileElapsedTimeMillis(any(Event.class),
            any(Event.class));
        verify(pipelineStepExecutor).timestampFileTimestamp(any(Event.class));
        verify(pipelineStepExecutor, times(4)).valueMetricAddValue(any(String.class),
            any(long.class));
        verify(pipelineStepExecutor, times(2)).processingFailureSummary();
        verify(pipelineStepExecutor, times(2)).persistResultsAndUpdateConsumers();
    }

    /**
     * Tests that the method that processes a task in STORING step throws the correct exception in
     * the event of an error.
     */
    @Test(expected = PipelineException.class)
    public void testProcessStoringError() {

        doReturn(ProcessingStep.STORING).when(pipelineStepExecutor).currentProcessingStep();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(true);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineStepExecutor)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineStepExecutor).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineStepExecutor).processingFailureSummary();
        doThrow(IllegalStateException.class).when(pipelineStepExecutor)
            .persistResultsAndUpdateConsumers();
        pipelineStepExecutor.storingTaskAction();
    }

    /**
     * Tests that the storing process will continue even if partial failures occur.
     */
    @Test
    public void testPartialFailureStoreResults() {

        doReturn(ProcessingStep.STORING).when(pipelineStepExecutor).currentProcessingStep();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(false);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineStepExecutor)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineStepExecutor).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineStepExecutor).processingFailureSummary();
        pipelineStepExecutor.storingTaskAction();
        verify(pipelineStepExecutor).persistResultsAndUpdateConsumers();
    }

    /**
     * Tests that the storing process will halt if partial failures occur and the configuration is
     * set up to reject same.
     */
    @Test(expected = PipelineException.class)
    public void testPartialFailureThrowException() {

        piWorkerAllowPartialTasksPropertyRule.setValue("false");
        doReturn(ProcessingStep.STORING).when(pipelineStepExecutor).currentProcessingStep();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(false);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineStepExecutor)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineStepExecutor).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineStepExecutor).processingFailureSummary();
        pipelineStepExecutor.storingTaskAction();
    }

    /**
     * Tests that the storing process will halt if all sub tasks have failed.
     */
    @Test(expected = PipelineException.class)
    public void testTotalFailureThrowsException() {

        doReturn(ProcessingStep.STORING).when(pipelineStepExecutor).currentProcessingStep();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(false);
        when(failureSummary.isAllTasksFailed()).thenReturn(true);
        doReturn(0L).when(pipelineStepExecutor)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineStepExecutor).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineStepExecutor).processingFailureSummary();
        pipelineStepExecutor.storingTaskAction();
    }

    /**
     * Tests the main processing loop in the local processing use case, and in the case in which the
     * marshaling successfully produces task directories.
     */
    @Test
    public void testProcessingMainLoopLocalTask1() {

        when(taskConfiguration.getSubtaskCount()).thenReturn(5);

        // setup mocking
        mockForLoopTest(true, false);

        // do the loop method
        pipelineStepExecutor.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor).marshalingTaskAction();
        verify(pipelineStepExecutor).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        assertEquals(ProcessingStep.SUBMITTING, pipelineStepExecutor.currentProcessingStep());
    }

    private void configureDatabaseProcessingSteps() {
        // Note that currentProcessingStep() is called twice during normal operations.
        doReturn(ProcessingStep.MARSHALING, ProcessingStep.MARSHALING, ProcessingStep.SUBMITTING,
            ProcessingStep.SUBMITTING, ProcessingStep.QUEUED, ProcessingStep.QUEUED,
            ProcessingStep.EXECUTING, ProcessingStep.EXECUTING, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.WAITING_TO_STORE, ProcessingStep.STORING, ProcessingStep.STORING,
            ProcessingStep.COMPLETE, ProcessingStep.COMPLETE).when(pipelineStepExecutor)
                .currentProcessingStep();
    }

    /**
     * Tests the main processing loop in the local processing use case, and in the case in which the
     * worker enters at COMPLETE.
     */
    @Test
    public void testProcessingMainLoopLocalTask2() {

        doReturn(ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING, ProcessingStep.COMPLETE,
            ProcessingStep.COMPLETE).when(pipelineStepExecutor).currentProcessingStep();

        // setup mocking
        mockForLoopTest(true, false);

        // do the loop method
        pipelineStepExecutor.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertTrue(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor).waitingToStoreTaskAction();
        verify(pipelineStepExecutor).storingTaskAction();
        assertEquals(ProcessingStep.STORING, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Tests the main processing loop in the remote execution case, first part of processing. In
     * this case execution returns once the task is queued to run on the remote server.
     */
    @Test
    public void testProcessingMainLoopRemote1() {

        // setup mocking
        mockForLoopTest(true, true);

        // do the loop method
        pipelineStepExecutor.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor, times(3)).currentProcessingStep();
        verify(pipelineStepExecutor, times(1)).incrementProcessingStep();
        verify(pipelineStepExecutor).marshalingTaskAction();
        verify(pipelineStepExecutor).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        assertEquals(ProcessingStep.SUBMITTING, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Tests the main processing loop in the remote execution case, second part of processing. In
     * this case execution starts at AC and goes to completion.
     */
    @Test
    public void testProcessingMainLoopRemote2() {

        doReturn(ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING, ProcessingStep.COMPLETE,
            ProcessingStep.COMPLETE).when(pipelineStepExecutor).currentProcessingStep();

        // setup mocking
        mockForLoopTest(true, true);

        // do the loop method
        pipelineStepExecutor.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertTrue(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor, times(3)).currentProcessingStep();
        verify(pipelineStepExecutor, times(1)).incrementProcessingStep();
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor).waitingToStoreTaskAction();
        verify(pipelineStepExecutor).storingTaskAction();
        assertEquals(ProcessingStep.STORING, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Tests the main processing loop in restart case, specifically makes sure that if the loop is
     * called with processing at some arbitrary point it does the right things.
     */
    @Test
    public void testProcessingMainLoopRestart1() {

        // Note that currentProcessingStep() is called twice during normal operations.
        doReturn(ProcessingStep.EXECUTING, ProcessingStep.EXECUTING,
            ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING, ProcessingStep.COMPLETE,
            ProcessingStep.COMPLETE).when(pipelineStepExecutor).currentProcessingStep();

        // setup mocking
        mockForLoopTest(true, false);

        // do the loop method
        pipelineStepExecutor.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor, times(1)).currentProcessingStep();
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        assertEquals(ProcessingStep.EXECUTING, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Tests the main processing loop in restart case, specifically makes sure that if the loop is
     * called with one of the remote steps (queued or executing), it does the right thing
     */
    @Test
    public void testProcessingMainLoopRestart2() {

        // Note that currentProcessingStep() is called twice during normal operations.
        doReturn(ProcessingStep.QUEUED, ProcessingStep.QUEUED, ProcessingStep.EXECUTING,
            ProcessingStep.EXECUTING, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.WAITING_TO_STORE, ProcessingStep.STORING, ProcessingStep.STORING,
            ProcessingStep.COMPLETE, ProcessingStep.COMPLETE).when(pipelineStepExecutor)
                .currentProcessingStep();

        // setup mocking
        mockForLoopTest(true, true);

        // do the loop method
        pipelineStepExecutor.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor, times(1)).currentProcessingStep();
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        assertEquals(ProcessingStep.QUEUED, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Tests the main processing loop in the case in which marshaling results in no task directories
     */
    @Test
    public void testProcessingMainLoopNoTaskDirs() {

        when(taskConfiguration.getSubtaskCount()).thenReturn(0);

        // setup mocking
        mockForLoopTest(false, false);

        // do the loop method
        pipelineStepExecutor.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertTrue(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        verify(pipelineStepExecutor, times(1)).currentProcessingStep();
        verify(pipelineStepExecutor, never()).incrementProcessingStep();
    }

    /**
     * Tests that processing halts at the end of MARSHALING when required.
     */
    @Test
    public void testHaltMarshaling() {

        piProcessingHaltStepPropertyRule.setValue(ProcessingStep.MARSHALING.toString());

        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineStepExecutor.processingMainLoop());
        assertEquals("Halting processing at end of step MARSHALING due to setting of "
            + PropertyName.PIPELINE_HALT.property(), exception.getMessage());
        verify(pipelineStepExecutor).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        assertEquals(ProcessingStep.MARSHALING, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Tests that processing halts at the end of WAITING when required.
     */
    @Test
    public void testHaltWaiting() {

        doReturn(ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING, ProcessingStep.COMPLETE,
            ProcessingStep.COMPLETE).when(pipelineStepExecutor).currentProcessingStep();

        piProcessingHaltStepPropertyRule.setValue(ProcessingStep.WAITING_TO_STORE.toString());

        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineStepExecutor.processingMainLoop());
        assertEquals("Halting processing at end of step WAITING_TO_STORE due to setting of "
            + PropertyName.PIPELINE_HALT.property(), exception.getMessage());
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        assertEquals(ProcessingStep.WAITING_TO_STORE, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Tests that processing halts at the end of STORING when required.
     */
    @Test
    public void testHaltStoring() {

        doReturn(ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING, ProcessingStep.COMPLETE,
            ProcessingStep.COMPLETE).when(pipelineStepExecutor).currentProcessingStep();

        piProcessingHaltStepPropertyRule.setValue(ProcessingStep.STORING.toString());

        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineStepExecutor.processingMainLoop());
        assertEquals("Unable to persist due to subtask failures", exception.getMessage());
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor).waitingToStoreTaskAction();
        verify(pipelineStepExecutor).storingTaskAction();
        assertEquals(ProcessingStep.STORING, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Tests that processing halts at the end of SUBMITTING when required.
     */
    @Test
    public void testHaltSubmitting() {

        piProcessingHaltStepPropertyRule.setValue(ProcessingStep.SUBMITTING.toString());

        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineStepExecutor.processingMainLoop());
        assertEquals("Halting processing at end of step SUBMITTING due to setting of "
            + PropertyName.PIPELINE_HALT.property(), exception.getMessage());
        verify(pipelineStepExecutor).marshalingTaskAction();
        verify(pipelineStepExecutor).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        assertEquals(ProcessingStep.SUBMITTING, pipelineStepExecutor.currentProcessingStep());
    }

    /**
     * Set up mocking for tests of the processingMainLoop() method.
     */
    private void mockForLoopTest(boolean successfulMarshaling, boolean remote) {

        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(true);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineStepExecutor)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineStepExecutor).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineStepExecutor).processingFailureSummary();

        // mock the algorithm lifecycle manager's isRemote() call
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(remote);
    }

    /** Tests restart from beginning. */
    @Test
    public void testRestartFromBeginning() {
        configurePipelineStepExecutor(RunMode.RESTART_FROM_BEGINNING);
        pipelineStepExecutor.processTask();
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor).processingMainLoop();
        verify(pipelineStepExecutor).marshalingTaskAction();
        verify(pipelineStepExecutor).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        verify(pipelineTaskDataOperations).updateProcessingStep(eq(pipelineTask),
            eq(ProcessingStep.SUBMITTING));
    }

    /** Tests a resubmit for a local-execution task. */
    @Test
    public void testResubmitLocalTask() {
        configurePipelineStepExecutor(RunMode.RESUBMIT);
        doReturn(ProcessingStep.SUBMITTING, ProcessingStep.SUBMITTING, ProcessingStep.QUEUED,
            ProcessingStep.QUEUED, ProcessingStep.EXECUTING, ProcessingStep.EXECUTING,
            ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING).when(pipelineStepExecutor)
                .currentProcessingStep();
        pipelineStepExecutor.processTask();
        verify(pipelineTaskDataOperations).updateProcessingStep(eq(pipelineTask),
            eq(ProcessingStep.SUBMITTING));
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor).processingMainLoop();
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        verify(pipelineTaskDataOperations).updateProcessingStep(eq(pipelineTask),
            any(ProcessingStep.class));
    }

    /** Tests a resubmit for a remote execution task. */
    @Test
    public void testResubmitRemoteTask() {
        configurePipelineStepExecutor(RunMode.RESUBMIT);
        doReturn(ProcessingStep.SUBMITTING, ProcessingStep.SUBMITTING, ProcessingStep.QUEUED,
            ProcessingStep.QUEUED, ProcessingStep.EXECUTING, ProcessingStep.EXECUTING,
            ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING).when(pipelineStepExecutor)
                .currentProcessingStep();
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(true);
        pipelineStepExecutor.processTask();
        verify(pipelineTaskDataOperations).updateProcessingStep(eq(pipelineTask),
            eq(ProcessingStep.SUBMITTING));
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor).processingMainLoop();
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
    }

    /** Test resumption of the marshaling step. */
    @Test
    public void testResumeMarshaling() {

        // resume current step
        configurePipelineStepExecutor(RunMode.RESUME_CURRENT_STEP);
        doReturn(ProcessingStep.MARSHALING, ProcessingStep.MARSHALING, ProcessingStep.SUBMITTING,
            ProcessingStep.SUBMITTING, ProcessingStep.QUEUED, ProcessingStep.QUEUED,
            ProcessingStep.EXECUTING, ProcessingStep.EXECUTING, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.WAITING_TO_STORE, ProcessingStep.STORING, ProcessingStep.STORING)
                .when(pipelineStepExecutor)
                .currentProcessingStep();
        pipelineStepExecutor.processTask();
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor).processingMainLoop();
        verify(pipelineStepExecutor).marshalingTaskAction();
        verify(pipelineStepExecutor).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        verify(pipelineTaskDataOperations).updateProcessingStep(eq(pipelineTask),
            eq(ProcessingStep.SUBMITTING));
    }

    /** Test resumption of algorithm execution. */
    @Test
    public void testResumeAlgorithmExecuting() {
        configurePipelineStepExecutor(RunMode.RESUME_CURRENT_STEP);
        doReturn(ProcessingStep.EXECUTING, ProcessingStep.EXECUTING,
            ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING).when(pipelineStepExecutor)
                .currentProcessingStep();
        pipelineStepExecutor.processTask();
        assertFalse(pipelineStepExecutor.isProcessingSuccessful());
        verify(pipelineStepExecutor).processingMainLoop();
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor).executingTaskAction();
        verify(pipelineStepExecutor, never()).waitingToStoreTaskAction();
        verify(pipelineStepExecutor, never()).storingTaskAction();
        verify(pipelineTaskDataOperations, never()).updateProcessingStep(eq(pipelineTask),
            eq(ProcessingStep.EXECUTING));
    }

    @Test
    public void testResumeAlgorithmComplete() {
        mockForLoopTest(false, true);
        configurePipelineStepExecutor(RunMode.RESUME_CURRENT_STEP);
        doReturn(ProcessingStep.WAITING_TO_STORE, ProcessingStep.WAITING_TO_STORE,
            ProcessingStep.STORING, ProcessingStep.STORING).when(pipelineStepExecutor)
                .currentProcessingStep();
        Mockito.doAnswer(invocation -> {
            pipelineStepExecutor.setDoneLooping(true);
            return null;
        }).when(pipelineStepExecutor).storingTaskAction();
        pipelineStepExecutor.processTask();
        verify(pipelineStepExecutor).processingMainLoop();
        verify(pipelineStepExecutor, never()).marshalingTaskAction();
        verify(pipelineStepExecutor, never()).submittingTaskAction();
        verify(pipelineStepExecutor, never()).queuedTaskAction();
        verify(pipelineStepExecutor, never()).executingTaskAction();
        verify(pipelineStepExecutor).waitingToStoreTaskAction();
        verify(pipelineStepExecutor).storingTaskAction();
        // TODO Replace with another verification?
        verify(pipelineTaskDataOperations).updateProcessingStep(eq(pipelineTask),
            eq(ProcessingStep.STORING));
    }

    @Test
    public void testProcessTask() {
        boolean b = pipelineStepExecutor.processTask();
        assertFalse(b);
        verify(pipelineStepExecutor).processingMainLoop();
    }
}
