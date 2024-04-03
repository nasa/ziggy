package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HALT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
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
import java.util.Collections;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager.InputFiles;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineOutputsSample1;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.module.remote.TimestampFile.Event;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Unit test class for ExternalProcessPipelineModule.
 *
 * @author PT
 */
public class ExternalProcessPipelineModuleTest {

    private PipelineTask pipelineTask;
    private PipelineInstance pipelineInstance;
    private ProcessingSummaryOperations processingSummaryOperations;
    private PipelineTaskCrud pipelineTaskCrud;
    private TaskConfiguration taskConfiguration;
    private AlgorithmLifecycleManager taskAlgorithmLifecycle;
    private File taskDir;
    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineModuleDefinition pipelineModuleDefinition;
    private DatabaseService databaseService;
    private AlgorithmExecutor algorithmExecutor;
    private DatastoreProducerConsumerCrud datastoreProducerConsumerCrud;
    private ExternalProcessPipelineModule pipelineModule;
    private TaskDirectoryManager taskDirManager;
    private DatastoreFileManager datastoreFileManager;

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
        pipelineModuleDefinition = mock(PipelineModuleDefinition.class);
        algorithmExecutor = mock(AlgorithmExecutor.class);
        when(pipelineTask.getId()).thenReturn(100L);
        when(pipelineTask.getPipelineInstance()).thenReturn(pipelineInstance);
        when(pipelineTask.getPipelineInstanceNode()).thenReturn(pipelineInstanceNode);
        when(pipelineTask.taskBaseName()).thenReturn("50-100-test");
        when(pipelineInstanceNode.getPipelineModuleDefinition())
            .thenReturn(pipelineModuleDefinition);
        when(pipelineModuleDefinition.getInputsClass())
            .thenReturn(new ClassWrapper<>(PipelineInputsSample.class));
        when(pipelineModuleDefinition.getOutputsClass())
            .thenReturn(new ClassWrapper<>(PipelineOutputsSample1.class));
        when(pipelineInstance.getId()).thenReturn(50L);
        processingSummaryOperations = mock(ProcessingSummaryOperations.class);
        pipelineTaskCrud = mock(PipelineTaskCrud.class);
        when(pipelineTaskCrud.retrieve(100L)).thenReturn(pipelineTask);
        taskConfiguration = mock(TaskConfiguration.class);
        taskDir = directoryRule.directory().toFile();
        taskDir.mkdirs();
        taskAlgorithmLifecycle = mock(AlgorithmLifecycleManager.class);
        when(taskAlgorithmLifecycle.getTaskDir(true)).thenReturn(taskDir);
        when(taskAlgorithmLifecycle.getTaskDir(false)).thenReturn(taskDir);
        when(taskAlgorithmLifecycle.getExecutor()).thenReturn(algorithmExecutor);
        taskDirManager = mock(TaskDirectoryManager.class);
        when(taskDirManager.taskDir()).thenReturn(directoryRule.directory());
        databaseService = mock(DatabaseService.class);
        DatabaseService.setInstance(databaseService);
        datastoreProducerConsumerCrud = mock(DatastoreProducerConsumerCrud.class);
        when(datastoreProducerConsumerCrud.retrieveFilesConsumedByTask(100L))
            .thenReturn(Collections.emptySet());

        datastoreFileManager = mock(DatastoreFileManager.class);
        when(datastoreFileManager.inputFilesByOutputStatus())
            .thenReturn(new InputFiles(new HashSet<>(), new HashSet<>()));

        configurePipelineModule(RunMode.STANDARD);

        // By default, mock 5 subtasks.
        when(taskConfiguration.getSubtaskCount()).thenReturn(5);
    }

    /** Sets up a pipeline module with a specified run mode. */
    private void configurePipelineModule(RunMode runMode) {
        pipelineModule = spy(new ExternalProcessPipelineModule(pipelineTask, runMode));
        doReturn(taskConfiguration).when(pipelineModule).taskConfiguration();
        doReturn(datastoreFileManager).when(pipelineModule).datastoreFileManager();
        doReturn(new HashSet<>()).when(pipelineModule)
            .datastorePathsToRelative(ArgumentMatchers.anySet());
        doReturn(new HashSet<>()).when(pipelineModule)
            .datastorePathsToNames(ArgumentMatchers.anySet());
        doReturn(datastoreProducerConsumerCrud).when(pipelineModule)
            .datastoreProducerConsumerCrud();
        doReturn(taskAlgorithmLifecycle).when(pipelineModule).algorithmManager();
        doReturn(processingSummaryOperations).when(pipelineModule).processingSummaryOperations();
        doReturn(pipelineTaskCrud).when(pipelineModule).pipelineTaskCrud();
        doReturn(taskDirManager).when(pipelineModule).taskDirManager();

        // Return the database processing states in the correct order.
        configureDatabaseProcessingStates();
    }

    @After
    public void teardown() throws IOException {
        DatabaseService.reset();
    }

    /**
     * Tests the ability to determine the next processing state given a current processing state.
     */
    @Test
    public void testNextProcessingState() {

        ProcessingState processingState = pipelineModule
            .nextProcessingState(ProcessingState.INITIALIZING);
        assertEquals(ProcessingState.MARSHALING, processingState);
        processingState = pipelineModule.nextProcessingState(processingState);
        assertEquals(ProcessingState.ALGORITHM_SUBMITTING, processingState);
        processingState = pipelineModule.nextProcessingState(processingState);
        assertEquals(ProcessingState.ALGORITHM_QUEUED, processingState);
        processingState = pipelineModule.nextProcessingState(processingState);
        assertEquals(ProcessingState.ALGORITHM_EXECUTING, processingState);
        processingState = pipelineModule.nextProcessingState(processingState);
        assertEquals(ProcessingState.ALGORITHM_COMPLETE, processingState);
        processingState = pipelineModule.nextProcessingState(processingState);
        assertEquals(ProcessingState.STORING, processingState);
        processingState = pipelineModule.nextProcessingState(processingState);
        assertEquals(ProcessingState.COMPLETE, processingState);
    }

    /**
     * Tests that attempting to get the next processing state after the final one will throw the
     * correct exception.
     */
    @Test(expected = PipelineException.class)
    public void testExceptionFinalState() {
        pipelineModule.nextProcessingState(ProcessingState.COMPLETE);
    }

    /**
     * Tests the ExternalProcessPipelineModule constructor.
     */
    @Test
    public void testConstructor() {
        assertEquals(100L, pipelineModule.taskId());
        assertEquals(50L, pipelineModule.instanceId());
        assertNotNull(pipelineModule.algorithmManager());
        assertTrue(pipelineModule.pipelineInputs() instanceof PipelineInputsSample);
        assertTrue(pipelineModule.pipelineOutputs() instanceof PipelineOutputsSample1);
    }

    /**
     * Tests that the method that processes a module in INITIALIZING state performs correctly.
     */
    @Test
    public void testProcessInitialize() {

        doReturn(ProcessingState.INITIALIZING).when(pipelineModule).databaseProcessingState();
        pipelineModule.initializingTaskAction();
        verify(pipelineModule).incrementDatabaseProcessingState();
        assertFalse(pipelineModule.getDoneLooping());
        assertFalse(pipelineModule.isProcessingSuccessful());
    }

    /**
     * Tests that the method that processes a task in MARSHALING state performs correctly for local
     * processing tasks.
     *
     * @throws Exception
     */
    @Test
    public void testProcessMarshalingLocal() throws Exception {

        doReturn(ProcessingState.MARSHALING).when(pipelineModule).databaseProcessingState();
        pipelineModule.marshalingTaskAction();

        verify(pipelineTask).clearProducerTaskIds();
        verify(pipelineModule).copyDatastoreFilesToTaskDirectory(eq(taskConfiguration),
            eq(taskDir));
        verify(taskConfiguration).serialize(eq(taskDir));
        verify(pipelineModule).incrementDatabaseProcessingState();
        assertFalse(pipelineModule.getDoneLooping());
        assertFalse(pipelineModule.isProcessingSuccessful());
    }

    /**
     * Tests that the method that processes a task in MARSHALING state performs correctly when no
     * inputs are generated.
     *
     * @throws Exception
     */
    @Test
    public void testProcessMarshalingNoInputs() throws Exception {

        doReturn(ProcessingState.MARSHALING).when(pipelineModule).databaseProcessingState();
        when(taskConfiguration.getSubtaskCount()).thenReturn(0);
        pipelineModule.marshalingTaskAction();

        verify(pipelineTask).clearProducerTaskIds();
        verify(pipelineModule).copyDatastoreFilesToTaskDirectory(eq(taskConfiguration),
            eq(taskDir));
        verify(taskConfiguration, never()).serialize(eq(taskDir));
        verify(pipelineModule, never()).incrementDatabaseProcessingState();
        assertTrue(pipelineModule.getDoneLooping());
        assertTrue(pipelineModule.isProcessingSuccessful());
    }

    /**
     * Tests that the correct exception is thrown when a problem arises in inputs generation.
     */
    @Test(expected = PipelineException.class)
    public void testProcessMarshalingError1() {

        doReturn(ProcessingState.MARSHALING).when(pipelineModule).databaseProcessingState();
        pipelineModule.marshalingTaskAction();
        doThrow(IllegalStateException.class).when(pipelineModule)
            .copyDatastoreFilesToTaskDirectory(eq(taskConfiguration), eq(taskDir));
        pipelineModule.marshalingTaskAction();
    }

    /**
     * Tests that the method that processes a task in ALGORITHM_EXECUTING state performs the correct
     * actions.
     */
    @Test
    public void testProcessAlgorithmExecuting() {

        // remote processing
        doReturn(ProcessingState.ALGORITHM_EXECUTING).when(pipelineModule)
            .databaseProcessingState();
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(true);
        pipelineModule.executingTaskAction();

        verify(taskAlgorithmLifecycle).executeAlgorithm(null);
        verify(pipelineModule, never()).incrementDatabaseProcessingState();
        assertTrue(pipelineModule.getDoneLooping());
        assertFalse(pipelineModule.isProcessingSuccessful());

        // local execution
        configurePipelineModule(RunMode.STANDARD);
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(false);
        pipelineModule.executingTaskAction();

        verify(taskAlgorithmLifecycle, times(2)).executeAlgorithm(null);
        verify(pipelineModule, never()).incrementDatabaseProcessingState();
        assertTrue(pipelineModule.getDoneLooping());
        assertFalse(pipelineModule.isProcessingSuccessful());
    }

    /**
     * Tests that the method that processes a task in ALGORITHM_COMPLETED state performs the correct
     * actions.
     */
    @Test
    public void testProcessAlgorithmCompleted() {

        doReturn(ProcessingState.ALGORITHM_COMPLETE).when(pipelineModule).databaseProcessingState();
        pipelineModule.algorithmCompleteTaskAction();
        verify(pipelineModule).incrementDatabaseProcessingState();

        when(taskAlgorithmLifecycle.isRemote()).thenReturn(true);
        pipelineModule.algorithmCompleteTaskAction();
        verify(pipelineModule, times(2)).incrementDatabaseProcessingState();
    }

    /**
     * Tests that the method that processes a task in STORING state performs the correct actions.
     */
    @Test
    public void testProcessStoring() {

        doReturn(ProcessingState.STORING).when(pipelineModule).databaseProcessingState();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(true);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineModule)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineModule).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineModule).processingFailureSummary();

        // the local version performs relatively limited activities
        pipelineModule.storingTaskAction();
        verify(pipelineModule, never()).timestampFileElapsedTimeMillis(any(Event.class),
            any(Event.class));
        verify(pipelineModule, never()).timestampFileTimestamp(any(Event.class));
        verify(pipelineModule, never()).valueMetricAddValue(any(String.class), any(long.class));
        verify(pipelineModule).processingFailureSummary();
        verify(pipelineModule).persistResultsAndUpdateConsumers();
        verify(pipelineModule).incrementDatabaseProcessingState();

        // the remote version does somewhat more
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(true);
        pipelineModule.storingTaskAction();
        verify(pipelineModule, times(3)).timestampFileElapsedTimeMillis(any(Event.class),
            any(Event.class));
        verify(pipelineModule).timestampFileTimestamp(any(Event.class));
        verify(pipelineModule, times(4)).valueMetricAddValue(any(String.class), any(long.class));
        verify(pipelineModule, times(2)).processingFailureSummary();
        verify(pipelineModule, times(2)).persistResultsAndUpdateConsumers();
        verify(pipelineModule, times(2)).incrementDatabaseProcessingState();
    }

    /**
     * Tests that the method that processes a task in STORING state throws the correct exception in
     * the event of an error.
     */
    @Test(expected = PipelineException.class)
    public void testProcessStoringError() {

        doReturn(ProcessingState.STORING).when(pipelineModule).databaseProcessingState();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(true);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineModule)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineModule).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineModule).processingFailureSummary();
        doThrow(IllegalStateException.class).when(pipelineModule)
            .persistResultsAndUpdateConsumers();
        pipelineModule.storingTaskAction();
    }

    /**
     * Tests that the storing process will continue even if partial failures occur.
     */
    @Test
    public void testPartialFailureStoreResults() {

        doReturn(ProcessingState.STORING).when(pipelineModule).databaseProcessingState();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(false);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineModule)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineModule).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineModule).processingFailureSummary();
        pipelineModule.storingTaskAction();
        verify(pipelineModule).persistResultsAndUpdateConsumers();
    }

    /**
     * Tests that the storing process will halt if partial failures occur and the configuration is
     * set up to reject same.
     */
    @Test(expected = PipelineException.class)
    public void testPartialFailureThrowException() {

        piWorkerAllowPartialTasksPropertyRule.setValue("false");
        doReturn(ProcessingState.STORING).when(pipelineModule).databaseProcessingState();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(false);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineModule)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineModule).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineModule).processingFailureSummary();
        pipelineModule.storingTaskAction();
    }

    /**
     * Tests that the storing process will halt if all sub tasks have failed.
     */
    @Test(expected = PipelineException.class)
    public void testTotalFailureThrowsException() {

        doReturn(ProcessingState.STORING).when(pipelineModule).databaseProcessingState();
        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(false);
        when(failureSummary.isAllTasksFailed()).thenReturn(true);
        doReturn(0L).when(pipelineModule)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineModule).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineModule).processingFailureSummary();
        pipelineModule.storingTaskAction();
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
        pipelineModule.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule).initializingTaskAction();
        verify(pipelineModule).marshalingTaskAction();
        verify(pipelineModule).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_SUBMITTING,
            pipelineModule.databaseProcessingState());
    }

    private void configureDatabaseProcessingStates() {
        // Note that getProcessingState() is called twice during normal operations:
        // once in ExternalProcessPipelineModule, once in ProcessingStatePipelineModule.
        doReturn(ProcessingState.INITIALIZING, ProcessingState.INITIALIZING,
            ProcessingState.MARSHALING, ProcessingState.MARSHALING,
            ProcessingState.ALGORITHM_SUBMITTING, ProcessingState.ALGORITHM_SUBMITTING,
            ProcessingState.ALGORITHM_QUEUED, ProcessingState.ALGORITHM_QUEUED,
            ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();
    }

    /**
     * Tests the main processing loop in the local processing use case, and in the case in which the
     * worker enters at ALGORITHM_COMPLETE.
     */
    @Test
    public void testProcessingMainLoopLocalTask2() {

        doReturn(ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();

        // setup mocking
        mockForLoopTest(true, false);

        // do the loop method
        pipelineModule.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertTrue(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule).algorithmCompleteTaskAction();
        verify(pipelineModule).storingTaskAction();
        assertEquals(ProcessingState.COMPLETE, pipelineModule.databaseProcessingState());
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
        pipelineModule.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule, times(5)).databaseProcessingState();
        verify(pipelineModule, times(2)).incrementDatabaseProcessingState();
        verify(pipelineModule).initializingTaskAction();
        verify(pipelineModule).marshalingTaskAction();
        verify(pipelineModule).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_SUBMITTING,
            pipelineModule.databaseProcessingState());
    }

    /**
     * Tests the main processing loop in the remote execution case, second part of processing. In
     * this case execution starts at AC and goes to completion.
     */
    @Test
    public void testProcessingMainLoopRemote2() {

        doReturn(ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();

        // setup mocking
        mockForLoopTest(true, true);

        // do the loop method
        pipelineModule.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertTrue(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule, times(4)).databaseProcessingState();
        verify(pipelineModule, times(2)).incrementDatabaseProcessingState();
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule).algorithmCompleteTaskAction();
        verify(pipelineModule).storingTaskAction();
        assertEquals(ProcessingState.COMPLETE, pipelineModule.databaseProcessingState());
    }

    /**
     * Tests the main processing loop in restart case, specifically makes sure that if the loop is
     * called with processing at some arbitrary point it does the right things.
     */
    @Test
    public void testProcessingMainLoopRestart1() {

        // Note that getProcessingState() is called twice during normal operations:
        // once in ExternalProcessPipelineModule, once in ProcessingStatePipelineModule.
        doReturn(ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();

        // setup mocking
        mockForLoopTest(true, false);

        // do the loop method
        pipelineModule.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule, times(1)).databaseProcessingState();
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_EXECUTING, pipelineModule.databaseProcessingState());
    }

    /**
     * Tests the main processing loop in restart case, specifically makes sure that if the loop is
     * called with one of the remote states (queued or executing), it does the right thing
     */
    @Test
    public void testProcessingMainLoopRestart2() {

        // Note that getProcessingState() is called twice during normal operations:
        // once in ExternalProcessPipelineModule, once in ProcessingStatePipelineModule.
        doReturn(ProcessingState.ALGORITHM_QUEUED, ProcessingState.ALGORITHM_QUEUED,
            ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();

        // setup mocking
        mockForLoopTest(true, true);

        // do the loop method
        pipelineModule.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule, times(1)).databaseProcessingState();
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_QUEUED, pipelineModule.databaseProcessingState());
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
        pipelineModule.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertTrue(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule).initializingTaskAction();
        verify(pipelineModule).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        verify(pipelineModule, times(3)).databaseProcessingState();
        verify(pipelineModule, times(1)).incrementDatabaseProcessingState();
    }

    /**
     * Tests that processing halts at the end of INITIALIZING state when required.
     */
    @Test
    public void testHaltInitialize() {

        // Set the desired stopping point
        doReturn("I").when(pipelineModule).haltStep();
        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineModule.processingMainLoop());
        assertEquals(
            "Halting processing at end of step INITIALIZING due to configuration request for halt after step I",
            exception.getMessage());
        verify(pipelineModule).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        assertEquals(ProcessingState.INITIALIZING, pipelineModule.databaseProcessingState());
    }

    /**
     * Tests that processing halts at the end of MARSHALING when required.
     */
    @Test
    public void testHaltMarshaling() {

        // Set the desired stopping point
        doReturn("M").when(pipelineModule).haltStep();
        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineModule.processingMainLoop());
        assertEquals(
            "Halting processing at end of step MARSHALING due to configuration request for halt after step M",
            exception.getMessage());
        verify(pipelineModule).initializingTaskAction();
        verify(pipelineModule).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        assertEquals(ProcessingState.MARSHALING, pipelineModule.databaseProcessingState());
    }

    /**
     * Tests that processing halts at the end of ALGORITHM_COMPLETE when required.
     */
    @Test
    public void testHaltAlgorithmComplete() {

        doReturn(ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();

        // Set the desired stopping point
        doReturn("Ac").when(pipelineModule).haltStep();

        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineModule.processingMainLoop());
        assertEquals(
            "Halting processing at end of step ALGORITHM_COMPLETE due to configuration request for halt after step Ac",
            exception.getMessage());
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_COMPLETE, pipelineModule.databaseProcessingState());
    }

    /**
     * Tests that processing halts at the end of STORING when required.
     */
    @Test
    public void testHaltStoring() {

        doReturn(ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();

        // Set the desired stopping point
        doReturn("S").when(pipelineModule).haltStep();

        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineModule.processingMainLoop());
        assertEquals("Unable to persist due to sub-task failures", exception.getMessage());
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule).algorithmCompleteTaskAction();
        verify(pipelineModule).storingTaskAction();
        assertEquals(ProcessingState.STORING, pipelineModule.databaseProcessingState());
    }

    /**
     * Tests that processing halts at the end of ALGORITHM_SUBMITTING when required.
     */
    @Test
    public void testHaltAlgorithmSubmitting() {

        // Set the desired stopping point
        doReturn("As").when(pipelineModule).haltStep();
        mockForLoopTest(true, true);
        PipelineException exception = assertThrows(PipelineException.class,
            () -> pipelineModule.processingMainLoop());
        assertEquals(
            "Halting processing at end of step ALGORITHM_SUBMITTING due to configuration request for halt after step As",
            exception.getMessage());
        verify(pipelineModule).initializingTaskAction();
        verify(pipelineModule).marshalingTaskAction();
        verify(pipelineModule).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_SUBMITTING,
            pipelineModule.databaseProcessingState());
    }

    /**
     * Set up mocking for tests of the processingMainLoop() method.
     *
     * @param t
     * @param tal
     * @param successfulMarshaling
     * @param remote
     */
    private void mockForLoopTest(boolean successfulMarshaling, boolean remote) {

        ProcessingFailureSummary failureSummary = mock(ProcessingFailureSummary.class);
        when(failureSummary.isAllTasksSucceeded()).thenReturn(true);
        when(failureSummary.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(pipelineModule)
            .timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(pipelineModule).timestampFileTimestamp(any(Event.class));
        doReturn(failureSummary).when(pipelineModule).processingFailureSummary();

        // mock the algorithm lifecycle manager's isRemote() call
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(remote);
    }

    /** Tests restart from beginning. */
    @Test
    public void testRestartFromBeginning() {
        configurePipelineModule(RunMode.RESTART_FROM_BEGINNING);
        pipelineModule.processTask();
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule).processingMainLoop();
        verify(pipelineModule).initializingTaskAction();
        verify(pipelineModule).marshalingTaskAction();
        verify(pipelineModule).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        verify(pipelineModule, never()).processingCompleteTaskAction();
        verify(processingSummaryOperations).updateProcessingState(eq(100L),
            eq(ProcessingState.ALGORITHM_SUBMITTING));
    }

    /** Tests a resubmit for a local-execution task. */
    @Test
    public void testResubmitLocalTask() {
        configurePipelineModule(RunMode.RESUBMIT);
        doReturn(ProcessingState.ALGORITHM_SUBMITTING, ProcessingState.ALGORITHM_SUBMITTING,
            ProcessingState.ALGORITHM_QUEUED, ProcessingState.ALGORITHM_QUEUED,
            ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();
        pipelineModule.processTask();
        verify(processingSummaryOperations).updateProcessingState(eq(100L),
            eq(ProcessingState.ALGORITHM_SUBMITTING));
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule).processingMainLoop();
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        verify(pipelineModule, never()).processingCompleteTaskAction();
        verify(processingSummaryOperations).updateProcessingState(eq(100L),
            any(ProcessingState.class));
    }

    /** Tests a resubmit for a remote execution task. */
    @Test
    public void testResubmitRemoteTask() {
        configurePipelineModule(RunMode.RESUBMIT);
        doReturn(ProcessingState.ALGORITHM_SUBMITTING, ProcessingState.ALGORITHM_SUBMITTING,
            ProcessingState.ALGORITHM_QUEUED, ProcessingState.ALGORITHM_QUEUED,
            ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();
        when(taskAlgorithmLifecycle.isRemote()).thenReturn(true);
        pipelineModule.processTask();
        verify(processingSummaryOperations).updateProcessingState(eq(100L),
            eq(ProcessingState.ALGORITHM_SUBMITTING));
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule).processingMainLoop();
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        verify(pipelineModule, never()).processingCompleteTaskAction();
    }

    @Test
    public void testResumeMonitoring() {
        configurePipelineModule(RunMode.RESUME_MONITORING);
        doReturn(ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();
        pipelineModule.processTask();
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(algorithmExecutor).resumeMonitoring();
        verify(pipelineModule, never()).processingMainLoop();
        verify(pipelineModule, never()).incrementDatabaseProcessingState();
        verify(pipelineModule, never()).databaseProcessingState();
    }

    /** Test resumption of the marshaling step. */
    @Test
    public void testResumeMarshaling() {

        // resume current step
        configurePipelineModule(RunMode.RESUME_CURRENT_STEP);
        doReturn(ProcessingState.MARSHALING, ProcessingState.MARSHALING,
            ProcessingState.ALGORITHM_SUBMITTING, ProcessingState.ALGORITHM_SUBMITTING,
            ProcessingState.ALGORITHM_QUEUED, ProcessingState.ALGORITHM_QUEUED,
            ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();
        pipelineModule.processTask();
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule).processingMainLoop();
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule).marshalingTaskAction();
        verify(pipelineModule).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        verify(pipelineModule, never()).processingCompleteTaskAction();
        verify(processingSummaryOperations).updateProcessingState(eq(100L),
            eq(ProcessingState.ALGORITHM_SUBMITTING));
    }

    /** Test resumption of algorithm execution. */
    @Test
    public void testResumeAlgorithmExecuting() {
        configurePipelineModule(RunMode.RESUME_CURRENT_STEP);
        doReturn(ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_EXECUTING,
            ProcessingState.ALGORITHM_COMPLETE, ProcessingState.ALGORITHM_COMPLETE,
            ProcessingState.STORING, ProcessingState.STORING, ProcessingState.COMPLETE,
            ProcessingState.COMPLETE).when(pipelineModule).databaseProcessingState();
        pipelineModule.processTask();
        assertFalse(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule).processingMainLoop();
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule).executingTaskAction();
        verify(pipelineModule, never()).algorithmCompleteTaskAction();
        verify(pipelineModule, never()).storingTaskAction();
        verify(pipelineModule, never()).processingCompleteTaskAction();
        verify(processingSummaryOperations, never()).updateProcessingState(eq(100L),
            eq(ProcessingState.ALGORITHM_EXECUTING));
    }

    @Test
    public void testResumeAlgorithmComplete() {
        mockForLoopTest(false, true);
        configurePipelineModule(RunMode.RESUME_CURRENT_STEP);
        doReturn(ProcessingState.ALGORITHM_COMPLETE, ProcessingState.STORING,
            ProcessingState.STORING, ProcessingState.COMPLETE, ProcessingState.COMPLETE)
                .when(pipelineModule)
                .databaseProcessingState();
        doNothing().when(pipelineModule).storingTaskAction();
        pipelineModule.processTask();
        assertTrue(pipelineModule.isProcessingSuccessful());
        verify(pipelineModule).processingMainLoop();
        verify(pipelineModule, never()).initializingTaskAction();
        verify(pipelineModule, never()).marshalingTaskAction();
        verify(pipelineModule, never()).submittingTaskAction();
        verify(pipelineModule, never()).queuedTaskAction();
        verify(pipelineModule, never()).executingTaskAction();
        verify(pipelineModule).algorithmCompleteTaskAction();
        verify(pipelineModule).storingTaskAction();
        verify(pipelineModule).processingCompleteTaskAction();
        verify(processingSummaryOperations).updateProcessingState(eq(100L),
            eq(ProcessingState.COMPLETE));
    }

    @Test
    public void testProcessTask() {
        boolean b = pipelineModule.processTask();
        assertFalse(b);
        verify(pipelineModule).processingMainLoop();
    }
}
