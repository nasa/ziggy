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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineOutputsSample1;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.module.remote.TimestampFile;
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

    private PipelineTask p;
    private PipelineInstance i;
    private ProcessingSummaryOperations a;
    private PipelineTaskCrud c;
    private TaskConfigurationManager ih;
    private TestAlgorithmLifecycle tal;
    private File taskDir;
    private RemoteParameters r;
    private PipelineInstanceNode pin;
    private PipelineModuleDefinition pmd;
    private DatabaseService ds;
    private AlgorithmExecutor ae;
    private TestPipelineModule t;
    private DatastoreProducerConsumerCrud dpcc;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, "/dev/null");

    @Rule
    public ZiggyPropertyRule piProcessingHaltStepPropertyRule = new ZiggyPropertyRule(PIPELINE_HALT,
        (String) null);

    @Rule
    public ZiggyPropertyRule piWorkerAllowPartialTasksPropertyRule = new ZiggyPropertyRule(
        PropertyName.ALLOW_PARTIAL_TASKS, (String) null);

    @Before
    public void setup() {
        r = new RemoteParameters();
        p = mock(PipelineTask.class);
        i = mock(PipelineInstance.class);
        pin = mock(PipelineInstanceNode.class);
        pmd = mock(PipelineModuleDefinition.class);
        ae = mock(AlgorithmExecutor.class);
        when(p.getId()).thenReturn(100L);
        when(p.getPipelineInstance()).thenReturn(i);
        when(p.getParameters(RemoteParameters.class, false)).thenReturn(r);
        when(p.getPipelineInstanceNode()).thenReturn(pin);
        when(pin.getPipelineModuleDefinition()).thenReturn(pmd);
        when(pmd.getInputsClass()).thenReturn(new ClassWrapper<>(PipelineInputsSample.class));
        when(pmd.getOutputsClass()).thenReturn(new ClassWrapper<>(PipelineOutputsSample1.class));
        when(i.getId()).thenReturn(50L);
        a = mock(ProcessingSummaryOperations.class);
        c = mock(PipelineTaskCrud.class);
        when(c.retrieve(100L)).thenReturn(p);
        ih = mock(TaskConfigurationManager.class);
        taskDir = directoryRule.directory().toFile();
        taskDir.mkdirs();
        tal = mock(TestAlgorithmLifecycle.class);
        when(tal.getTaskDir(true)).thenReturn(taskDir);
        when(tal.getTaskDir(false)).thenReturn(taskDir);
        when(tal.getExecutor()).thenReturn(ae);
        ds = mock(DatabaseService.class);
        DatabaseService.setInstance(ds);
        dpcc = mock(DatastoreProducerConsumerCrud.class);
        when(dpcc.retrieveFilesConsumedByTask(100L)).thenReturn(Collections.emptySet());
        t = new TestPipelineModule(p, RunMode.STANDARD);
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

        ProcessingState s = t.nextProcessingState(ProcessingState.INITIALIZING);
        assertEquals(ProcessingState.MARSHALING, s);
        s = t.nextProcessingState(s);
        assertEquals(ProcessingState.ALGORITHM_SUBMITTING, s);
        s = t.nextProcessingState(s);
        assertEquals(ProcessingState.ALGORITHM_QUEUED, s);
        s = t.nextProcessingState(s);
        assertEquals(ProcessingState.ALGORITHM_EXECUTING, s);
        s = t.nextProcessingState(s);
        assertEquals(ProcessingState.ALGORITHM_COMPLETE, s);
        s = t.nextProcessingState(s);
        assertEquals(ProcessingState.STORING, s);
        s = t.nextProcessingState(s);
        assertEquals(ProcessingState.COMPLETE, s);
    }

    /**
     * Tests that attempting to get the next processing state after the final one will throw the
     * correct exception.
     */
    @Test(expected = PipelineException.class)
    public void testExceptionFinalState() {

        t.nextProcessingState(ProcessingState.COMPLETE);
    }

    /**
     * Tests the initialize() method of ExternalProcessPipelineModule.
     */
    @Test
    public void testInitialize() {
        assertEquals(p, t.pipelineTask());
        assertEquals(100L, t.taskId());
        assertEquals(50L, t.instanceId());
        assertNotNull(t.algorithmManager());
        assertTrue(t.pipelineInputs() instanceof PipelineInputsSample);
        assertTrue(t.pipelineOutputs() instanceof PipelineOutputsSample1);
    }

    /**
     * Tests that the method that processes a module in INITIALIZING state performs correctly.
     */
    @Test
    public void testProcessInitialize() {

        t = Mockito.spy(t);
        t.initializingTaskAction();
        verify(t).incrementProcessingState();
    }

    /**
     * Tests that the method that processes a task in MARSHALING state performs correctly for local
     * processing tasks.
     *
     * @throws Exception
     */
    @Test
    public void testProcessMarshalingLocal() throws Exception {

        t = spy(t);
        boolean b = t.processMarshaling();
        assertFalse(b);

        verify(p).clearProducerTaskIds();
        verify(t).copyDatastoreFilesToTaskDirectory(eq(ih), eq(p), eq(taskDir));
        verify(ih).validate();
        verify(ih).persist(eq(taskDir));
        verify(t).incrementProcessingState();
    }

    /**
     * Tests that the method that processes a task in MARSHALING state performs correctly for remote
     * processing tasks.
     *
     * @throws Exception
     */
    @Test
    public void testProcessMarshalingRemote() throws Exception {

        t = spy(t);
        r.setEnabled(true);
        when(tal.isRemote()).thenReturn(true);
        boolean b = t.processMarshaling();
        assertFalse(b);

        verify(p).clearProducerTaskIds();
        verify(t).copyDatastoreFilesToTaskDirectory(eq(ih), eq(p), eq(taskDir));
        verify(ih).validate();
        verify(ih).persist(eq(taskDir));
        verify(t).incrementProcessingState();
    }

    /**
     * Tests that the method that processes a task in MARSHALING state performs correctly when no
     * inputs are generated.
     *
     * @throws Exception
     */
    @Test
    public void testProcessMarshalingNoInputs() throws Exception {

        t = spy(t);
        when(ih.isEmpty()).thenReturn(true);
        boolean b = t.processMarshaling();
        assertTrue(b);

        verify(p).clearProducerTaskIds();
        verify(t).copyDatastoreFilesToTaskDirectory(eq(ih), eq(p), eq(taskDir));
        verify(ih).validate();
        verify(ih, never()).persist(eq(taskDir));
        verify(t, never()).incrementProcessingState();
    }

    /**
     * Tests that the correct exception is thrown when a problem arises in inputs generation.
     */
    @Test(expected = PipelineException.class)
    public void testProcessMarshalingError1() {

        t = spy(t);
        doThrow(IllegalStateException.class).when(t)
            .copyDatastoreFilesToTaskDirectory(eq(ih), eq(p), eq(taskDir));
        t.processMarshaling();
    }

    /**
     * Tests that the correct exception is thrown when a problem arises while trying to commit the
     * database transaction.
     *
     * @throws Exception
     */
    @Test(expected = PipelineException.class)
    public void testProcessMarshalingError2() throws Exception {

        t = spy(t);
        doThrow(IllegalStateException.class).when(ds).commitTransaction();
        t.processMarshaling();
    }

    /**
     * Tests that the method that processes a task in ALGORITHM_EXECUTING state performs the correct
     * actions.
     */
    @Test
    public void testProcessAlgorithmExecuting() {

        // remote processing
        t = spy(t);
        when(tal.isRemote()).thenReturn(true);
        t.executingTaskAction();

        verify(tal).executeAlgorithm(null);
        verify(t, never()).incrementProcessingState();

        // local execution
        when(tal.isRemote()).thenReturn(false);
        t = new TestPipelineModule(p, RunMode.STANDARD);
        t = spy(t);
        t.executingTaskAction();

        verify(tal, times(2)).executeAlgorithm(null);
        verify(t, never()).incrementProcessingState();
    }

    /**
     * Tests that the method that processes a task in ALGORITHM_COMPLETED state performs the correct
     * actions.
     */
    @Test
    public void testProcessAlgorithmCompleted() {

        t = spy(t);
        t.algorithmCompleteTaskAction();
        verify(t).incrementProcessingState();

        when(tal.isRemote()).thenReturn(true);
        t.algorithmCompleteTaskAction();
        verify(t, times(2)).incrementProcessingState();
    }

    /**
     * Tests that the method that processes a task in STORING state performs the correct actions.
     */
    @Test
    public void testProcessStoring() {

        t = spy(t);
        ProcessingFailureSummary f = mock(ProcessingFailureSummary.class);
        when(f.isAllTasksSucceeded()).thenReturn(true);
        when(f.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(t).timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(t).timestampFileTimestamp(any(Event.class));
        doReturn(f).when(t).processingFailureSummary();

        // the local version performs relatively limited activities
        t.storingTaskAction();
        verify(t, never()).timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        verify(t, never()).timestampFileTimestamp(any(Event.class));
        verify(t, never()).valueMetricAddValue(any(String.class), any(long.class));
        verify(t).processingFailureSummary();
        verify(t).persistResultsAndDeleteTempFiles(eq(p), eq(f));
        verify(t).incrementProcessingState();

        // the remote version does somewhat more
        when(tal.isRemote()).thenReturn(true);
        t.storingTaskAction();
        verify(t, times(3)).timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        verify(t).timestampFileTimestamp(any(Event.class));
        verify(t, times(4)).valueMetricAddValue(any(String.class), any(long.class));
        verify(t, times(2)).processingFailureSummary();
        verify(t, times(2)).persistResultsAndDeleteTempFiles(eq(p), eq(f));
        verify(t, times(2)).incrementProcessingState();
    }

    /**
     * Tests that the method that processes a task in STORING state throws the correct exception in
     * the event of an error.
     */
    @Test(expected = PipelineException.class)
    public void testProcessStoringError() {

        t = spy(t);
        ProcessingFailureSummary f = mock(ProcessingFailureSummary.class);
        when(f.isAllTasksSucceeded()).thenReturn(true);
        when(f.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(t).timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(t).timestampFileTimestamp(any(Event.class));
        doReturn(f).when(t).processingFailureSummary();
        doThrow(IllegalStateException.class).when(t).persistResultsAndDeleteTempFiles(eq(p), eq(f));
        t.storingTaskAction();
    }

    /**
     * Tests that the storing process will continue even if partial failures occur.
     */
    @Test
    public void testPartialFailureStoreResults() {

        t = spy(t);
        ProcessingFailureSummary f = mock(ProcessingFailureSummary.class);
        when(f.isAllTasksSucceeded()).thenReturn(false);
        when(f.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(t).timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(t).timestampFileTimestamp(any(Event.class));
        doReturn(f).when(t).processingFailureSummary();
        t.storingTaskAction();
        verify(t).persistResultsAndDeleteTempFiles(eq(p), eq(f));
    }

    /**
     * Tests that the storing process will halt if partial failures occur and the configuration is
     * set up to reject same.
     */
    @Test(expected = PipelineException.class)
    public void testPartialFailureThrowException() {

        System.setProperty(PropertyName.ALLOW_PARTIAL_TASKS.property(), "false");
        t = spy(t);
        ProcessingFailureSummary f = mock(ProcessingFailureSummary.class);
        when(f.isAllTasksSucceeded()).thenReturn(false);
        when(f.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(t).timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(t).timestampFileTimestamp(any(Event.class));
        doReturn(f).when(t).processingFailureSummary();
        t.storingTaskAction();
    }

    /**
     * Tests that the storing process will halt if all sub tasks have failed.
     */
    @Test(expected = PipelineException.class)
    public void testTotalFailureThrowsException() {

        t = spy(t);
        ProcessingFailureSummary f = mock(ProcessingFailureSummary.class);
        when(f.isAllTasksSucceeded()).thenReturn(false);
        when(f.isAllTasksFailed()).thenReturn(true);
        doReturn(0L).when(t).timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(t).timestampFileTimestamp(any(Event.class));
        doReturn(f).when(t).processingFailureSummary();
        t.storingTaskAction();
    }

    /**
     * Tests the main processing loop in the local processing use case, and in the case in which the
     * marshaling successfully produces task directories.
     */
    @Test
    public void testProcessingMainLoopLocalTask1() {

        // create the pipeline module and its database
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);

        // setup mocking

        mockForLoopTest(t, tal, true, false);

        // do the loop method
        t.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(t.isProcessingSuccessful());
        verify(t).initializingTaskAction();
        verify(t).marshalingTaskAction();
        verify(t).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_SUBMITTING, t.getProcessingState());
    }

    /**
     * Tests the main processing loop in the local processing use case, and in the case in which the
     * worker enters at ALGORITHM_COMPLETE.
     */
    @Test
    public void testProcessingMainLoopLocalTask2() {

        // create the pipeline module and its database
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        t.setInitialProcessingState(ProcessingState.ALGORITHM_COMPLETE);

        // setup mocking

        mockForLoopTest(t, tal, true, false);

        // do the loop method
        t.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertTrue(t.isProcessingSuccessful());
        // verify(t, times(5)).getProcessingState();
        verify(t, never()).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t).algorithmCompleteTaskAction();
        verify(t).storingTaskAction();
        assertEquals(ProcessingState.COMPLETE, t.getProcessingState());
    }

    /**
     * Tests the main processing loop in the remote execution case, first part of processing. In
     * this case execution returns once the task is queued to run on the remote server.
     */
    @Test
    public void testProcessingMainLoopRemote1() {

        // create the pipeline module and its database
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);

        // setup mocking

        mockForLoopTest(t, tal, true, true);

        // do the loop method
        t.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(t.isProcessingSuccessful());
        verify(t, times(5)).getProcessingState();
        verify(t, times(2)).incrementProcessingState();
        verify(t).initializingTaskAction();
        verify(t).marshalingTaskAction();
        verify(t).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_SUBMITTING, t.getProcessingState());
    }

    /**
     * Tests the main processing loop in the remote execution case, second part of processing. In
     * this case execution starts at AC and goes to completion.
     */
    @Test
    public void testProcessingMainLoopRemote2() {

        // create the pipeline module and its database
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);

        // setup mocking

        mockForLoopTest(t, tal, true, true);

        // set up the state so it's at ALGORITHM_COMPLETE, the point at which the remote
        // system hands execution back to the local one
        t.setInitialProcessingState(ProcessingState.ALGORITHM_COMPLETE);

        // do the loop method
        t.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertTrue(t.isProcessingSuccessful());
        verify(t, times(4)).getProcessingState();
        verify(t, times(2)).incrementProcessingState();
        verify(t, never()).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t).algorithmCompleteTaskAction();
        verify(t).storingTaskAction();
        assertEquals(ProcessingState.COMPLETE, t.getProcessingState());
    }

    /**
     * Tests the main processing loop in restart case, specifically makes sure that if the loop is
     * called with processing at some arbitrary point it does the right things.
     */
    @Test
    public void testProcessingMainLoopRestart1() {

        // create the pipeline module and its database
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);

        // setup mocking

        mockForLoopTest(t, tal, true, false);

        // put the state to ALGORITHM_EXECUTING (emulates a restart after
        // the task failed in the middle of running)
        t.setInitialProcessingState(ProcessingState.ALGORITHM_EXECUTING);

        // do the loop method
        t.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(t.isProcessingSuccessful());
        verify(t, times(1)).getProcessingState();
        verify(t, never()).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_EXECUTING, t.getProcessingState());
    }

    /**
     * Tests the main processing loop in restart case, specifically makes sure that if the loop is
     * called with one of the remote states (queued or executing), it does the right thing
     */
    @Test
    public void testProcessingMainLoopRestart2() {

        // create the pipeline module and its database
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);

        // setup mocking

        mockForLoopTest(t, tal, true, true);

        // put the state to ALGORITHM_EXECUTING (emulates a restart after
        // the task failed in the middle of running)
        t.setInitialProcessingState(ProcessingState.ALGORITHM_QUEUED);

        // do the loop method
        t.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(t.isProcessingSuccessful());
        verify(t, times(1)).getProcessingState();
        verify(t, never()).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_QUEUED, t.getProcessingState());
    }

    /**
     * Tests the main processing loop in the case in which marshaling results in no task directories
     */
    @Test
    public void testProcessingMainLoopNoTaskDirs() {

        // create the pipeline module and its database
        TestPipelineModule t = new TestPipelineModule(p, RunMode.STANDARD);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);

        // setup mocking

        mockForLoopTest(t, tal, false, false);

        // do the loop method
        t.processingMainLoop();

        // check that everything we wanted to happen, happened
        assertFalse(t.isProcessingSuccessful());
        verify(t, times(4)).getProcessingState();
        verify(t, times(2)).incrementProcessingState();
        verify(t).initializingTaskAction();
        verify(t).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
    }

    /**
     * Tests that processing halts at the end of INITIALIZING state when required.
     */
    @Test
    public void testHaltInitialize() {

        // Set the desired stopping point
        System.setProperty(PIPELINE_HALT.property(), "I");
        // Set up mockery and states as though to run the main loop for local processing
        t = new TestPipelineModule(p, RunMode.STANDARD);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        PipelineException exception = assertThrows(PipelineException.class,
            () -> t.processingMainLoop());
        assertEquals(
            "Halting processing at end of step INITIALIZING due to configuration request for halt after step I",
            exception.getMessage());
        verify(t).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.INITIALIZING, t.getProcessingState());
    }

    /**
     * Tests that processing halts at the end of MARSHALING when required.
     */
    @Test
    public void testHaltMarshaling() {

        // Set the desired stopping point
        System.setProperty(PIPELINE_HALT.property(), "M");
        // Set up mockery and states as though to run the main loop for local processing
        t = new TestPipelineModule(p, RunMode.STANDARD);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        PipelineException exception = assertThrows(PipelineException.class,
            () -> t.processingMainLoop());
        assertEquals(
            "Halting processing at end of step MARSHALING due to configuration request for halt after step M",
            exception.getMessage());
        verify(t).initializingTaskAction();
        verify(t).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.MARSHALING, t.getProcessingState());
    }

    /**
     * Tests that processing halts at the end of ALGORITHM_COMPLETE when required.
     */
    @Test
    public void testHaltAlgorithmComplete() {

        // Set the desired stopping point
        System.setProperty(PIPELINE_HALT.property(), "Ac");
        // Set up mockery and states as though to run the main loop for local processing
        t = new TestPipelineModule(p, RunMode.STANDARD);
        t = spy(t);
        t.tdb.setPState(ProcessingState.ALGORITHM_COMPLETE);
        when(t.algorithmManager()).thenReturn(tal);
        PipelineException exception = assertThrows(PipelineException.class,
            () -> t.processingMainLoop());
        assertEquals(
            "Halting processing at end of step ALGORITHM_COMPLETE due to configuration request for halt after step Ac",
            exception.getMessage());
        verify(t, never()).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_COMPLETE, t.getProcessingState());
    }

    /**
     * Tests that processing halts at the end of STORING when required.
     */
    @Test
    public void testHaltStoring() {

        // Set the desired stopping point
        System.setProperty(PIPELINE_HALT.property(), "S");
        // Set up mockery and states as though to run the main loop for local processing
        t = new TestPipelineModule(p, RunMode.STANDARD);
        t = spy(t);
        t.tdb.setPState(ProcessingState.ALGORITHM_COMPLETE);
        when(t.algorithmManager()).thenReturn(tal);
        PipelineException exception = assertThrows(PipelineException.class,
            () -> t.processingMainLoop());
        assertEquals("Unable to persist due to sub-task failures", exception.getMessage());
        verify(t, never()).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t).algorithmCompleteTaskAction();
        verify(t).storingTaskAction();
        assertEquals(ProcessingState.STORING, t.getProcessingState());
    }

    /**
     * Tests that processing halts at the end of ALGORITHM_SUBMITTING when required.
     */
    @Test
    public void testHaltAlgorithmSubmitting() {

        // Set the desired stopping point
        System.setProperty(PIPELINE_HALT.property(), "As");
        // Set up mockery and states as though to run the main loop for local processing
        t = new TestPipelineModule(p, RunMode.STANDARD);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        mockForLoopTest(t, tal, true, true);
        t.setInitialProcessingState(ProcessingState.ALGORITHM_SUBMITTING);
        PipelineException exception = assertThrows(PipelineException.class,
            () -> t.processingMainLoop());
        assertEquals(
            "Halting processing at end of step ALGORITHM_SUBMITTING due to configuration request for halt after step As",
            exception.getMessage());
        verify(t, never()).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t).submittingTaskAction();
        verify(t, never()).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_SUBMITTING, t.getProcessingState());
    }

    /**
     * Tests that processing halts at the end of ALGORITHM_QUEUED when required.
     */
    @Test
    public void testHaltAlgorithmQueued() {

        // Set the desired stopping point
        System.setProperty(PIPELINE_HALT.property(), "Aq");
        // Set up mockery and states as though to run the main loop for local processing
        t = new TestPipelineModule(p, RunMode.STANDARD);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        mockForLoopTest(t, tal, true, true);
        t.setInitialProcessingState(ProcessingState.ALGORITHM_QUEUED);
        PipelineException exception = assertThrows(PipelineException.class,
            () -> t.processingMainLoop());
        assertEquals(
            "Halting processing at end of step ALGORITHM_QUEUED due to configuration request for halt after step Aq",
            exception.getMessage());
        verify(t, never()).initializingTaskAction();
        verify(t, never()).marshalingTaskAction();
        verify(t, never()).submittingTaskAction();
        verify(t).queuedTaskAction();
        verify(t, never()).executingTaskAction();
        verify(t, never()).algorithmCompleteTaskAction();
        verify(t, never()).storingTaskAction();
        assertEquals(ProcessingState.ALGORITHM_QUEUED, t.getProcessingState());
    }

    /**
     * Set up mocking for tests of the processingMainLoop() method.
     *
     * @param t
     * @param tal
     * @param successfulMarshaling
     * @param remote
     */
    private void mockForLoopTest(TestPipelineModule t, TestAlgorithmLifecycle tal,
        boolean successfulMarshaling, boolean remote) {

        t.setDoneLoopingValue(!successfulMarshaling);
        ProcessingFailureSummary f = mock(ProcessingFailureSummary.class);
        when(f.isAllTasksSucceeded()).thenReturn(true);
        when(f.isAllTasksFailed()).thenReturn(false);
        doReturn(0L).when(t).timestampFileElapsedTimeMillis(any(Event.class), any(Event.class));
        doReturn(0L).when(t).timestampFileTimestamp(any(Event.class));
        doReturn(f).when(t).processingFailureSummary();

        // mock the algorithm lifecycle manager's isRemote() call
        when(tal.isRemote()).thenReturn(remote);
    }

    /**
     * Tests that the processRestart() method performs the correct actions.
     */
    @Test
    public void testProcessRestart() {

        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        when(t.processingSummaryOperations()).thenReturn(a);
        doNothing().when(a).updateProcessingState(eq(100L), any(ProcessingState.class));

        // restart from beginning
        t = new TestPipelineModule(p, RunMode.RESTART_FROM_BEGINNING, true);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        when(t.processingSummaryOperations()).thenReturn(a);
        t.processTask();
        assertTrue(t.isProcessingSuccessful());
        verify(t).processingMainLoop();
        verify(a).updateProcessingState(eq(100L), eq(ProcessingState.INITIALIZING));

        // resume current step
        t = new TestPipelineModule(p, RunMode.RESUME_CURRENT_STEP, true);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        when(t.processingSummaryOperations()).thenReturn(a);
        t.processTask();
        assertTrue(t.isProcessingSuccessful());
        verify(a).updateProcessingState(eq(100L), any(ProcessingState.class));
        verify(t).processingMainLoop();

        // resubmit to PBS -- this is a local task, so nothing at all should happen
        t = new TestPipelineModule(p, RunMode.RESUBMIT, true);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        when(t.processingSummaryOperations()).thenReturn(a);
        t.processTask();
        assertTrue(t.isProcessingSuccessful());
        verify(a, times(2)).updateProcessingState(eq(100L), any(ProcessingState.class));
        verify(t).processingMainLoop();

        // resubmit to PBS for a remote task
        when(tal.isRemote()).thenReturn(true);
        t.processTask();
        assertTrue(t.isProcessingSuccessful());
        verify(a, times(3)).updateProcessingState(eq(100L), any(ProcessingState.class));
        verify(a, times(2)).updateProcessingState(eq(100L),
            eq(ProcessingState.ALGORITHM_SUBMITTING));
        verify(t, times(2)).processingMainLoop();

        // restart PBS monitoring
        t = new TestPipelineModule(p, RunMode.RESUME_MONITORING, true);
        t = spy(t);
        when(t.algorithmManager()).thenReturn(tal);
        when(t.processingSummaryOperations()).thenReturn(a);
        when(tal.isRemote()).thenReturn(true);
        t.processTask();
        assertFalse(t.isProcessingSuccessful());
        verify(a, times(3)).updateProcessingState(eq(100L), any(ProcessingState.class));
        verify(a, times(2)).updateProcessingState(eq(100L),
            eq(ProcessingState.ALGORITHM_SUBMITTING));
        verify(t, never()).processingMainLoop();
        verify(ae).resumeMonitoring();
    }

    @Test
    public void testProcessTask() {

        t = new TestPipelineModule(p, RunMode.STANDARD, true);
        t = spy(t);

        boolean b = t.processTask();
        assertTrue(b);
        verify(t).processingMainLoop();

        t = new TestPipelineModule(p, RunMode.RESUBMIT, true);
        t = spy(t);
        when(tal.isRemote()).thenReturn(true);
        b = t.processTask();
        assertTrue(b);
        verify(t).processingMainLoop();
        verify(t).resubmit();
    }

    /**
     * Stubbed implementation of the ExternalProcessPipelineModule abstract class for test purposes.
     * In addition to stubbing the methods that are actually used in normal processing (specifically
     * generateInputs(), outputsClass(), processOutputs(), getModuleName(), unitOfWorkTaskType()),
     * several additional methods are overridden: in some cases they are set up to return mocked
     * objects, in other cases they support use of the TestAttributesDatabase in place of a real
     * database.
     *
     * @author PT
     */
    public class TestPipelineModule extends ExternalProcessPipelineModule {

        public TestAttributesDatabase tdb = new TestAttributesDatabase();
        public Boolean marshalingReturn = null;
        public Boolean processingLoopSuccessState = null;

        public TestPipelineModule(PipelineTask p, RunMode r) {
            super(p, r);
        }

        public TestPipelineModule(PipelineTask p, RunMode r, boolean processingLoopSuccessState) {
            this(p, r);
            this.processingLoopSuccessState = processingLoopSuccessState;
        }

        @Override
        public ProcessingSummaryOperations processingSummaryOperations() {
            return a;
        }

        @Override
        PipelineTaskCrud pipelineTaskCrud() {
            return c;
        }

        @Override
        TaskConfigurationManager taskConfigurationManager() {
            return ih;
        }

        @Override
        public AlgorithmLifecycle algorithmManager() {
            return tal;
        }

        @Override
        StateFile generateStateFile() {
            return new StateFile();
        }

        @Override
        public ProcessingState getProcessingState() {
            return tdb.getPState();
        }

        @Override
        public void incrementProcessingState() {
            tdb.setPState(nextProcessingState(getProcessingState()));
        }

        void setInitialProcessingState(ProcessingState pState) {
            tdb.setPState(pState);
        }

        @Override
        long timestampFileElapsedTimeMillis(TimestampFile.Event startEvent,
            TimestampFile.Event finishEvent) {
            return 0L;
        }

        @Override
        long timestampFileTimestamp(TimestampFile.Event event) {
            return 0L;
        }

        @Override
        public void marshalingTaskAction() {
            super.marshalingTaskAction();
            boolean doneLooping = marshalingReturn == null ? getDoneLooping() : marshalingReturn;
            setDoneLooping(doneLooping);
        }

        // For some reason attempting to use Mockito to return the value required for the
        // test is not working, so we'll handle it this way:
        boolean processMarshaling() {
            super.marshalingTaskAction();
            return getDoneLooping();
        }

        void setDoneLoopingValue(boolean v) {
            marshalingReturn = v;
        }

        // Allows the processing main loop to either run normally or else skip execution
        // and simply set its return value, depending on context
        @Override
        public void processingMainLoop() {
            if (processingLoopSuccessState != null) {
                processingSuccessful = processingLoopSuccessState;
                return;
            }
            super.processingMainLoop();
        }

        public PipelineTask pipelineTask() {
            return pipelineTask;
        }

        @Override
        DatastoreProducerConsumerCrud datastoreProducerConsumerCrud() {
            return dpcc;
        }
    }

    /**
     * Stubbed implementation of AlgorithmLifecycle interface for use in testing.
     *
     * @author PT
     */
    class TestAlgorithmLifecycle implements AlgorithmLifecycle {

        @Override
        public File getTaskDir(boolean cleanExisting) {
            return null;
        }

        @Override
        public void executeAlgorithm(TaskConfigurationManager inputs) {
        }

        @Override
        public void doPostProcessing() {
        }

        @Override
        public boolean isRemote() {
            return false;
        }

        @Override
        public AlgorithmExecutor getExecutor() {
            return ae;
        }
    }

    /**
     * Emulates the database that contains the processing state for pipeline tasks.
     *
     * @author PT
     */
    class TestAttributesDatabase {

        private ProcessingState pState = null;

        public TestAttributesDatabase() {
            pState = ProcessingState.INITIALIZING;
        }

        public ProcessingState getPState() {
            return pState;
        }

        public void setPState(ProcessingState pState) {
            this.pState = pState;
        }
    }
}
