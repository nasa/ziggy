package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyUnitTest;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;
import gov.nasa.ziggy.util.io.Filenames;
import gov.nasa.ziggy.worker.WorkerPipelineProcess;

/**
 * Unit tests for {@link AlgorithmMonitor}.
 *
 * @author PT
 */
public class AlgorithmMonitorTest extends ZiggyUnitTest {

    private AlgorithmMonitor monitor;
    private PipelineTask pipelineTask;
    private StateFile stateFile;
    private JobMonitor jobMonitor;
    private PipelineTaskCrud pipelineTaskCrud;
    private PipelineExecutor pipelineExecutor;
    private AlertService alertService;
    private ProcessingSummaryOperations attrOps;
    private PipelineInstanceNodeCrud nodeCrud;

    @Override
    public void setUp() throws IOException, ConfigurationException {

        DatabaseService.setInstance(Mockito.mock(DatabaseService.class));

        // Set the "pipeline results directory" to build/test
        System.setProperty(PropertyNames.RESULTS_DIR_PROP_NAME, Filenames.BUILD_TEST);
        Files.createDirectories(DirectoryProperties.stateFilesDir());
        jobMonitor = Mockito.mock(JobMonitor.class);
        monitor = Mockito.spy(new AlgorithmMonitor(false));
        Mockito.when(monitor.jobMonitor()).thenReturn(jobMonitor);
        Mockito.when(monitor.pollingIntervalMillis()).thenReturn(50L);
        pipelineTask = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask.pipelineInstanceId()).thenReturn(50L);
        Mockito.when(pipelineTask.getId()).thenReturn(100L);
        Mockito.when(pipelineTask.getModuleName()).thenReturn("dummy");
        Mockito.when(pipelineTask.getPipelineInstance())
            .thenReturn(Mockito.mock(PipelineInstance.class));
        Mockito.when(pipelineTask.getPipelineDefinitionNode())
            .thenReturn(Mockito.mock(PipelineDefinitionNode.class));
        pipelineTaskCrud = Mockito.mock(PipelineTaskCrud.class);
        Mockito.when(pipelineTaskCrud.retrieve(100L)).thenReturn(pipelineTask);
        Mockito.when(monitor.pipelineTaskCrud()).thenReturn(pipelineTaskCrud);
        nodeCrud = Mockito.mock(PipelineInstanceNodeCrud.class);
        pipelineExecutor = Mockito.spy(PipelineExecutor.class);
        pipelineExecutor.setPipelineTaskCrud(pipelineTaskCrud);
        pipelineExecutor.setPipelineInstanceNodeCrud(nodeCrud);
        Mockito
            .when(nodeCrud.retrieve(ArgumentMatchers.isA(PipelineInstance.class),
                ArgumentMatchers.isA(PipelineDefinitionNode.class)))
            .thenReturn(Mockito.mock(PipelineInstanceNode.class));
        Mockito.doReturn(null)
            .when(pipelineExecutor)
            .updateTaskCountsForCurrentNode(ArgumentMatchers.isA(PipelineTask.class),
                ArgumentMatchers.anyBoolean());
        Mockito.doNothing()
            .when(pipelineExecutor)
            .updateInstanceState(ArgumentMatchers.isA(PipelineInstance.class));
        attrOps = Mockito.mock(ProcessingSummaryOperations.class);
        pipelineExecutor.setPipelineInstanceCrud(Mockito.mock(PipelineInstanceCrud.class));
        Mockito.when(monitor.pipelineExecutor()).thenReturn(pipelineExecutor);
        Mockito.when(monitor.processingSummaryOperations()).thenReturn(attrOps);
        Mockito.when(monitor.pipelineTaskOperations())
            .thenReturn(Mockito.mock(PipelineTaskOperations.class));
        alertService = Mockito.mock(AlertService.class);
        Mockito.when(monitor.alertService()).thenReturn(alertService);
        stateFile = StateFile.generateStateFile(pipelineTask, null, 100);
        stateFile.persist();
        monitor.startMonitoring(stateFile);
    }

    @Override
    @After
    public void tearDown() throws IOException {
        WorkerPipelineProcess.workerTaskRequestQueue.clear();
    }

    @Override
    public Map<String, String> systemProperties() {
        Map<String, String> systemProperties = new HashMap<>();
        systemProperties.put(PropertyNames.RESULTS_DIR_PROP_NAME,
            ZiggyUnitTest.BUILD_TEST_PATH.toString());
        return systemProperties;
    }

    /**
     * Executes the {@link AlgorithmMonitor#run()} method a fixed number of times. This is necessary
     * because in some cases a later pass through run() is needed to respond to an action taken in
     * an earlier pass.
     *
     * @param iterationCount number of executions of run.
     */
    private void iterateAlgorithmMonitorRunMethod(int iterationCount) {
        for (int i = 0; i < iterationCount; i++) {
            monitor.run();
        }
    }

    // Tests basic "does the state file get into the Map?" functionality.
    @Test
    public void testStateFileSubmission() {
        StateFile storedStateFile = monitor.getStateFile(stateFile);
        assertEquals(50L, storedStateFile.getPipelineInstanceId());
        assertEquals(100L, storedStateFile.getPipelineTaskId());
        assertEquals("dummy", storedStateFile.getModuleName());
        assertEquals(100, storedStateFile.getNumTotal());
        assertEquals(0, storedStateFile.getNumComplete());
        assertEquals(0, storedStateFile.getNumFailed());
        assertEquals(StateFile.State.QUEUED, storedStateFile.getState());
    }

    // Tests that an update of the state file on disk gets reflected in the stored
    // state file.
    @Test
    public void testStateFileUpdate()
        throws ConfigurationException, IOException, InterruptedException {
        stateFile.setState(StateFile.State.PROCESSING);
        stateFile.setNumComplete(10);
        stateFile.setNumFailed(5);
        stateFile.persist();
        iterateAlgorithmMonitorRunMethod(1);
        StateFile storedStateFile = monitor.getStateFile(stateFile);
        assertEquals(50L, storedStateFile.getPipelineInstanceId());
        assertEquals(100L, storedStateFile.getPipelineTaskId());
        assertEquals("dummy", storedStateFile.getModuleName());
        assertEquals(100, storedStateFile.getNumTotal());
        assertEquals(10, storedStateFile.getNumComplete());
        assertEquals(5, storedStateFile.getNumFailed());
        assertEquals(StateFile.State.PROCESSING, storedStateFile.getState());
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_EXECUTING);
    }

    // Test a failed execution, that is to say one in which:
    // (a) The JobMonitor detects that the task failed, and
    // (b) the number of failed subtasks exceeds the number allowed for a
    // declaration of victory.
    @Test
    public void testExecutionFailed()
        throws ConfigurationException, IOException, InterruptedException {
        Mockito.when(pipelineTask.maxFailedSubtasks()).thenReturn(4);
        stateFile.setState(StateFile.State.PROCESSING);
        stateFile.setNumComplete(90);
        stateFile.setNumFailed(5);
        stateFile.persist();
        Mockito.when(jobMonitor.isFinished(ArgumentMatchers.any(StateFile.class))).thenReturn(true);

        iterateAlgorithmMonitorRunMethod(3);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The state file on disk is moved to state COMPLETE.
        StateFile updatedStateFile = StateFile.newStateFileFromDiskFile(
            DirectoryProperties.stateFilesDir().resolve(stateFile.name()).toFile(), true);
        assertEquals(StateFile.State.COMPLETE, updatedStateFile.getState());

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // The pipeline task state was set to ERROR.
        Mockito.verify(pipelineTask).setState(PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, WorkerPipelineProcess.workerTaskRequestQueue.size());
    }

    // Tests the reaction to a deleted task. In particular, even if the task is
    // "successful" in terms, of having few enough failed tasks, it won't be
    // submitted for re-running or for persisting.
    @Test
    public void testTaskDeletion()
        throws ConfigurationException, IOException, InterruptedException {
        Mockito.when(pipelineTask.maxFailedSubtasks()).thenReturn(6);
        stateFile.setState(StateFile.State.DELETED);
        stateFile.setNumComplete(95);
        stateFile.setNumFailed(5);
        stateFile.persist();
        iterateAlgorithmMonitorRunMethod(3);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The state file on disk is moved to state COMPLETE.
        StateFile updatedStateFile = StateFile.newStateFileFromDiskFile(
            DirectoryProperties.stateFilesDir().resolve(stateFile.name()).toFile(), true);
        assertEquals(StateFile.State.DELETED, updatedStateFile.getState());

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // The pipeline task state was set to ERROR.
        Mockito.verify(pipelineTask).setState(PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, WorkerPipelineProcess.workerTaskRequestQueue.size());

        // An alert should have been issued.
        Mockito.verify(alertService)
            .generateAndBroadcastAlert(ArgumentMatchers.anyString(), ArgumentMatchers.anyLong(),
                ArgumentMatchers.eq(AlertService.Severity.ERROR), ArgumentMatchers.anyString());
    }

    // Tests an execution that is complete, but has too many errors to be persisted.
    @Test
    public void testExecutionCompleteTooManyErrors()
        throws ConfigurationException, IOException, InterruptedException {

        Mockito.when(pipelineTask.maxFailedSubtasks()).thenReturn(4);
        stateFile.setState(StateFile.State.COMPLETE);
        stateFile.setNumComplete(95);
        stateFile.setNumFailed(5);
        stateFile.persist();
        iterateAlgorithmMonitorRunMethod(2);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // The pipeline task state was set to ERROR.
        Mockito.verify(pipelineTask).setState(PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, WorkerPipelineProcess.workerTaskRequestQueue.size());
    }

    // Tests an execution that is complete, but has too many unprocessed subtasks to
    // declare victory.
    @Test
    public void testExecutionCompleteTooManyMissed()
        throws ConfigurationException, IOException, InterruptedException {

        Mockito.when(pipelineTask.maxFailedSubtasks()).thenReturn(4);
        stateFile.setState(StateFile.State.COMPLETE);
        stateFile.setNumComplete(95);
        stateFile.setNumFailed(0);
        stateFile.persist();
        iterateAlgorithmMonitorRunMethod(2);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // The pipeline task state was set to ERROR.
        Mockito.verify(pipelineTask).setState(PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, WorkerPipelineProcess.workerTaskRequestQueue.size());
    }

    // Tests that when execution is COMPLETE and the number of failed subtasks is
    // small enough, a request to persist the task results is issued.
    @Test
    public void testExecutionComplete()
        throws ConfigurationException, IOException, InterruptedException {

        Mockito.when(pipelineTask.maxFailedSubtasks()).thenReturn(6);
        stateFile.setState(StateFile.State.COMPLETE);
        stateFile.setNumComplete(95);
        stateFile.setNumFailed(5);
        stateFile.persist();
        iterateAlgorithmMonitorRunMethod(2);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // There should be a task request in the queue
        assertEquals(1, WorkerPipelineProcess.workerTaskRequestQueue.size());
        WorkerTaskRequest taskRequest = WorkerPipelineProcess.workerTaskRequestQueue.take();
        assertEquals(0, taskRequest.getPriority());
        assertEquals(50L, taskRequest.getInstanceId());
        assertEquals(100L, taskRequest.getTaskId());
        assertEquals(RunMode.STANDARD, taskRequest.getRunMode());
    }

    // Test automatic resubmission of a task that's almost but not quite finished
    // according to the number of failed subtasks.
    @Test
    public void testAutoResubmit()
        throws ConfigurationException, IOException, InterruptedException {

        Mockito.when(pipelineTask.maxFailedSubtasks()).thenReturn(4);
        Mockito.when(pipelineTask.maxAutoResubmits()).thenReturn(3);
        Mockito.when(pipelineTask.getAutoResubmitCount()).thenReturn(1);
        Mockito.when(pipelineTask.getState()).thenReturn(PipelineTask.State.ERROR);
        stateFile.setState(StateFile.State.COMPLETE);
        stateFile.setNumComplete(95);
        stateFile.setNumFailed(5);
        stateFile.persist();
        iterateAlgorithmMonitorRunMethod(3);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // The task should have its auto-resubmit count incremented
        Mockito.verify(pipelineTask).incrementAutoResubmitCount();

        // The pipeline task state was set to ERROR, then to SUBMITTED
        Mockito.verify(pipelineTask).setState(PipelineTask.State.ERROR);
        Mockito.verify(pipelineTask).setState(PipelineTask.State.SUBMITTED);

        // There should be a task request in the queue
        assertEquals(1, WorkerPipelineProcess.workerTaskRequestQueue.size());
        WorkerTaskRequest taskRequest = WorkerPipelineProcess.workerTaskRequestQueue.take();
        assertEquals(0, taskRequest.getPriority());
        assertEquals(50L, taskRequest.getInstanceId());
        assertEquals(100L, taskRequest.getTaskId());
        assertEquals(RunMode.RESUBMIT, taskRequest.getRunMode());
    }

    // Test the case where automatic submission would be called except that the
    // task is out of automatic resubmits.
    @Test
    public void testOutOfAutoResubmits()
        throws ConfigurationException, IOException, InterruptedException {

        Mockito.when(pipelineTask.maxFailedSubtasks()).thenReturn(4);
        Mockito.when(pipelineTask.maxAutoResubmits()).thenReturn(3);
        Mockito.when(pipelineTask.getAutoResubmitCount()).thenReturn(3);
        Mockito.when(pipelineTask.getState()).thenReturn(PipelineTask.State.ERROR);
        stateFile.setState(StateFile.State.COMPLETE);
        stateFile.setNumComplete(95);
        stateFile.setNumFailed(5);
        stateFile.persist();
        iterateAlgorithmMonitorRunMethod(3);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // The pipeline task state was set to ERROR, then to SUBMITTED
        Mockito.verify(pipelineTask).setState(PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, WorkerPipelineProcess.workerTaskRequestQueue.size());

    }

}
