package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.AlgorithmExecutor.AlgorithmType;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.supervisor.TaskRequestHandlerLifecycleManager;
import gov.nasa.ziggy.supervisor.TaskRequestHandlerLifecycleManagerTest.InstrumentedTaskRequestHandlerLifecycleManager;

/**
 * Unit tests for {@link AlgorithmMonitor}.
 *
 * @author PT
 */
public class AlgorithmMonitorTest {

    private AlgorithmMonitor monitor;
    private PipelineTask pipelineTask;
    private StateFile stateFile;
    private JobMonitor jobMonitor;
    private PipelineTaskCrud pipelineTaskCrud;
    private PipelineExecutor pipelineExecutor;
    private PipelineOperations pipelineOperations;
    private AlertService alertService;
    private ProcessingSummaryOperations attrOps;
    private PipelineInstanceNodeCrud nodeCrud;
    private PipelineDefinitionNodeExecutionResources resources = new PipelineDefinitionNodeExecutionResources(
        "dummy", "dummy");

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
    public TaskRequestHandlerLifecycleManager lifecycleManager = new InstrumentedTaskRequestHandlerLifecycleManager();

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(resultsDirPropertyRule);

    @Before
    public void setUp() throws IOException, ConfigurationException {

        DatabaseService.setInstance(Mockito.mock(DatabaseService.class));

        Files.createDirectories(DirectoryProperties.stateFilesDir());
        jobMonitor = Mockito.mock(JobMonitor.class);
        monitor = Mockito.spy(new AlgorithmMonitor(AlgorithmType.LOCAL));
        Mockito.when(monitor.jobMonitor()).thenReturn(jobMonitor);
        Mockito.when(monitor.pollingIntervalMillis()).thenReturn(50L);
        Mockito.doReturn(false).when(monitor).taskIsKilled(ArgumentMatchers.isA(long.class));
        pipelineTask = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(50L).when(pipelineTask).pipelineInstanceId();
        Mockito.doReturn(100L).when(pipelineTask).getId();
        Mockito.doReturn("dummy").when(pipelineTask).getModuleName();
        Mockito.doReturn(Mockito.mock(PipelineInstance.class))
            .when(pipelineTask)
            .getPipelineInstance();
        Mockito.doReturn(100).when(pipelineTask).exeTimeoutSeconds();
        Mockito.doReturn(new HashMap<>())
            .when(pipelineTask)
            .getPipelineParameterSets();
        Mockito.doReturn(new HashMap<>())
            .when(pipelineTask)
            .getModuleParameterSets();
        pipelineTaskCrud = Mockito.mock(PipelineTaskCrud.class);
        Mockito.when(pipelineTaskCrud.retrieve(100L)).thenReturn(pipelineTask);
        Mockito.when(pipelineTaskCrud.merge(ArgumentMatchers.isA(PipelineTask.class)))
            .thenReturn(pipelineTask);
        Mockito.when(monitor.pipelineTaskCrud()).thenReturn(pipelineTaskCrud);
        nodeCrud = Mockito.mock(PipelineInstanceNodeCrud.class);
        pipelineOperations = Mockito.mock(PipelineOperations.class);
        Mockito
            .when(pipelineOperations.taskCounts(ArgumentMatchers.isA(PipelineInstanceNode.class)))
            .thenReturn(new TaskCounts(50, 50, 10, 1));
        Mockito.when(monitor.pipelineOperations()).thenReturn(pipelineOperations);
        pipelineExecutor = Mockito.spy(PipelineExecutor.class);
        Mockito.doReturn(pipelineTaskCrud).when(pipelineExecutor).pipelineTaskCrud();
        Mockito.doReturn(nodeCrud).when(pipelineExecutor).pipelineInstanceNodeCrud();
        Mockito.doReturn(pipelineOperations).when(pipelineExecutor).pipelineOperations();
        Mockito.doNothing()
            .when(pipelineExecutor)
            .removeTaskFromKilledTaskList(ArgumentMatchers.isA(long.class));
        Mockito
            .when(nodeCrud.retrieve(ArgumentMatchers.isA(PipelineInstance.class),
                ArgumentMatchers.isA(PipelineDefinitionNode.class)))
            .thenReturn(Mockito.mock(PipelineInstanceNode.class));
        Mockito.when(pipelineExecutor.taskRequestEnabled()).thenReturn(false);
        attrOps = Mockito.mock(ProcessingSummaryOperations.class);
        Mockito.doReturn(Mockito.mock(PipelineInstanceCrud.class))
            .when(pipelineExecutor)
            .pipelineInstanceCrud();
        PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud = Mockito
            .mock(PipelineDefinitionNodeCrud.class);
        PipelineDefinitionNode pipelineDefinitionNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.when(monitor.pipelineDefinitionNodeCrud()).thenReturn(pipelineDefinitionNodeCrud);
        Mockito.doReturn(pipelineDefinitionNode).when(pipelineTask).pipelineDefinitionNode();
        Mockito.when(pipelineDefinitionNodeCrud.retrieveExecutionResources(pipelineDefinitionNode))
            .thenReturn(resources);
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

    @After
    public void tearDown() throws IOException {
        DatabaseService.reset();
        TestEventDetector.detectTestEvent(500L, () -> (lifecycleManager.taskRequestSize() == 0));
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
        resources.setMaxFailedSubtaskCount(4);
        stateFile.setState(StateFile.State.PROCESSING);
        stateFile.setNumComplete(90);
        stateFile.setNumFailed(5);
        stateFile.persist();
        Mockito.when(jobMonitor.isFinished(ArgumentMatchers.any(StateFile.class))).thenReturn(true);
        iterateAlgorithmMonitorRunMethod(3);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The state file on disk is moved to state COMPLETE.
        StateFile updatedStateFile = stateFile.newStateFileFromDiskFile();
        assertEquals(StateFile.State.COMPLETE, updatedStateFile.getState());

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // The pipeline task state was set to ERROR.
        Mockito.verify(pipelineOperations).setTaskState(pipelineTask, PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, lifecycleManager.taskRequestSize());
    }

    // Tests an execution that is complete, but has too many errors to be persisted.
    @Test
    public void testExecutionCompleteTooManyErrors()
        throws ConfigurationException, IOException, InterruptedException {

        resources.setMaxFailedSubtaskCount(4);
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
        Mockito.verify(pipelineOperations).setTaskState(pipelineTask, PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, lifecycleManager.taskRequestSize());
    }

    // Tests an execution that is complete, but has too many unprocessed subtasks to
    // declare victory.
    @Test
    public void testExecutionCompleteTooManyMissed()
        throws ConfigurationException, IOException, InterruptedException {

        resources.setMaxFailedSubtaskCount(4);
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
        Mockito.verify(pipelineOperations).setTaskState(pipelineTask, PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, lifecycleManager.taskRequestSize());
    }

    // Tests that when execution is COMPLETE and the number of failed subtasks is
    // small enough, a request to persist the task results is issued.
    @Test
    public void testExecutionComplete()
        throws ConfigurationException, IOException, InterruptedException {

        resources.setMaxFailedSubtaskCount(6);
        stateFile.setState(StateFile.State.COMPLETE);
        stateFile.setNumComplete(95);
        stateFile.setNumFailed(5);
        stateFile.persist();
        iterateAlgorithmMonitorRunMethod(2);

        // No state file remains in the monitoring system.
        assertNull(monitor.getStateFile(stateFile));

        // The task should be advanced to Ac state
        Mockito.verify(attrOps).updateProcessingState(100L, ProcessingState.ALGORITHM_COMPLETE);

        // The PipelineExecutor should have been asked to submit the task for persisting.
        Mockito.verify(pipelineExecutor).persistTaskResults(pipelineTask);
    }

    // Test automatic resubmission of a task that's almost but not quite finished
    // according to the number of failed subtasks.
    @Test
    public void testAutoResubmit()
        throws ConfigurationException, IOException, InterruptedException {

        int taskCount = lifecycleManager.taskRequestSize();
        assertEquals(0, taskCount);
        resources.setMaxFailedSubtaskCount(4);
        resources.setMaxAutoResubmits(3);
        Mockito.when(pipelineTask.getAutoResubmitCount()).thenReturn(1);
        Mockito.when(pipelineTask.getState()).thenReturn(PipelineTask.State.ERROR);
        Mockito.doNothing()
            .when(pipelineExecutor)
            .restartFailedTasks(ArgumentMatchers.anyCollection(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.isA(RunMode.class));
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

        // The pipeline task state was set to ERROR
        Mockito.verify(pipelineOperations).setTaskState(pipelineTask, PipelineTask.State.ERROR);

        // The pipeline executor method to restart tasks was called
        Mockito.verify(pipelineExecutor)
            .restartFailedTasks(List.of(pipelineTask), false, RunMode.RESUBMIT);
    }

    // Test the case where automatic submission would be called except that the
    // task is out of automatic resubmits.
    @Test
    public void testOutOfAutoResubmits()
        throws ConfigurationException, IOException, InterruptedException {

        Mockito.when(pipelineTask.getMaxFailedSubtaskCount()).thenReturn(4);
        Mockito.when(pipelineTask.getMaxAutoResubmits()).thenReturn(3);
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
        Mockito.verify(pipelineOperations).setTaskState(pipelineTask, PipelineTask.State.ERROR);

        // There are no task requests in the queue.
        assertEquals(0, lifecycleManager.taskRequestSize());
    }
}
