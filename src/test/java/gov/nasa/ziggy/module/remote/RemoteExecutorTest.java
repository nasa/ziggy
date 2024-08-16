package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.module.TaskConfiguration;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.SingleThreadExecutor;

/**
 * Class that provides unit tests for the {@link RemoteExecutor} abstract class.
 *
 * @author PT
 */
public class RemoteExecutorTest {

    private GenericRemoteExecutor executor;
    private PipelineTask pipelineTask;
    private PipelineTaskOperations pipelineTaskOperations;
    private TaskConfiguration taskConfigurationManager;
    private PipelineInstance pipelineInstance;
    private static Future<Void> futureVoid;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(resultsDirPropertyRule);

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws InterruptedException, ExecutionException {

        File taskDir = directoryRule.directory()
            .resolve("task-data")
            .resolve("10-50-modulename")
            .toFile();
        taskDir.mkdirs();
        new File(taskDir, "st-0").mkdirs();
        new File(taskDir, "st-1").mkdirs();

        pipelineTask = mock(PipelineTask.class);
        pipelineInstance = mock(PipelineInstance.class);
        pipelineTaskOperations = mock(PipelineTaskOperations.class);
        taskConfigurationManager = mock(TaskConfiguration.class);
        executor = new GenericRemoteExecutor(pipelineTask);
        futureVoid = mock(Future.class);

        when(pipelineTask.getPipelineInstanceId()).thenReturn(10L);
        when(pipelineTask.getModuleName()).thenReturn("modulename");
        when(pipelineTask.getId()).thenReturn(50L);
        when(pipelineTask.taskBaseName()).thenReturn("10-50-modulename");
        when(
            pipelineTaskOperations.pipelineDefinitionNode(ArgumentMatchers.any(PipelineTask.class)))
                .thenReturn(new PipelineDefinitionNode());
        when(pipelineTaskOperations.pipelineInstance(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineInstance);
        when(pipelineInstance.getId()).thenReturn(10L);
        when(taskConfigurationManager.getSubtaskCount()).thenReturn(500);
        when(pipelineTask.getTotalSubtaskCount()).thenReturn(500);
        when(pipelineTask.getCompletedSubtaskCount()).thenReturn(400);
        when(futureVoid.get()).thenReturn(null);
    }

    @After
    public void teardown() throws IOException {
        SingleThreadExecutor.reset();
        DatabaseService.reset();
    }

    @Test
    public void testExecuteAlgorithmFirstIteration() {

        when(pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(remoteExecutionConfigurationForPipelineTask());

        executor.submitAlgorithm(taskConfigurationManager);

        // The correct call to generate PBS parameters should have occurred.
        GenericRemoteExecutor gExecutor = executor;
        assertEquals(500, gExecutor.totalSubtaskCount);
        checkPbsParameterValues(remoteExecutionConfigurationForPipelineTask(),
            gExecutor.pbsParameters);
        assertEquals(RemoteNodeDescriptor.BROADWELL, gExecutor.pbsParameters.getArchitecture());

        // The correct calls should have occurred.
        assertEquals(pipelineTask, gExecutor.pipelineTask());
        StateFile stateFile = gExecutor.stateFile();
        assertEquals(10L, stateFile.getPipelineInstanceId());
        assertEquals(50L, stateFile.getPipelineTaskId());
        assertEquals(500, stateFile.getNumTotal());
        assertEquals(0, stateFile.getNumFailed());
        assertEquals(0, stateFile.getNumComplete());

        checkStateFileValues(gExecutor.pbsParameters, stateFile);

        assertEquals(stateFile, gExecutor.monitoredStateFile);
    }

    @Test
    public void testExecuteAlgorithmLaterIteration() {

        when(pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(remoteExecutionConfigurationFromDatabase());

        executor.submitAlgorithm(null);

        // The correct call to generate PBS parameters should have occurred,
        // including the fact that the number of remaining subtasks should be
        // lower.
        GenericRemoteExecutor gExecutor = executor;
        assertEquals(100, gExecutor.totalSubtaskCount);
        checkPbsParameterValues(remoteExecutionConfigurationFromDatabase(),
            gExecutor.pbsParameters);
        assertEquals(RemoteNodeDescriptor.ROME, gExecutor.pbsParameters.getArchitecture());

        // The correct calls should have occurred, including that
        // the state file gets the full number of subtasks and no information about failed
        // or complete (that has to be generated at runtime by the remote job itself).
        assertEquals(pipelineTask, gExecutor.pipelineTask());
        StateFile stateFile = gExecutor.stateFile();
        assertEquals(10L, stateFile.getPipelineInstanceId());
        assertEquals(50L, stateFile.getPipelineTaskId());
        assertEquals(500, stateFile.getNumTotal());
        assertEquals(0, stateFile.getNumFailed());
        assertEquals(0, stateFile.getNumComplete());

        checkStateFileValues(gExecutor.pbsParameters, stateFile);

        assertEquals(stateFile, gExecutor.monitoredStateFile);
    }

    private void checkPbsParameterValues(
        PipelineDefinitionNodeExecutionResources executionResources, PbsParameters pParameters) {

        assertEquals(executionResources.getQueueName(), pParameters.getQueueName());
        assertEquals(executionResources.getMinCoresPerNode(), pParameters.getMinCoresPerNode());
        assertEquals(executionResources.getMinGigsPerNode(), pParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(executionResources.getMaxNodes(), pParameters.getRequestedNodeCount());
    }

    private void checkStateFileValues(PbsParameters pParameters, StateFile stateFile) {
        assertEquals(pParameters.getArchitecture().getNodeName(),
            stateFile.getRemoteNodeArchitecture());
        assertEquals(pParameters.getQueueName(), stateFile.getQueueName());
        assertEquals(pParameters.getMinGigsPerNode(), stateFile.getMinGigsPerNode(), 1e-3);
        assertEquals(pParameters.getMinCoresPerNode(), stateFile.getMinCoresPerNode());
        assertEquals(pParameters.getRequestedNodeCount(), stateFile.getRequestedNodeCount());
    }

    // Parameters that come from the PipelineTask
    private PipelineDefinitionNodeExecutionResources remoteExecutionConfigurationForPipelineTask() {

        PipelineDefinitionNodeExecutionResources executionResources = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setSubtaskMaxWallTimeHours(5.0);
        executionResources.setSubtaskTypicalWallTimeHours(1.0);
        executionResources.setGigsPerSubtask(4.5);
        executionResources.setQueueName("normal");
        executionResources.setRemoteNodeArchitecture("bro");
        executionResources.setSubtasksPerCore(5.0);
        executionResources.setMaxNodes(10);
        executionResources.setMinGigsPerNode(100.0);
        executionResources.setMinCoresPerNode(50);
        return executionResources;
    }

    // Different parameters that come from the database

    private PipelineDefinitionNodeExecutionResources remoteExecutionConfigurationFromDatabase() {
        PipelineDefinitionNodeExecutionResources executionResources = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setSubtaskMaxWallTimeHours(6.0);
        executionResources.setSubtaskTypicalWallTimeHours(2.0);
        executionResources.setGigsPerSubtask(8);
        executionResources.setQueueName("long");
        executionResources.setRemoteNodeArchitecture("rom_ait");
        executionResources.setSubtasksPerCore(1.0);
        executionResources.setMaxNodes(12);
        executionResources.setMinGigsPerNode(101.0);
        executionResources.setMinCoresPerNode(51);
        return executionResources;
    }

    private class GenericRemoteExecutor extends RemoteExecutor {

        int totalSubtaskCount;
        PbsParameters pbsParameters;
        StateFile monitoredStateFile;

        public GenericRemoteExecutor(PipelineTask pipelineTask) {
            super(pipelineTask);
        }

        public PipelineTask pipelineTask() {
            return pipelineTask;
        }

        public StateFile stateFile() {
            return getStateFile();
        }

        @Override
        public PbsParameters generatePbsParameters(
            PipelineDefinitionNodeExecutionResources remoteParameters, int totalSubtaskCount) {
            this.totalSubtaskCount = totalSubtaskCount;
            pbsParameters = remoteParameters.pbsParametersInstance();
            return pbsParameters;
        }

        @Override
        public void addToMonitor(StateFile stateFile) {
            monitoredStateFile = stateFile;
        }

        @Override
        protected void submitForExecution(StateFile stateFile) {
            addToMonitor(stateFile);
        }

        @Override
        protected PipelineTaskOperations pipelineTaskOperations() {
            return pipelineTaskOperations;
        }
    }
}
