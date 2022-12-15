package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.services.config.PropertyNames.RESULTS_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.module.TaskConfigurationManager;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.SingleThreadExecutor;
import gov.nasa.ziggy.uow.TaskConfigurationParameters;

/**
 * Class that provides unit tests for the {@link RemoteExecutor} abstract class.
 *
 * @author PT
 */
public class RemoteExecutorTest {

    private ParameterSetCrud parameterSetCrud;
    private GenericRemoteExecutor executor;
    private PipelineTask pipelineTask;
    private ProcessingSummaryOperations crud;
    private TaskConfigurationManager taskConfigurationManager;
    private PipelineInstance pipelineInstance;
    private ProcessingSummary taskAttr;
    private DatabaseService databaseService;
    private static Future<Void> futureVoid;
    private static TaskConfigurationParameters tcp;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR_PROP_NAME,
        directoryRule);

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

        parameterSetCrud = mock(ParameterSetCrud.class);
        pipelineTask = mock(PipelineTask.class);
        pipelineInstance = mock(PipelineInstance.class);
        crud = mock(ProcessingSummaryOperations.class);
        taskConfigurationManager = mock(TaskConfigurationManager.class);
        taskAttr = mock(ProcessingSummary.class);
        executor = new GenericRemoteExecutor(pipelineTask);
        executor.setParameterSetCrud(parameterSetCrud);
        executor.setProcessingSummaryOperations(crud);
        databaseService = mock(DatabaseService.class);
        DatabaseService.setInstance(databaseService);
        futureVoid = mock(Future.class);
        tcp = mock(TaskConfigurationParameters.class);

        when(pipelineTask.getParameters(RemoteParameters.class, false))
            .thenReturn(remoteParametersForPipelineTask());
        when(pipelineTask.getParameters(TaskConfigurationParameters.class)).thenReturn(tcp);
        when(tcp.getMaxFailedSubtaskCount()).thenReturn(0);
        when(pipelineTask.getPipelineInstance()).thenReturn(pipelineInstance);
        when(pipelineTask.pipelineInstanceId()).thenReturn(10L);
        when(pipelineTask.getModuleName()).thenReturn("modulename");
        when(pipelineTask.getId()).thenReturn(50L);
        when(pipelineTask.taskBaseName()).thenReturn("10-50-modulename");
        when(pipelineInstance.getId()).thenReturn(10L);
        when(parameterSetCrud.retrieveRemoteParameters(pipelineTask))
            .thenReturn(remoteParametersFromDatabase());
        when(taskConfigurationManager.numSubTasks()).thenReturn(500);
        when(crud.processingSummary(50L)).thenReturn(taskAttr);
        when(taskAttr.getTotalSubtaskCount()).thenReturn(500);
        when(taskAttr.getCompletedSubtaskCount()).thenReturn(400);
        when(futureVoid.get()).thenReturn(null);
    }

    @After
    public void teardown() throws IOException {
        SingleThreadExecutor.reset();
        DatabaseService.reset();
    }

    @Test
    public void testExecuteAlgorithmFirstIteration() {
        executor.submitAlgorithm(taskConfigurationManager);

        // The correct call to generate PBS parameters should have occurred.
        GenericRemoteExecutor gExecutor = executor;
        assertEquals(500, gExecutor.totalSubtaskCount);
        checkPbsParameterValues(remoteParametersForPipelineTask(), gExecutor.pbsParameters);
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

        // Make sure that the things that should not get called did not, in fact,
        // get called.
        verify(crud, never()).processingSummary(any(long.class));
        verify(parameterSetCrud, never()).retrieveRemoteParameters(any(PipelineTask.class));

    }

    @Test
    public void testExecuteAlgorithmLaterIteration() {

        executor.submitAlgorithm(null);

        // The CRUDs should have been called
        verify(crud).processingSummary(50L);
        verify(parameterSetCrud).retrieveRemoteParameters(pipelineTask);

        // The correct call to generate PBS parameters should have occurred,
        // including the fact that the number of remaining subtasks should be
        // lower.
        GenericRemoteExecutor gExecutor = executor;
        assertEquals(100, gExecutor.totalSubtaskCount);
        checkPbsParameterValues(remoteParametersFromDatabase(), gExecutor.pbsParameters);
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

    private void checkPbsParameterValues(RemoteParameters rParameters, PbsParameters pParameters) {

        assertEquals(rParameters.getQueueName(), pParameters.getQueueName());
        assertEquals(rParameters.getMinCoresPerNode(),
            Integer.toString(pParameters.getMinCoresPerNode()));
        assertEquals(rParameters.getMinGigsPerNode(),
            Integer.toString(pParameters.getMinGigsPerNode()));
        assertEquals(rParameters.getMaxNodes(),
            Integer.toString(pParameters.getRequestedNodeCount()));
    }

    private void checkStateFileValues(PbsParameters pParameters, StateFile stateFile) {
        assertEquals(pParameters.getArchitecture().getNodeName(),
            stateFile.getRemoteNodeArchitecture());
        assertEquals(pParameters.getQueueName(), stateFile.getQueueName());
        assertEquals(pParameters.getMinGigsPerNode(), stateFile.getMinGigsPerNode());
        assertEquals(pParameters.getMinCoresPerNode(), stateFile.getMinCoresPerNode());
        assertEquals(pParameters.getRequestedNodeCount(), stateFile.getRequestedNodeCount());
    }

    // Parameters that come from the PipelineTask
    private RemoteParameters remoteParametersForPipelineTask() {

        RemoteParameters parameters = new RemoteParameters();
        parameters.setEnabled(true);
        parameters.setSubtaskMaxWallTimeHours(5.0);
        parameters.setSubtaskTypicalWallTimeHours(1.0);
        parameters.setGigsPerSubtask(4.5);
        parameters.setQueueName("normal");
        parameters.setRemoteNodeArchitecture("bro");
        parameters.setSubtasksPerCore("5");
        parameters.setMaxNodes("10");
        parameters.setMinGigsPerNode("100");
        parameters.setMinCoresPerNode("50");
        return parameters;
    }

    // Different parameters that come from the database

    private RemoteParameters remoteParametersFromDatabase() {
        RemoteParameters parameters = new RemoteParameters();
        parameters.setEnabled(true);
        parameters.setSubtaskMaxWallTimeHours(6.0);
        parameters.setSubtaskTypicalWallTimeHours(2.0);
        parameters.setGigsPerSubtask(8);
        parameters.setQueueName("long");
        parameters.setRemoteNodeArchitecture("rom_ait");
        parameters.setSubtasksPerCore("1");
        parameters.setMaxNodes("12");
        parameters.setMinGigsPerNode("101");
        parameters.setMinCoresPerNode("51");
        return parameters;

    }

    private static class GenericRemoteExecutor extends RemoteExecutor {

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
        public PbsParameters generatePbsParameters(RemoteParameters remoteParameters,
            int totalSubtaskCount) {
            this.totalSubtaskCount = totalSubtaskCount;
            pbsParameters = remoteParameters.pbsParametersInstance();
            return pbsParameters;
        }

        @Override
        public void addToMonitor(StateFile stateFile) {
            monitoredStateFile = stateFile;
        }

        @Override
        public void setParameterSetCrud(ParameterSetCrud parameterSetCrud) {
            super.setParameterSetCrud(parameterSetCrud);
        }

        @Override
        public void setProcessingSummaryOperations(ProcessingSummaryOperations crud) {
            super.setProcessingSummaryOperations(crud);
        }

        @Override
        protected void submitForExecution(StateFile stateFile) throws Exception {
            // do nothing
        }
    }

}
