package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.TaskConfiguration;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
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
    private PipelineTaskDataOperations pipelineTaskDataOperations;
    private TaskConfiguration taskConfigurationManager;
    private PipelineInstance pipelineInstance;
    private QstatParser qstatParser;
    private static Future<Void> futureVoid;
    private RemoteJob completeRemoteJob;
    private RemoteJob incompleteRemoteJob;
    private RemoteJobInformation incompleteRemoteJobInformation;

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
        pipelineTaskDataOperations = mock(PipelineTaskDataOperations.class);
        taskConfigurationManager = mock(TaskConfiguration.class);
        executor = new GenericRemoteExecutor(pipelineTask);
        futureVoid = mock(Future.class);
        qstatParser = mock(QstatParser.class);

        when(pipelineTask.getPipelineInstanceId()).thenReturn(10L);
        when(pipelineTask.getModuleName()).thenReturn("modulename");
        when(pipelineTask.getId()).thenReturn(50L);
        when(pipelineTask.taskBaseName()).thenReturn("10-50-modulename");
        when(
            pipelineTaskOperations.pipelineDefinitionNode(ArgumentMatchers.any(PipelineTask.class)))
                .thenReturn(new PipelineDefinitionNode());
        when(pipelineTaskOperations.pipelineInstance(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineInstance);
        SubtaskCounts subtaskCounts = new SubtaskCounts(500, 400, 0);
        when(pipelineTaskDataOperations.subtaskCounts(pipelineTask)).thenReturn(subtaskCounts);
        when(pipelineInstance.getId()).thenReturn(10L);
        when(taskConfigurationManager.getSubtaskCount()).thenReturn(500);
        when(futureVoid.get()).thenReturn(null);
        completeRemoteJob = new RemoteJob();
        completeRemoteJob.setFinished(true);
        completeRemoteJob.setJobId(1234567L);
        incompleteRemoteJob = new RemoteJob();
        incompleteRemoteJob.setFinished(false);
        incompleteRemoteJob.setJobId(1234568L);
        incompleteRemoteJobInformation = new RemoteJobInformation("test1", "test2");
        incompleteRemoteJobInformation.setJobId(1234568L);
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
            gExecutor.getPbsParameters());
        assertEquals(RemoteNodeDescriptor.BROADWELL,
            gExecutor.getPbsParameters().getArchitecture());

        // The correct calls should have occurred.
        assertEquals(pipelineTask, gExecutor.pipelineTask());
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
            gExecutor.getPbsParameters());
        assertEquals(RemoteNodeDescriptor.ROME, gExecutor.getPbsParameters().getArchitecture());

        // The correct calls should have occurred, including that
        // the state file gets the full number of subtasks and no information about failed
        // or complete (that has to be generated at runtime by the remote job itself).
        assertEquals(pipelineTask, gExecutor.pipelineTask());
    }

    @Test
    public void testResumeMonitoringNoRemoteJobs() {
        Mockito.when(pipelineTaskDataOperations.remoteJobs(pipelineTask))
            .thenReturn(new HashSet<>());
        RemoteExecutor remoteExecutor = new GenericRemoteExecutor(pipelineTask);
        assertFalse(remoteExecutor.resumeMonitoring());
        assertTrue(CollectionUtils.isEmpty(remoteExecutor.getRemoteJobsInformation()));
    }

    @Test
    public void testResumeMonitoringNoIncompleteRemoteJobs() {
        Mockito.when(pipelineTaskDataOperations.remoteJobs(pipelineTask))
            .thenReturn(Set.of(completeRemoteJob));
        RemoteExecutor remoteExecutor = new GenericRemoteExecutor(pipelineTask);
        assertFalse(remoteExecutor.resumeMonitoring());
        assertTrue(CollectionUtils.isEmpty(remoteExecutor.getRemoteJobsInformation()));
    }

    @Test
    public void testResumeMonitoring() {
        Mockito.when(pipelineTaskDataOperations.remoteJobs(pipelineTask))
            .thenReturn(Set.of(completeRemoteJob, incompleteRemoteJob));
        Mockito.when(qstatParser.remoteJobInformation(incompleteRemoteJob))
            .thenReturn(incompleteRemoteJobInformation);
        RemoteExecutor remoteExecutor = new GenericRemoteExecutor(pipelineTask);
        assertTrue(remoteExecutor.resumeMonitoring());
        assertFalse(CollectionUtils.isEmpty(remoteExecutor.getRemoteJobsInformation()));
        RemoteJobInformation remoteJobInformation = remoteExecutor.getRemoteJobsInformation()
            .get(0);
        assertEquals("test1", remoteJobInformation.getLogFile());
        assertEquals("test2", remoteJobInformation.getJobName());
        assertEquals(1234568L, remoteJobInformation.getJobId());
    }

    private void checkPbsParameterValues(
        PipelineDefinitionNodeExecutionResources executionResources, PbsParameters pParameters) {

        assertEquals(executionResources.getQueueName(), pParameters.getQueueName());
        assertEquals(executionResources.getMinCoresPerNode(), pParameters.getMinCoresPerNode());
        assertEquals(executionResources.getMinGigsPerNode(), pParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(2, pParameters.getRequestedNodeCount());
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

        public GenericRemoteExecutor(PipelineTask pipelineTask) {
            super(pipelineTask);
        }

        public PipelineTask pipelineTask() {
            return pipelineTask;
        }

        @Override
        public PbsParameters generatePbsParameters(
            PipelineDefinitionNodeExecutionResources remoteParameters, int totalSubtaskCount) {
            this.totalSubtaskCount = totalSubtaskCount;
            PbsParameters pbsParameters = remoteParameters.pbsParametersInstance();
            pbsParameters.populateResourceParameters(remoteParameters, totalSubtaskCount);
            return pbsParameters;
        }

        @Override
        public void addToMonitor() {
        }

        @Override
        protected void submitForExecution() {
            addToMonitor();
        }

        @Override
        protected PipelineTaskOperations pipelineTaskOperations() {
            return pipelineTaskOperations;
        }

        @Override
        protected PipelineTaskDataOperations pipelineTaskDataOperations() {
            return pipelineTaskDataOperations;
        }

        @Override
        protected QstatParser qstatParser() {
            return qstatParser;
        }
    }
}
