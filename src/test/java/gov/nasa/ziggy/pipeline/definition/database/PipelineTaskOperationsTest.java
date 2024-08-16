package gov.nasa.ziggy.pipeline.definition.database;

import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_QUEUE_COMMAND_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.hibernate.Hibernate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.module.remote.QueueCommandManagerTest;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskExecutionLog;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.util.ZiggyCollectionUtils;

/**
 * Unit tests for {@link PipelineTaskOperations}. Legacy methods are not tested, but all methods
 * added since Ziggy version 0.3.0 are tested.
 * <p>
 * In addition to {@link PipelineTaskOperations}, there are implicit tests of some methods in
 * {@link PipelineInstanceCrud} and {@link TestCounts}.
 *
 * @author PT
 */
public class PipelineTaskOperationsTest {

    private PipelineTask task1, task2, task3, task4;
    private PipelineInstance pipelineInstance;
    private PipelineInstanceNode newInstanceNode;
    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineDefinitionNode definitionNode;
    private PipelineDefinition pipelineDefinition;
    private PipelineTaskOperations pipelineTaskOperations;
    private PipelineInstanceOperations pipelineInstanceOperations;
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations;
    private QueueCommandManager cmdManager;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule queueCommandClassPropertyRule = new ZiggyPropertyRule(
        REMOTE_QUEUE_COMMAND_CLASS, "gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests");

    @Before
    public void setUp() {

        pipelineTaskOperations = Mockito.spy(PipelineTaskOperations.class);
        pipelineInstanceOperations = Mockito.spy(PipelineInstanceOperations.class);
        pipelineInstanceNodeOperations = Mockito.spy(PipelineInstanceNodeOperations.class);
        cmdManager = spy(QueueCommandManager.newInstance());
        when(pipelineTaskOperations.queueCommandManager()).thenReturn(cmdManager);
        when(pipelineInstanceOperations.pipelineTaskOperations())
            .thenReturn(pipelineTaskOperations);
    }

    private void setUpSingleModulePipeline() {
        PipelineOperationsTestUtils testUtils = new PipelineOperationsTestUtils();
        testUtils.setUpSingleModulePipeline();
        pipelineDefinition = testUtils.pipelineDefinition();
        definitionNode = testUtils.pipelineDefinitionNode();
        pipelineInstance = testUtils.pipelineInstance();
        pipelineInstanceNode = testUtils.pipelineInstanceNode();
        task1 = testUtils.getPipelineTasks().get(0);
        task2 = testUtils.getPipelineTasks().get(1);
    }

    @Test
    public void testPipelineInstanceNode() {
        setUpSingleModulePipeline();
        PipelineInstanceNode pipelineInstanceNode = pipelineTaskOperations
            .pipelineInstanceNode(task1);
        assertNotNull(pipelineInstanceNode);
        assertEquals(1L, pipelineInstanceNode.getId().longValue());
    }

    @Test
    public void testPipelineInstance() {
        setUpSingleModulePipeline();
        PipelineInstance pipelineInstance = pipelineTaskOperations.pipelineInstance(task1);
        assertNotNull(pipelineInstance);
        assertEquals(1L, pipelineInstance.getId().longValue());
    }

    @Test
    public void testPipelineDefinitionNode() {
        setUpSingleModulePipeline();
        PipelineDefinitionNode node = pipelineTaskOperations.pipelineDefinitionNode(task1);
        assertNotNull(node);
        assertEquals(1L, node.getId().longValue());
    }

    /**
     * Tests that the {@link PipelineTaskOperations#updateProcessingStep(long, ProcessingStep)}
     * method performs correctly in the case where all tasks run to completion without errors. In
     * particular, the pipeline task execution clocks should start and stop at the correct times,
     * the pipeline instance task execution clock should start and stop at the correct times, and
     * the pipeline instance and pipeline instance node task counts should be correct.
     */
    @Test
    public void testUpdateProcessingStepNoErrors() {

        setUpSingleModulePipeline();

        // Check initial states.
        List<PipelineTask> databaseTasks = testOperations.allPipelineTasks();

        for (PipelineTask task : databaseTasks) {
            assertTrue(task.getCurrentExecutionStartTimeMillis() <= 0);
            assertEquals(ProcessingStep.INITIALIZING, task.getProcessingStep());
        }
        PipelineInstance instance = testOperations.allPipelineInstances().get(0);
        assertTrue(instance.getCurrentExecutionStartTimeMillis() <= 0);
        assertEquals(PipelineInstance.State.INITIALIZED, instance.getState());

        TaskCounts counts = pipelineInstanceOperations.taskCounts(instance);
        PipelineOperationsTestUtils.testTaskCounts(2, 2, 0, 0, counts);

        counts = pipelineInstanceNodeOperations.taskCounts(pipelineInstanceNode);
        PipelineOperationsTestUtils.testTaskCounts(2, 2, 0, 0, counts);

        // Move the tasks one at a time to the WAITING_TO_RUN step.
        PipelineTask updatedTask = pipelineTaskOperations.updateProcessingStep(databaseTasks.get(0),
            ProcessingStep.WAITING_TO_RUN);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertEquals(ProcessingStep.WAITING_TO_RUN, updatedTask.getProcessingStep());
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 2, 0, 0, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 2, 0, 0, counts);
        assertTrue(updatedTask.getCurrentExecutionStartTimeMillis() > 0);
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);

        updatedTask = pipelineTaskOperations.updateProcessingStep(databaseTasks.get(1),
            ProcessingStep.WAITING_TO_RUN);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertEquals(ProcessingStep.WAITING_TO_RUN, updatedTask.getProcessingStep());
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 2, 0, 0, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 2, 0, 0, counts);
        assertTrue(updatedTask.getCurrentExecutionStartTimeMillis() > 0);
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);

        // Make sure that the state changes are actually persisted to the database
        List<PipelineTask> submittedTasks = testOperations.allPipelineTasks();
        for (PipelineTask task : submittedTasks) {
            assertEquals(ProcessingStep.WAITING_TO_RUN, task.getProcessingStep());
            assertTrue(task.getCurrentExecutionStartTimeMillis() > 0);
        }
        instance = testOperations.allPipelineInstances().get(0);
        assertEquals(PipelineInstance.State.PROCESSING, instance.getState());
        assertTrue(instance.getCurrentExecutionStartTimeMillis() > 0);

        // Move the tasks to the EXECUTING step.
        pipelineTaskOperations.updateProcessingStep(submittedTasks.get(0),
            ProcessingStep.EXECUTING);
        updatedTask = pipelineTaskOperations.updateProcessingStep(submittedTasks.get(1),
            ProcessingStep.EXECUTING);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 0, 0, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 0, 0, counts);

        // Move both tasks to the COMPLETE step.
        List<PipelineTask> tasks = testOperations.allPipelineTasks();
        pipelineTaskOperations.updateProcessingStep(tasks.get(0), ProcessingStep.COMPLETE);
        updatedTask = pipelineTaskOperations.updateProcessingStep(tasks.get(1),
            ProcessingStep.COMPLETE);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.COMPLETED, pipelineInstance.getState());
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() <= 0);
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 2, 0, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 2, 0, counts);

        // Make sure the tasks got their clocks stopped.
        tasks = testOperations.allPipelineTasks();
        assertTrue(tasks.get(0).getCurrentExecutionStartTimeMillis() <= 0);
        assertTrue(tasks.get(1).getCurrentExecutionStartTimeMillis() <= 0);
    }

    /**
     * Tests that the correct state transitions are performed when there are tasks that error out.
     */
    @Test
    public void testUpdateProcessingStepWithErrors() {

        setUpSingleModulePipeline();

        // Move the tasks to the EXECUTING step.
        pipelineTaskOperations.updateProcessingStep(task1, ProcessingStep.EXECUTING);
        pipelineTaskOperations.updateProcessingStep(task2, ProcessingStep.EXECUTING);

        // Move one task as errored.
        PipelineTask updatedTask = pipelineTaskOperations.taskErrored(task1);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());

        // Check that the updated states are all correct.
        assertEquals(PipelineInstance.State.ERRORS_RUNNING, pipelineInstance.getState());
        assertTrue(updatedTask.isError());
        TaskCounts counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 0, 1, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 0, 1, counts);

        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);
        assertTrue(updatedTask.getCurrentExecutionStartTimeMillis() <= 0);

        // Move the other task to the COMPLETE step.
        updatedTask = pipelineTaskOperations.updateProcessingStep(task2, ProcessingStep.COMPLETE);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());

        // Check that the updated states are all correct.
        assertEquals(PipelineInstance.State.ERRORS_STALLED, pipelineInstance.getState());
        assertEquals(ProcessingStep.COMPLETE, updatedTask.getProcessingStep());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 1, 1, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 1, 1, counts);

        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() <= 0);
        assertTrue(updatedTask.getCurrentExecutionStartTimeMillis() <= 0);
    }

    /**
     * Tests that the correct state transitions occur when there are multiple nodes in the pipeline.
     */
    @Test
    public void testMultipleInstanceNodesNoErrors() {

        setUpSingleModulePipeline();

        // Create a new instance node with 2 tasks in it and attach it to the instance.
        PipelineModuleDefinition newModuleDefinition = testOperations
            .merge(new PipelineModuleDefinition("dummy6"));
        PipelineDefinitionNode newDefinitionNode = testOperations
            .merge(new PipelineDefinitionNode("dummy7", "dummy7"));
        definitionNode.setNextNodes(List.of(newDefinitionNode));
        pipelineDefinition.setRootNodes(List.of(definitionNode));
        testOperations.merge(definitionNode);
        newInstanceNode = testOperations
            .merge(new PipelineInstanceNode(newDefinitionNode, newModuleDefinition));
        pipelineInstance.setEndNode(newInstanceNode);
        pipelineInstance.addPipelineInstanceNode(newInstanceNode);
        testOperations.merge(pipelineInstance);

        // Mark the first 2 tasks as complete and check that all the correct states are
        // generated. Then create 2 new pipeline tasks in the second instance node.
        pipelineTaskOperations.updateProcessingStep(task1, ProcessingStep.COMPLETE);
        PipelineTask updatedTask = pipelineTaskOperations.updateProcessingStep(task2,
            ProcessingStep.COMPLETE);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        TaskCounts counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 2, 0, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 2, 0, counts);
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);

        task3 = new PipelineTask(pipelineInstance, newInstanceNode);
        task4 = new PipelineTask(pipelineInstance, newInstanceNode);
        testOperations.persistPipelineTasks(List.of(task3, task4));
        newInstanceNode.addPipelineTask(task3);
        newInstanceNode.addPipelineTask(task4);
        newInstanceNode = testOperations.merge(newInstanceNode);

        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(4, 2, 2, 0, counts);

        counts = pipelineInstanceNodeOperations.taskCounts(newInstanceNode);
        PipelineOperationsTestUtils.testTaskCounts(2, 2, 0, 0, counts);

        // Move the new tasks to the EXECUTING step.
        pipelineTaskOperations.updateProcessingStep(task3, ProcessingStep.EXECUTING);
        updatedTask = pipelineTaskOperations.updateProcessingStep(task4, ProcessingStep.EXECUTING);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(4, 0, 2, 0, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 0, 0, counts);
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);

        // Move the tasks to COMPLETE.
        pipelineTaskOperations.updateProcessingStep(task3, ProcessingStep.COMPLETE);
        updatedTask = pipelineTaskOperations.updateProcessingStep(task4, ProcessingStep.COMPLETE);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.COMPLETED, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(4, 0, 4, 0, counts);
        counts = pipelineInstanceNodeOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(updatedTask));
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 2, 0, counts);
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() <= 0);
    }

    @Test
    public void testMultipleInstanceNodesWithErrors() {

        setUpSingleModulePipeline();

        // Create a new instance node with 2 tasks in it and attach it to the instance.
        PipelineModuleDefinition newModuleDefinition = testOperations
            .merge(new PipelineModuleDefinition("dummy6"));
        PipelineDefinitionNode newDefinitionNode = new PipelineDefinitionNode("dummy7", "dummy7");
        definitionNode.setNextNodes(ZiggyCollectionUtils.mutableListOf(newDefinitionNode));
        pipelineDefinition.setRootNodes(ZiggyCollectionUtils.mutableListOf(definitionNode));
        newDefinitionNode = testOperations.merge(newDefinitionNode);
        definitionNode = testOperations.merge(definitionNode);
        pipelineDefinition = testOperations.merge(pipelineDefinition);
        newInstanceNode = testOperations
            .merge(new PipelineInstanceNode(newDefinitionNode, newModuleDefinition));
        pipelineInstance.setEndNode(newInstanceNode);
        pipelineInstance = testOperations.merge(pipelineInstance);

        // Move the first task into the EXECUTING step.
        PipelineTask updatedTask = pipelineTaskOperations.updateProcessingStep(task1,
            ProcessingStep.EXECUTING);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);

        // Now mark the first task as errored.
        updatedTask = pipelineTaskOperations.taskErrored(task1);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);
        assertEquals(PipelineInstance.State.ERRORS_RUNNING, pipelineInstance.getState());
        TaskCounts counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 1, 0, 1, counts);

        // Move the second task into EXECUTING.
        updatedTask = pipelineTaskOperations.updateProcessingStep(task2, ProcessingStep.EXECUTING);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);

        // When the second task completes, the instance should go to ERRORS_STALLED even
        // though there are pipeline instance nodes that have not yet been run.
        updatedTask = pipelineTaskOperations.updateProcessingStep(task2, ProcessingStep.COMPLETE);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(updatedTask.getPipelineInstanceId());
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() <= 0);
        assertEquals(PipelineInstance.State.ERRORS_STALLED, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 1, 1, counts);
    }

    @Test
    public void testCreateRemoteJobsFromQstat() {

        PipelineTask task = createPipelineTask();
        pipelineTaskOperations.createRemoteJobsFromQstat(task.getId());

        // retrieve the task from the database
        Set<RemoteJob> remoteJobs = testOperations.remoteJobs(1L);
        assertEquals(3, remoteJobs.size());
        assertTrue(remoteJobs.contains(new RemoteJob(9101154)));
        assertTrue(remoteJobs.contains(new RemoteJob(9102337)));
        assertTrue(remoteJobs.contains(new RemoteJob(6020203)));
        for (RemoteJob job : remoteJobs) {
            assertFalse(job.isFinished());
            assertEquals(0, job.getCostEstimate(), 1e-9);
        }
    }

    @Test
    public void testUpdateJobs() {

        // Create the task with 3 remote jobs
        createPipelineTask();
        pipelineTaskOperations.createRemoteJobsFromQstat(1L);

        mockRemoteJobUpdates();

        PipelineTask task = testOperations.updateJobs(1L);

        // Check for the expected results
        List<RemoteJob> remoteJobs = new ArrayList<>(pipelineTaskOperations.remoteJobs(task));
        RemoteJob job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9101154)));
        assertTrue(job.isFinished());
        assertEquals(20.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9102337)));
        assertFalse(job.isFinished());
        assertEquals(8.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(6020203)));
        assertFalse(job.isFinished());
        assertEquals(0, job.getCostEstimate(), 1e-9);

        // Make sure that the database was also updated
        Set<RemoteJob> databaseRemoteJobs = testOperations.remoteJobs(1L);
        for (RemoteJob remoteJob : databaseRemoteJobs) {
            RemoteJob otherJob = remoteJobs.get(remoteJobs.indexOf(remoteJob));
            assertEquals(otherJob.isFinished(), remoteJob.isFinished());
            assertEquals(otherJob.getCostEstimate(), remoteJob.getCostEstimate(), 1e-9);
        }
    }

    @Test
    public void testUpdateJobsForPipelineInstance() {

        // Create the task with 3 remote jobs
        PipelineTask task = createPipelineTask();
        pipelineTaskOperations.createRemoteJobsFromQstat(1L);

        mockRemoteJobUpdates();
        List<PipelineTask> tasks = testOperations
            .updateJobs(pipelineTaskOperations.pipelineInstance(task));

        assertEquals(1, tasks.size());
        PipelineTask updatedTask = tasks.get(0);

        // Check for the expected results
        List<RemoteJob> remoteJobs = new ArrayList<>(updatedTask.getRemoteJobs());
        RemoteJob job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9101154)));
        assertTrue(job.isFinished());
        assertEquals(20.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9102337)));
        assertFalse(job.isFinished());
        assertEquals(8.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(6020203)));
        assertFalse(job.isFinished());
        assertEquals(0, job.getCostEstimate(), 1e-9);

        // Make sure that the database was also updated
        PipelineTask databaseTask = testOperations.initializeRemoteJobs(1L);
        assertEquals(3, databaseTask.getRemoteJobs().size());
        for (RemoteJob remoteJob : databaseTask.getRemoteJobs()) {
            RemoteJob otherJob = remoteJobs.get(remoteJobs.indexOf(remoteJob));
            assertEquals(otherJob.isFinished(), remoteJob.isFinished());
            assertEquals(otherJob.getCostEstimate(), remoteJob.getCostEstimate(), 1e-9);
        }
    }

    @Test
    public void testTaskIdsForPipelineDefinitionNode() {

        setUpSingleModulePipeline();
        List<Long> taskIds = pipelineTaskOperations.taskIdsForPipelineDefinitionNode(task1);
        assertTrue(taskIds.contains(1L));
        assertTrue(taskIds.contains(2L));
        assertEquals(2, taskIds.size());
    }

    @Test
    public void testSummaryMetrics() {
        PipelineTask pipelineTask = createPipelineTask();
        List<PipelineTaskMetrics> summaryMetrics = new ArrayList<>();
        summaryMetrics.add(new PipelineTaskMetrics("dummy1", 100, Units.TIME));
        summaryMetrics.add(new PipelineTaskMetrics("dummy2", 200, Units.BYTES));
        pipelineTask.getSummaryMetrics().addAll(summaryMetrics);
        pipelineTaskOperations.merge(pipelineTask);
        pipelineTask.getSummaryMetrics().clear();
        List<PipelineTaskMetrics> databaseMetrics = pipelineTaskOperations
            .summaryMetrics(pipelineTask);
        Map<String, PipelineTaskMetrics> metricsByCategory = new HashMap<>();
        for (PipelineTaskMetrics metric : databaseMetrics) {
            metricsByCategory.put(metric.getCategory(), metric);
        }
        PipelineTaskMetrics dummy1Metric = metricsByCategory.get("dummy1");
        assertNotNull(dummy1Metric);
        assertEquals(100L, dummy1Metric.getValue());
        assertEquals(Units.TIME, dummy1Metric.getUnits());
        PipelineTaskMetrics dummy2Metric = metricsByCategory.get("dummy2");
        assertNotNull(dummy2Metric);
        assertEquals(200L, dummy2Metric.getValue());
        assertEquals(Units.BYTES, dummy2Metric.getUnits());
        assertEquals(2, metricsByCategory.size());
    }

    @Test
    public void testExecLogs() {
        PipelineTask pipelineTask = createPipelineTask();
        List<TaskExecutionLog> taskExecutionLogs = new ArrayList<>();
        taskExecutionLogs.add(new TaskExecutionLog("dummy1", 1));
        taskExecutionLogs.add(new TaskExecutionLog("dummy2", 2));
        pipelineTask.getExecLog().addAll(taskExecutionLogs);
        pipelineTaskOperations.merge(pipelineTask);
        pipelineTask.getExecLog().clear();
        List<TaskExecutionLog> execLogs = pipelineTaskOperations.execLogs(pipelineTask);
        Map<String, TaskExecutionLog> logsByWorkerHost = new HashMap<>();
        for (TaskExecutionLog log : execLogs) {
            logsByWorkerHost.put(log.getWorkerHost(), log);
        }
        TaskExecutionLog log1 = logsByWorkerHost.get("dummy1");
        assertNotNull(log1);
        assertEquals(1, log1.getWorkerThread());
        TaskExecutionLog log2 = logsByWorkerHost.get("dummy2");
        assertNotNull(log2);
        assertEquals(2, log2.getWorkerThread());
        assertEquals(2, logsByWorkerHost.size());
    }

    @Test
    public void testInputDataFileTypes() {
        PipelineTask pipelineTask = createPipelineTask();
        Set<DataFileType> inputDataFileTypes = pipelineTaskOperations
            .inputDataFileTypes(pipelineTask);
        assertFalse(CollectionUtils.isEmpty(inputDataFileTypes));
        DataFileType dataFileType = inputDataFileTypes.iterator().next();
        assertEquals("dummy1", dataFileType.getName());
        assertEquals(1, inputDataFileTypes.size());
    }

    @Test
    public void testOutputDataFileTypes() {
        PipelineTask pipelineTask = createPipelineTask();
        Set<DataFileType> outputDataFileTypes = pipelineTaskOperations
            .outputDataFileTypes(pipelineTask);
        assertFalse(CollectionUtils.isEmpty(outputDataFileTypes));
        DataFileType dataFileType = outputDataFileTypes.iterator().next();
        assertEquals("dummy4", dataFileType.getName());
        assertEquals(1, outputDataFileTypes.size());
    }

    @Test
    public void testModelTypes() {
        PipelineTask pipelineTask = createPipelineTask();
        Set<ModelType> modelTypes = pipelineTaskOperations.modelTypes(pipelineTask);
        assertFalse(CollectionUtils.isEmpty(modelTypes));
        ModelType modelType = modelTypes.iterator().next();
        assertEquals("dummy7", modelType.getType());
        assertEquals(1, modelTypes.size());
    }

    @Test
    public void testParameterSets() {
        setUpSingleModulePipeline();
        assertTrue(CollectionUtils.isEmpty(pipelineTaskOperations.parameterSets(task1)));
        persistParameterSets();
        testOperations.bindParameterSets();
        Set<ParameterSet> parameterSets = pipelineTaskOperations.parameterSets(task1);
        Map<String, ParameterSet> parameterSetByName = ParameterSet
            .parameterSetByName(parameterSets);
        Set<String> parameterSetNames = parameterSetByName.keySet();
        assertTrue(parameterSetNames.contains("parameter1"));
        assertTrue(parameterSetNames.contains("parameter2"));
        assertTrue(parameterSetNames.contains("parameter3"));
        assertTrue(parameterSetNames.contains("parameter4"));
        assertEquals(4, parameterSetNames.size());
    }

    private PipelineTask createPipelineTask() {

        PipelineInstance instance = testOperations.merge(new PipelineInstance());
        PipelineModuleDefinition modDef = testOperations.merge(new PipelineModuleDefinition("tps"));
        PipelineDefinitionNode defNode = testOperations
            .merge(new PipelineDefinitionNode(modDef.getName(), "dummy"));
        PipelineInstanceNode instNode = testOperations
            .merge(new PipelineInstanceNode(defNode, modDef));
        DataFileType inputDataFileType = testOperations
            .merge(new DataFileType("dummy1", "dummy2", "dummy3"));
        DataFileType outputDataFileType = testOperations
            .merge(new DataFileType("dummy4", "dummy5", "dummy6"));
        ModelType modelType = new ModelType();
        modelType.setType("dummy7");
        modelType = new ModelCrud().merge(modelType);
        defNode.addAllInputDataFileTypes(List.of(inputDataFileType));
        defNode.addAllOutputDataFileTypes(List.of(outputDataFileType));
        defNode.addAllModelTypes(List.of(modelType));
        defNode = testOperations.merge(defNode);
        PipelineTask task = new PipelineTask(instance, instNode);
        task = pipelineTaskOperations.merge(task);
        instNode.addPipelineTask(task);
        instNode = testOperations.merge(instNode);
        instance.addPipelineInstanceNode(instNode);
        instance.setEndNode(instNode);
        instance = testOperations.merge(instance);

        // Mock up the QueueCommandManager so that it returns 3 jobs for the pipeline task
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "1-1-tps" },
            "9101154.batch user low   1-1-tps.0   5   5 04:00 F 01:34 419%",
            "9102337.batch user low   1-1-tps.1   5   5 04:00 F 01:34 419%",
            "6020203.batch user low   1-1-tps.2   5   5 04:00 F 01:34 419%");

        // add mocking that gets the correct server in each case
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 6020203 9101154 9102337",
            new String[] { "Job:", "Job_Owner" }, "Job: 6020203.batch.example.com",
            "    Job_Owner = user@host.example.com", "Job: 9101154.batch.example.com",
            "    Job_Owner = user@host.example.com", "Job: 9102337.batch.example.com",
            "    Job_Owner = user@host.example.com");

        return task;
    }

    private void mockRemoteJobUpdates() {

        // Set up the QueueCommandManager to inform the operations class that
        // one of the tasks is complete, one is running, and one is still queued
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 9101154",
            new String[] { "Exit_status" }, "    Exit_status = 0");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 9102337",
            new String[] { "Exit_status" }, "");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 6020203",
            new String[] { "Exit_status" }, "");

        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 9101154",
            new String[] { QueueCommandManager.SELECT, QueueCommandManager.WALLTIME },
            "    " + QueueCommandManager.WALLTIME + " = 10:00:00",
            "    " + QueueCommandManager.SELECT + " = 2:model=bro");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 9102337",
            new String[] { QueueCommandManager.SELECT, QueueCommandManager.WALLTIME },
            "    " + QueueCommandManager.WALLTIME + " = 05:00:00",
            "    " + QueueCommandManager.SELECT + " = 2:model=has");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 6020203",
            new String[] { QueueCommandManager.SELECT, QueueCommandManager.WALLTIME }, "");
    }

    private void persistParameterSets() {
        ParameterSet parameterSet = new ParameterSet("parameter1");
        parameterSet.getParameters()
            .add(new Parameter("p1", "p1", ZiggyDataType.ZIGGY_STRING, false));
        testOperations.persist(parameterSet);
        parameterSet = new ParameterSet("parameter2");
        parameterSet.getParameters()
            .add(new Parameter("p2", "p2", ZiggyDataType.ZIGGY_STRING, false));
        testOperations.persist(parameterSet);
        parameterSet = new ParameterSet("parameter3");
        parameterSet.getParameters()
            .add(new Parameter("p3", "p3", ZiggyDataType.ZIGGY_STRING, false));
        testOperations.persist(parameterSet);
        parameterSet = new ParameterSet("parameter4");
        parameterSet.getParameters()
            .add(new Parameter("p4", "p4", ZiggyDataType.ZIGGY_STRING, false));
        testOperations.persist(parameterSet);
    }

    private class TestOperations extends DatabaseOperations {

        public List<PipelineTask> allPipelineTasks() {
            return performTransaction(() -> new PipelineTaskCrud().retrieveAll());
        }

        public List<PipelineInstance> allPipelineInstances() {
            return performTransaction(() -> new PipelineInstanceCrud().retrieveAll());
        }

        public PipelineModuleDefinition merge(PipelineModuleDefinition moduleDefinition) {
            return performTransaction(
                () -> new PipelineModuleDefinitionCrud().merge(moduleDefinition));
        }

        public PipelineDefinitionNode merge(PipelineDefinitionNode pipelineDefinitionNode) {
            return performTransaction(
                () -> new PipelineDefinitionNodeCrud().merge(pipelineDefinitionNode));
        }

        public PipelineInstanceNode merge(PipelineInstanceNode pipelineInstanceNode) {
            return performTransaction(
                () -> new PipelineInstanceNodeCrud().merge(pipelineInstanceNode));
        }

        public PipelineInstance merge(PipelineInstance pipelineInstance) {
            return performTransaction(() -> new PipelineInstanceCrud().merge(pipelineInstance));
        }

        public void persistPipelineTasks(List<PipelineTask> pipelineTasks) {
            performTransaction(() -> new PipelineTaskCrud().persist(pipelineTasks));
        }

        public PipelineDefinition merge(PipelineDefinition pipelineDefinition) {
            return performTransaction(() -> new PipelineDefinitionCrud().merge(pipelineDefinition));
        }

        public DataFileType merge(DataFileType dataFileType) {
            return performTransaction(() -> {
                new DataFileTypeCrud().persist(dataFileType);
                return new DataFileTypeCrud().retrieveByName(dataFileType.getName());
            });
        }

        public Set<RemoteJob> remoteJobs(long taskId) {
            return performTransaction(() -> {
                PipelineTask task = new PipelineTaskCrud().retrieve(taskId);
                Hibernate.initialize(task.getRemoteJobs());
                return task.getRemoteJobs();
            });
        }

        public PipelineTask updateJobs(long taskId) {
            return performTransaction(() -> {
                PipelineTask pipelineTask = new PipelineTaskCrud().retrieve(taskId);
                pipelineTaskOperations.updateJobs(pipelineTask);
                return pipelineTask;
            });
        }

        public List<PipelineTask> updateJobs(PipelineInstance pipelineInstance) {
            return performTransaction(() -> {
                List<PipelineTask> pipelineTasks = pipelineInstanceOperations
                    .updateJobs(pipelineInstance);
                for (PipelineTask pipelineTask : pipelineTasks) {
                    Hibernate.initialize(pipelineTask.getRemoteJobs());
                }
                return pipelineTasks;
            });
        }

        public PipelineTask initializeRemoteJobs(long taskId) {
            return performTransaction(() -> {
                PipelineTask pipelineTask = new PipelineTaskCrud().retrieve(taskId);
                Hibernate.initialize(pipelineTask.getRemoteJobs());
                return pipelineTask;
            });
        }

        public void persist(ParameterSet parameterSet) {
            performTransaction(() -> new ParameterSetCrud().persist(parameterSet));
        }

        public void bindParameterSets() {
            performTransaction(() -> {
                new PipelineInstanceOperations().bindParameterSets(pipelineDefinition,
                    pipelineInstance);
                new PipelineInstanceNodeOperations().bindParameterSets(definitionNode,
                    pipelineInstanceNode);
            });
        }
    }
}
