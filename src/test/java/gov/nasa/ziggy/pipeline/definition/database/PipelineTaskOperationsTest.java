package gov.nasa.ziggy.pipeline.definition.database;

import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_QUEUE_COMMAND_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskCountsTest;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.UnitOfWork;
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
    private PipelineTaskDataOperations pipelineTaskDataOperations;
    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations;
    private PipelineInstanceOperations pipelineInstanceOperations;
    private QueueCommandManager cmdManager;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule queueCommandClassPropertyRule = new ZiggyPropertyRule(
        REMOTE_QUEUE_COMMAND_CLASS, "gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests");

    @Before
    public void setUp() {

        pipelineTaskOperations = new PipelineTaskOperations();
        pipelineTaskDataOperations = spy(PipelineTaskDataOperations.class);
        pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
        pipelineInstanceOperations = spy(PipelineInstanceOperations.class);
        cmdManager = spy(QueueCommandManager.newInstance());
        when(pipelineTaskDataOperations.queueCommandManager()).thenReturn(cmdManager);
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
     * Tests that the
     * {@link PipelineTaskDataOperations#updateProcessingStep(PipelineTask, ProcessingStep)} method
     * performs correctly in the case where all tasks run to completion without errors. In
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
            assertFalse(pipelineTaskDataOperations.executionClock(task).isRunning());
            assertEquals(ProcessingStep.WAITING_TO_RUN,
                pipelineTaskDataOperations.processingStep(task));
        }
        PipelineInstance instance = testOperations.allPipelineInstances().get(0);
        assertFalse(instance.getExecutionClock().isRunning());
        assertEquals(PipelineInstance.State.INITIALIZED, instance.getState());

        TaskCounts counts = pipelineInstanceOperations.taskCounts(instance);
        TaskCountsTest.testTaskCounts(2, 2, 0, 0, counts);

        counts = pipelineTaskDisplayDataOperations.taskCounts(pipelineInstanceNode);
        TaskCountsTest.testTaskCounts(2, 2, 0, 0, counts);

        // Move the tasks one at a time to the WAITING_TO_RUN step.
        moveTaskToWaitingToRun(databaseTasks.get(0));
        moveTaskToWaitingToRun(databaseTasks.get(1));

        // Make sure that the state changes are actually persisted to the database
        List<PipelineTask> submittedTasks = testOperations.allPipelineTasks();
        for (PipelineTask task : submittedTasks) {
            assertEquals(ProcessingStep.WAITING_TO_RUN,
                pipelineTaskDataOperations.processingStep(task));
            assertTrue(pipelineTaskDataOperations.executionClock(task).isRunning());
        }
        instance = testOperations.allPipelineInstances().get(0);
        assertEquals(PipelineInstance.State.PROCESSING, instance.getState());
        assertTrue(instance.getExecutionClock().isRunning());

        // Move the tasks to the EXECUTING step.
        pipelineTaskDataOperations.updateProcessingStep(submittedTasks.get(0),
            ProcessingStep.EXECUTING);
        PipelineTask pipelineTask = submittedTasks.get(1);
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.EXECUTING);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(pipelineTask.getPipelineInstanceId());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 0, 0, 0, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(pipelineTask));
        TaskCountsTest.testTaskCounts(2, 0, 0, 0, counts);

        // Move both tasks to the COMPLETE step.
        List<PipelineTask> tasks = testOperations.allPipelineTasks();
        pipelineTaskDataOperations.updateProcessingStep(tasks.get(0), ProcessingStep.COMPLETE);
        pipelineTask = tasks.get(1);
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.COMPLETE);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(pipelineTask.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.COMPLETED, pipelineInstance.getState());
        assertFalse(pipelineInstance.getExecutionClock().isRunning());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 0, 2, 0, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(pipelineTask));
        TaskCountsTest.testTaskCounts(2, 0, 2, 0, counts);

        // Make sure the tasks got their clocks stopped.
        tasks = testOperations.allPipelineTasks();
        assertFalse(pipelineTaskDataOperations.executionClock(tasks.get(0)).isRunning());
        assertFalse(pipelineTaskDataOperations.executionClock(tasks.get(1)).isRunning());
    }

    private void moveTaskToWaitingToRun(PipelineTask pipelineTask) {
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask,
            ProcessingStep.WAITING_TO_RUN);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(pipelineTask.getPipelineInstanceId());
        assertEquals(ProcessingStep.WAITING_TO_RUN,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        TaskCounts counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 2, 0, 0, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(pipelineTask));
        TaskCountsTest.testTaskCounts(2, 2, 0, 0, counts);
        assertTrue(pipelineTaskDataOperations.executionClock(pipelineTask).isRunning());
        assertTrue(pipelineInstance.getExecutionClock().isRunning());
    }

    /**
     * Tests that the correct state transitions are performed when there are tasks that error out.
     */
    @Test
    public void testUpdateProcessingStepWithErrors() {

        setUpSingleModulePipeline();

        // Move the tasks to the EXECUTING step.
        pipelineTaskDataOperations.updateProcessingStep(task1, ProcessingStep.EXECUTING);
        pipelineTaskDataOperations.updateProcessingStep(task2, ProcessingStep.EXECUTING);

        // Move one task as errored.
        pipelineTaskDataOperations.taskErrored(task1);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task1.getPipelineInstanceId());

        // Check that the updated states are all correct.
        assertEquals(PipelineInstance.State.ERRORS_RUNNING, pipelineInstance.getState());
        assertTrue(pipelineTaskDataOperations.hasErrored(task1));
        TaskCounts counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 0, 0, 1, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(task1));
        TaskCountsTest.testTaskCounts(2, 0, 0, 1, counts);

        assertTrue(pipelineInstance.getExecutionClock().isRunning());
        assertFalse(pipelineTaskDataOperations.executionClock(task1).isRunning());

        // Move the other task to the COMPLETE step.
        pipelineTaskDataOperations.updateProcessingStep(task2, ProcessingStep.COMPLETE);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task2.getPipelineInstanceId());

        // Check that the updated states are all correct.
        assertEquals(PipelineInstance.State.ERRORS_STALLED, pipelineInstance.getState());
        assertEquals(ProcessingStep.COMPLETE, pipelineTaskDataOperations.processingStep(task2));
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 0, 1, 1, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(task2));
        TaskCountsTest.testTaskCounts(2, 0, 1, 1, counts);

        assertFalse(pipelineInstance.getExecutionClock().isRunning());
        assertFalse(pipelineTaskDataOperations.executionClock(task2).isRunning());
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
        pipelineTaskDataOperations.updateProcessingStep(task1, ProcessingStep.COMPLETE);
        pipelineTaskDataOperations.updateProcessingStep(task2, ProcessingStep.COMPLETE);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task1.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        TaskCounts counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 0, 2, 0, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(task1));
        TaskCountsTest.testTaskCounts(2, 0, 2, 0, counts);
        assertTrue(pipelineInstance.getExecutionClock().isRunning());

        List<UnitOfWork> unitsOfWork = List.of(new UnitOfWork("brief3"), new UnitOfWork("brief4"));
        List<PipelineTask> pipelineTasks = new RuntimeObjectFactory()
            .newPipelineTasks(newInstanceNode, pipelineInstance, unitsOfWork);
        task3 = pipelineTasks.get(0);
        task4 = pipelineTasks.get(1);

        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(4, 2, 2, 0, counts);

        counts = pipelineTaskDisplayDataOperations.taskCounts(newInstanceNode);
        TaskCountsTest.testTaskCounts(2, 2, 0, 0, counts);

        // Move the new tasks to the EXECUTING step.
        pipelineTaskDataOperations.updateProcessingStep(task3, ProcessingStep.EXECUTING);
        pipelineTaskDataOperations.updateProcessingStep(task4, ProcessingStep.EXECUTING);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task4.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(4, 0, 2, 0, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(task4));
        TaskCountsTest.testTaskCounts(2, 0, 0, 0, counts);
        assertTrue(pipelineInstance.getExecutionClock().isRunning());

        // Move the tasks to COMPLETE.
        pipelineTaskDataOperations.updateProcessingStep(task3, ProcessingStep.COMPLETE);
        pipelineTaskDataOperations.updateProcessingStep(task4, ProcessingStep.COMPLETE);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task4.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.COMPLETED, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(4, 0, 4, 0, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(task4));
        TaskCountsTest.testTaskCounts(2, 0, 2, 0, counts);
        assertFalse(pipelineInstance.getExecutionClock().isRunning());
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
        pipelineTaskDataOperations.updateProcessingStep(task1, ProcessingStep.EXECUTING);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task1.getPipelineInstanceId());
        assertTrue(pipelineInstance.getExecutionClock().isRunning());

        // Now mark the first task as errored.
        pipelineTaskDataOperations.taskErrored(task1);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task1.getPipelineInstanceId());
        assertTrue(pipelineInstance.getExecutionClock().isRunning());
        assertEquals(PipelineInstance.State.ERRORS_RUNNING, pipelineInstance.getState());
        TaskCounts counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 1, 0, 1, counts);

        // Move the second task into EXECUTING.
        pipelineTaskDataOperations.updateProcessingStep(task2, ProcessingStep.EXECUTING);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task2.getPipelineInstanceId());
        assertTrue(pipelineInstance.getExecutionClock().isRunning());

        // When the second task completes, the instance should go to ERRORS_STALLED even
        // though there are pipeline instance nodes that have not yet been run.
        pipelineTaskDataOperations.updateProcessingStep(task2, ProcessingStep.COMPLETE);
        pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task2.getPipelineInstanceId());
        assertFalse(pipelineInstance.getExecutionClock().isRunning());
        assertEquals(PipelineInstance.State.ERRORS_STALLED, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 0, 1, 1, counts);
    }

    @Test
    public void testTaskIdsForPipelineDefinitionNode() {
        setUpSingleModulePipeline();
        List<PipelineTask> taskIds = pipelineTaskOperations.tasksForPipelineDefinitionNode(task1);
        assertEquals(2, taskIds.size());
        assertTrue(taskIds.contains(task1));
        assertTrue(taskIds.contains(task2));
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

    PipelineTask createPipelineTask() {

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
        PipelineTask task = new PipelineTask(instance, instNode, null);
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

        public PipelineDefinition merge(PipelineDefinition pipelineDefinition) {
            return performTransaction(() -> new PipelineDefinitionCrud().merge(pipelineDefinition));
        }

        public DataFileType merge(DataFileType dataFileType) {
            return performTransaction(() -> {
                new DataFileTypeCrud().persist(dataFileType);
                return new DataFileTypeCrud().retrieveByName(dataFileType.getName());
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
