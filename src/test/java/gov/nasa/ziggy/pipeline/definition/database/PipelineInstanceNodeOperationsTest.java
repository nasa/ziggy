package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskCountsTest;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;

/** Unit tests for {@link PipelineInstanceNodeOperations}. */
public class PipelineInstanceNodeOperationsTest {

    private PipelineTask task1, task2, task3, task4;
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations;
    private PipelineInstance pipelineInstance;
    private PipelineNode pipelineNode;
    private TestOperations testOperations;
    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineInstanceNode newInstanceNode;
    private PipelineTaskOperations pipelineTaskOperations;
    private PipelineTaskDataOperations pipelineTaskDataOperations;
    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations;
    private PipelineInstanceOperations pipelineInstanceOperations;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineInstanceNodeOperations = Mockito.spy(PipelineInstanceNodeOperations.class);
        testOperations = new TestOperations();
        pipelineTaskOperations = new PipelineTaskOperations();
        pipelineTaskDataOperations = new PipelineTaskDataOperations();
        pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
        pipelineInstanceOperations = new PipelineInstanceOperations();
    }

    @Test
    public void testMarkInstanceNodeTransition() {
        setUpSingleNodePipeline();
        PipelineInstanceNode databaseInstanceNode = testOperations
            .pipelineInstanceNode(pipelineInstanceNode.getId());
        assertFalse(databaseInstanceNode.isTransitionComplete());
        pipelineInstanceNodeOperations
            .markInstanceNodeTransitionComplete(pipelineInstanceNode.getId());
        databaseInstanceNode = testOperations.pipelineInstanceNode(pipelineInstanceNode.getId());
        assertTrue(databaseInstanceNode.isTransitionComplete());
        pipelineInstanceNodeOperations
            .markInstanceNodeTransitionIncomplete(pipelineInstanceNode.getId());
        databaseInstanceNode = testOperations.pipelineInstanceNode(pipelineInstanceNode.getId());
        assertFalse(databaseInstanceNode.isTransitionComplete());
    }

    private void setUpSingleNodePipeline() {
        PipelineOperationsTestUtils testUtils = new PipelineOperationsTestUtils();
        testUtils.setUpSingleNodePipeline();
        pipelineNode = testUtils.pipelineNode();
        pipelineInstance = testUtils.pipelineInstance();
        pipelineInstanceNode = testUtils.pipelineInstanceNode();
        task1 = testUtils.getPipelineTasks().get(0);
        task2 = testUtils.getPipelineTasks().get(1);
    }

    /**
     * Tests that the correct state transitions occur when there are multiple nodes in the pipeline.
     */
    @Test
    public void testMultipleInstanceNodesNoErrors() {

        setUpSingleNodePipeline();

        // Create a new instance node with 2 tasks in it and attach it to the instance.
        testOperations.addPipelineNode();

        // Mark the first 2 tasks as complete and check that all the correct states are
        // generated. Then create 2 new pipeline tasks in the second instance node.
        pipelineTaskDataOperations.updateProcessingStep(task1, ProcessingStep.COMPLETE);
        pipelineTaskDataOperations.updateProcessingStep(task2, ProcessingStep.COMPLETE);
        PipelineInstance pipelineInstance = pipelineInstanceOperations
            .pipelineInstance(task1.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        TaskCounts counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 0, 2, 0, counts);
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(2, 0, 2, 0, counts);
        assertTrue(pipelineInstance.getExecutionClock().isRunning());

        List<UnitOfWork> unitsOfWork = List.of(new UnitOfWork("brief3"), new UnitOfWork("brief4"));
        List<PipelineTask> pipelineTasks = testOperations
            .mergePipelineTasks(new RuntimeObjectFactory().newPipelineTasks(newInstanceNode,
                pipelineInstance, unitsOfWork));

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
            .pipelineInstance(task3.getPipelineInstanceId());
        assertEquals(PipelineInstance.State.PROCESSING, pipelineInstance.getState());
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        TaskCountsTest.testTaskCounts(4, 0, 2, 0, counts);
        counts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineTaskOperations.pipelineInstanceNode(task3));
        TaskCountsTest.testTaskCounts(2, 0, 0, 0, counts);
        assertTrue(pipelineInstance.getExecutionClock().isRunning());

        // Move the tasks to COMPLETED.
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
    public void testNextNodes() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpFourNodePipelineWithInstanceNodes();
        List<PipelineInstanceNode> nextNodes = pipelineInstanceNodeOperations
            .nextPipelineInstanceNodes(2L);
        assertTrue(
            nextNodes.contains(pipelineOperationsTestUtils.getPipelineInstanceNodes().get(2)));
        assertEquals(1, nextNodes.size());
        nextNodes = pipelineInstanceNodeOperations.nextPipelineInstanceNodes(4L);
        assertTrue(nextNodes.isEmpty());
    }

    @Test
    public void testAddNextNodes() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpFourNodePipelineWithInstanceNodes();
        PipelineInstanceNode newNode = testOperations.merge(new PipelineInstanceNode());
        pipelineInstanceNodeOperations.addNextNodes(
            pipelineOperationsTestUtils.getPipelineInstanceNodes().get(1), List.of(newNode));
        List<PipelineInstanceNode> nextNodes = pipelineInstanceNodeOperations
            .nextPipelineInstanceNodes(2L);
        assertTrue(
            nextNodes.contains(pipelineOperationsTestUtils.getPipelineInstanceNodes().get(2)));
        assertTrue(nextNodes.contains(newNode));
        assertEquals(2, nextNodes.size());
    }

    @Test
    public void testAddPipelineTasks() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        List<PipelineTask> pipelineTasks = pipelineInstanceNodeOperations
            .pipelineTasks(List.of(pipelineOperationsTestUtils.pipelineInstanceNode()));
        assertTrue(pipelineTasks.contains(pipelineOperationsTestUtils.getPipelineTasks().get(0)));
        assertTrue(pipelineTasks.contains(pipelineOperationsTestUtils.getPipelineTasks().get(1)));
        assertEquals(2, pipelineTasks.size());
        PipelineTask newTask = testOperations.mergePipelineTask(new PipelineTask());
        pipelineInstanceNodeOperations
            .addPipelineTasks(pipelineOperationsTestUtils.pipelineInstanceNode(), List.of(newTask));
        pipelineTasks = pipelineInstanceNodeOperations
            .pipelineTasks(List.of(pipelineOperationsTestUtils.pipelineInstanceNode()));
        assertTrue(pipelineTasks.contains(pipelineOperationsTestUtils.getPipelineTasks().get(0)));
        assertTrue(pipelineTasks.contains(pipelineOperationsTestUtils.getPipelineTasks().get(1)));
        assertTrue(pipelineTasks.contains(newTask));
        assertEquals(3, pipelineTasks.size());
    }

    @Test
    public void testInputDataFileTypes() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        testOperations.addInputDataFileType(pipelineOperationsTestUtils);
        Set<DataFileType> dataFileTypes = pipelineInstanceNodeOperations
            .inputDataFileTypes(pipelineOperationsTestUtils.pipelineInstanceNode());
        assertFalse(CollectionUtils.isEmpty(dataFileTypes));
        DataFileType databaseDataFileType = dataFileTypes.iterator().next();
        assertEquals("dummy4", databaseDataFileType.getName());
        assertEquals(1, dataFileTypes.size());
    }

    @Test
    public void testBindParameters() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        PipelineInstanceNode pipelineInstanceNode = pipelineOperationsTestUtils
            .pipelineInstanceNode();
        testOperations.configureNodeParameterSets(pipelineInstanceNode.getId());
        PipelineInstanceNode databaseNode = pipelineInstanceNodeOperations
            .pipelineInstanceNode(pipelineInstanceNode.getId());
        pipelineInstanceNodeOperations.bindParameterSets(databaseNode.getPipelineNode(),
            databaseNode);
        Set<ParameterSet> parameterSets = pipelineInstanceNodeOperations
            .parameterSets(pipelineInstanceNode);
        assertFalse(CollectionUtils.isEmpty(parameterSets));
        ParameterSet parameterSet = parameterSets.iterator().next();
        assertEquals("dummy100", parameterSet.getName());
        assertEquals(1, parameterSets.size());
    }

    /**
     * Perform basic tests on the two taskCounts() signatures. The TaskCounts are more rigorously
     * tested in testMultipleInstanceNodesNoErrors().
     */
    @Test
    public void testTaskCounts() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        assertEquals(1, pipelineOperationsTestUtils.getPipelineInstanceNodes().size());

        TaskCounts taskCounts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineOperationsTestUtils.getPipelineInstanceNodes().get(0));
        assertEquals(1, taskCounts.getPipelineStepNames().size());
        assertEquals("step1", taskCounts.getPipelineStepNames().get(0));
        assertEquals(1, taskCounts.getPipelineStepCounts().size());
        TaskCountsTest.testTaskCounts(2, 2, 0, 0, taskCounts);
        TaskCountsTest.testCounts(2, 0, 0, 0, 0, 0, 0,
            taskCounts.getPipelineStepCounts().get("step1"));

        taskCounts = pipelineTaskDisplayDataOperations
            .taskCounts(pipelineOperationsTestUtils.getPipelineInstanceNodes());
    }

    /**
     * Test that when a pipeline instance node is set to a transition-failed state, the appropriate
     * state changes for the instance node and its parent instance occur; then do the same for when
     * the transition-failed state is removed.
     */
    @Test
    public void testTransitionFailureAndRestart() {

        // Set up a pipeline with multiple instance nodes.
        PipelineOperationsTestUtils testUtils = new PipelineOperationsTestUtils();
        testUtils.setUpFourNodePipelineWithInstanceNodes();
        PipelineInstance pipelineInstance = testUtils.pipelineInstance();
        PipelineInstanceNode rootNode = pipelineInstanceOperations.rootNodes(pipelineInstance)
            .get(0);

        // Add a pipeline task to the first instance node.
        List<PipelineTask> pipelineTasks = new RuntimeObjectFactory().newPipelineTasks(rootNode,
            pipelineInstance, new SingleUnitOfWorkGenerator().generateUnitsOfWork(rootNode));
        PipelineTask pipelineTask = pipelineTasks.get(0);

        // Set the pipeline task state to executing.
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.EXECUTING);
        assertEquals(PipelineInstance.State.PROCESSING,
            pipelineInstanceOperations.pipelineInstance(pipelineInstance.getId()).getState());

        // Move the task to completed.
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.COMPLETE);
        assertEquals(PipelineInstance.State.PROCESSING,
            pipelineInstanceOperations.pipelineInstance(pipelineInstance.getId()).getState());
        assertFalse(pipelineInstanceNodeOperations.pipelineInstanceNode(rootNode.getId())
            .isTransitionFailed());

        // Move the instance node to transition failed.
        pipelineInstanceNodeOperations.markInstanceNodeTransitionFailed(rootNode);
        assertEquals(PipelineInstance.State.TRANSITION_FAILED,
            pipelineInstanceOperations.pipelineInstance(pipelineInstance.getId()).getState());
        PipelineInstanceNode nodeFromDatabase = pipelineInstanceNodeOperations
            .pipelineInstanceNode(rootNode.getId());
        assertTrue(nodeFromDatabase.isTransitionFailed());
        assertFalse(nodeFromDatabase.isTransitionComplete());
        assertEquals(ProcessingStep.COMPLETE,
            pipelineTaskDataOperations.processingStep(pipelineTask));

        // Retry the transition.
        pipelineInstanceNodeOperations.clearTransitionFailedState(rootNode);
        nodeFromDatabase = pipelineInstanceNodeOperations.pipelineInstanceNode(rootNode.getId());
        assertFalse(nodeFromDatabase.isTransitionFailed());
        assertFalse(nodeFromDatabase.isTransitionComplete());
        assertEquals(ProcessingStep.COMPLETE,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        assertEquals(PipelineInstance.State.PROCESSING,
            pipelineInstanceOperations.pipelineInstance(pipelineInstance.getId()).getState());
    }

    private class TestOperations extends DatabaseOperations {

        public PipelineInstanceNode pipelineInstanceNode(long instanceNodeId) {
            return performTransaction(
                () -> new PipelineInstanceNodeCrud().retrieve(instanceNodeId));
        }

        public PipelineInstanceNode merge(PipelineInstanceNode pipelineInstanceNode) {
            return performTransaction(
                () -> new PipelineInstanceNodeCrud().merge(pipelineInstanceNode));
        }

        public PipelineNode merge(PipelineNode pipelineNode) {
            return performTransaction(() -> new PipelineNodeCrud().merge(pipelineNode));
        }

        public PipelineStep merge(PipelineStep pipelineStep) {
            return performTransaction(() -> new PipelineStepCrud().merge(pipelineStep));
        }

        public PipelineInstance merge(PipelineInstance pipelineInstance) {
            return performTransaction(() -> new PipelineInstanceCrud().merge(pipelineInstance));
        }

        public List<PipelineTask> mergePipelineTasks(List<PipelineTask> pipelineTasks) {
            List<PipelineTask> mergedTasks = new ArrayList<>();
            performTransaction(() -> {
                for (PipelineTask task : pipelineTasks) {
                    mergedTasks.add(new PipelineTaskCrud().merge(task));
                }
            });
            return mergedTasks;
        }

        public PipelineTask mergePipelineTask(PipelineTask pipelineTask) {
            return performTransaction(() -> new PipelineTaskCrud().merge(pipelineTask));
        }

        public PipelineNode addPipelineNode() {
            return performTransaction(() -> {
                // Create a new instance node with 2 tasks in it and attach it to the instance.
                PipelineStep newPipelineStep = merge(new PipelineStep("dummy6"));
                PipelineNode newPipelineNode = merge(new PipelineNode("dummy7", "dummy7"));
                pipelineNode.addNextNode(newPipelineNode);
                newInstanceNode = testOperations
                    .merge(new PipelineInstanceNode(newPipelineNode, newPipelineStep));
                pipelineInstance.setEndNode(newInstanceNode);
                pipelineInstance.addPipelineInstanceNode(newInstanceNode);
                testOperations.merge(pipelineInstance);
                return newPipelineNode;
            });
        }

        public void addInputDataFileType(PipelineOperationsTestUtils pipelineOperationsTestUtils) {
            performTransaction(() -> {
                DataFileType inputDataFileType = new DataFileType("dummy4", "dummy5", "dummy6");
                new DataFileTypeCrud().persist(inputDataFileType);
                inputDataFileType = new DataFileTypeCrud().retrieveByName("dummy4");
                pipelineOperationsTestUtils.pipelineNode()
                    .addAllInputDataFileTypes(List.of(inputDataFileType));
                new PipelineNodeCrud().merge(pipelineOperationsTestUtils.pipelineNode());
            });
        }

        public void configureNodeParameterSets(long instanceId) {
            performTransaction(() -> {
                PipelineInstanceNode instanceNode = new PipelineInstanceNodeCrud()
                    .retrieve(instanceId);
                PipelineNode pipelineNode = instanceNode.getPipelineNode();
                pipelineNode.getParameterSetNames().clear();
                pipelineNode.getParameterSetNames().add("dummy100");
                ParameterSet parameterSet = new ParameterSet();
                parameterSet.setName("dummy100");
                new ParameterSetCrud().persist(parameterSet);
                new PipelineCrud().merge(pipelineNode);
                new PipelineInstanceCrud().merge(instanceNode);
            });
        }
    }
}
