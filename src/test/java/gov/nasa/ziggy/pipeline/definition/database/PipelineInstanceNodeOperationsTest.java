package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/** Unit tests for {@link PipelineInstanceNodeOperations}. */
public class PipelineInstanceNodeOperationsTest {

    private PipelineTask task1, task2, task3, task4;
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations;
    private PipelineInstance pipelineInstance;
    private PipelineDefinitionNode definitionNode;
    private TestOperations testOperations;
    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineInstanceNode newInstanceNode;
    private PipelineTaskOperations pipelineTaskOperations;
    private PipelineInstanceOperations pipelineInstanceOperations;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineInstanceNodeOperations = Mockito.spy(PipelineInstanceNodeOperations.class);
        testOperations = new TestOperations();
        pipelineTaskOperations = new PipelineTaskOperations();
        pipelineInstanceOperations = new PipelineInstanceOperations();
    }

    @Test
    public void testMarkInstanceNodeTransition() {
        setUpSingleModulePipeline();
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

    private void setUpSingleModulePipeline() {
        PipelineOperationsTestUtils testUtils = new PipelineOperationsTestUtils();
        testUtils.setUpSingleModulePipeline();
        definitionNode = testUtils.pipelineDefinitionNode();
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

        setUpSingleModulePipeline();

        // Create a new instance node with 2 tasks in it and attach it to the instance.
        testOperations.addPipelineDefinitionNode();

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
        counts = pipelineInstanceOperations.taskCounts(pipelineInstance);
        PipelineOperationsTestUtils.testTaskCounts(2, 0, 2, 0, counts);
        assertTrue(pipelineInstance.getCurrentExecutionStartTimeMillis() > 0);

        task3 = new PipelineTask(pipelineInstance, newInstanceNode);
        task4 = new PipelineTask(pipelineInstance, newInstanceNode);
        List<PipelineTask> pipelineTasks = testOperations.mergePipelineTasks(List.of(task3, task4));
        for (PipelineTask pipelineTask : pipelineTasks) {
            newInstanceNode.addPipelineTask(pipelineTask);
        }
        newInstanceNode = testOperations.merge(newInstanceNode);
        task3 = pipelineTasks.get(0);
        task4 = pipelineTasks.get(1);

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

        // Move the tasks to COMPLETED.
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
    public void testNextNodes() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpFourModulePipelineWithInstanceNodes();
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
        pipelineOperationsTestUtils.setUpFourModulePipelineWithInstanceNodes();
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
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
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
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
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
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        PipelineInstanceNode pipelineInstanceNode = pipelineOperationsTestUtils
            .pipelineInstanceNode();
        testOperations.configureModuleParameterSets(pipelineInstanceNode.getId());
        PipelineInstanceNode databaseNode = pipelineInstanceNodeOperations
            .pipelineInstanceNode(pipelineInstanceNode.getId());
        pipelineInstanceNodeOperations.bindParameterSets(databaseNode.getPipelineDefinitionNode(),
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
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        assertEquals(1, pipelineOperationsTestUtils.getPipelineInstanceNodes().size());

        TaskCounts taskCounts = pipelineInstanceNodeOperations
            .taskCounts(pipelineOperationsTestUtils.getPipelineInstanceNodes().get(0));
        assertEquals(1, taskCounts.getModuleNames().size());
        assertEquals("module1", taskCounts.getModuleNames().get(0));
        assertEquals(1, taskCounts.getModuleCounts().size());
        PipelineOperationsTestUtils.testTaskCounts(2, 2, 0, 0, taskCounts);
        PipelineOperationsTestUtils.testCounts(2, 0, 0, 0, 0, 0, 0,
            taskCounts.getModuleCounts().get("module1"));

        taskCounts = pipelineInstanceNodeOperations
            .taskCounts(pipelineOperationsTestUtils.getPipelineInstanceNodes());
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

        public PipelineDefinitionNode merge(PipelineDefinitionNode pipelineDefinitionNode) {
            return performTransaction(
                () -> new PipelineDefinitionNodeCrud().merge(pipelineDefinitionNode));
        }

        public PipelineModuleDefinition merge(PipelineModuleDefinition moduleDefinition) {
            return performTransaction(
                () -> new PipelineModuleDefinitionCrud().merge(moduleDefinition));
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

        public PipelineDefinitionNode addPipelineDefinitionNode() {
            return performTransaction(() -> {
                // Create a new instance node with 2 tasks in it and attach it to the instance.
                PipelineModuleDefinition newModuleDefinition = merge(
                    new PipelineModuleDefinition("dummy6"));
                PipelineDefinitionNode newDefinitionNode = merge(
                    new PipelineDefinitionNode("dummy7", "dummy7"));
                definitionNode.addNextNode(newDefinitionNode);
                newInstanceNode = testOperations
                    .merge(new PipelineInstanceNode(newDefinitionNode, newModuleDefinition));
                pipelineInstance.setEndNode(newInstanceNode);
                pipelineInstance.addPipelineInstanceNode(newInstanceNode);
                testOperations.merge(pipelineInstance);
                return newDefinitionNode;
            });
        }

        public void addInputDataFileType(PipelineOperationsTestUtils pipelineOperationsTestUtils) {
            performTransaction(() -> {
                DataFileType inputDataFileType = new DataFileType("dummy4", "dummy5", "dummy6");
                new DataFileTypeCrud().persist(inputDataFileType);
                inputDataFileType = new DataFileTypeCrud().retrieveByName("dummy4");
                pipelineOperationsTestUtils.pipelineDefinitionNode()
                    .addAllInputDataFileTypes(List.of(inputDataFileType));
                new PipelineDefinitionNodeCrud()
                    .merge(pipelineOperationsTestUtils.pipelineDefinitionNode());
            });
        }

        public void configureModuleParameterSets(long instanceId) {
            performTransaction(() -> {
                PipelineInstanceNode instanceNode = new PipelineInstanceNodeCrud()
                    .retrieve(instanceId);
                PipelineDefinitionNode pipelineDefinitionNode = instanceNode
                    .getPipelineDefinitionNode();
                pipelineDefinitionNode.getParameterSetNames().clear();
                pipelineDefinitionNode.getParameterSetNames().add("dummy100");
                ParameterSet parameterSet = new ParameterSet();
                parameterSet.setName("dummy100");
                new ParameterSetCrud().persist(parameterSet);
                new PipelineDefinitionCrud().merge(pipelineDefinitionNode);
                new PipelineInstanceCrud().merge(instanceNode);
            });
        }
    }
}
