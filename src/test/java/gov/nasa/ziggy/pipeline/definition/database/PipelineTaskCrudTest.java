package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.util.PipelineException;

// TODO Rename to PipelineTaskOperationsTest and adjust
//
// Only Operations classes should extend DatabaseOperations classes. CRUD classes should not be
// tested directly, but indirectly through their associated Operations class.
//
// If there is test code that isn't appropriate for a production operations class, move it to an
// inner class called TestOperations that extends DatabaseOperations.

/**
 * Implements unit tests for {@link PipelineTaskCrud}.
 * <p>
 * Note: As of now, 2017-06-30, the tests herein only serve to test newly-added retrieval methods.
 * If you flesh this out to cover all of the CRUD, please remove this note.
 */
public class PipelineTaskCrudTest {

    private static final String TEST_PIPELINE_NAME = "testPipeline";
    private static final String TEST_PIPELINE_STEP_NAME = "testPipelineStep";

    private PipelineStep pipelineStep;
    private Pipeline pipeline;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setup() {
        pipelineStep = createPipelineStep(TEST_PIPELINE_STEP_NAME);
        pipeline = createPipeline(TEST_PIPELINE_NAME, pipelineStep);
    }

    /**
     * Tests that if we create no tasks we will retrieve an empty list.
     */
    @Test
    public void testRetrieveNoTasks() {
        List<PipelineTask> allTasks = testOperations.allPipelineTasks();
        List<PipelineTask> pipelineStepTasks = testOperations
            .pipelineTasksForPipelineStep(TEST_PIPELINE_STEP_NAME);
        PipelineTask task = testOperations
            .latestPipelineTaskForPipelineStep(TEST_PIPELINE_STEP_NAME);
        assertTrue(allTasks.isEmpty());
        assertTrue(pipelineStepTasks.isEmpty());
        assertNull(task);
    }

    /**
     * Tests that we can create a single task and then retrieve it.
     */
    @Test
    public void testRetrieveOneTask() {
        createTasksForPipeline("pipeline1", pipeline, pipelineStep, 1);

        List<PipelineTask> allTasks = testOperations.allPipelineTasks();
        List<PipelineTask> pipelineStepTasks = testOperations
            .pipelineTasksForPipelineStep(TEST_PIPELINE_STEP_NAME);
        PipelineTask task = testOperations
            .latestPipelineTaskForPipelineStep(TEST_PIPELINE_STEP_NAME);
        assertEquals(1, allTasks.size());
        assertEquals(TEST_PIPELINE_STEP_NAME, allTasks.get(0).getPipelineStepName());

        assertEquals(1, pipelineStepTasks.size());
        assertEquals(TEST_PIPELINE_STEP_NAME, pipelineStepTasks.get(0).getPipelineStepName());

        assertEquals(allTasks.get(0), task);
        assertEquals("pipeline1", new PipelineTaskOperations().pipelineInstance(task).getName());
    }

    /**
     * Tests that we can create a multiple tasks and then retrieve them.
     */
    @Test
    public void testRetrieveTwoTasks() {
        createTasksForPipeline("pipeline1", pipeline, pipelineStep, 1);
        createTasksForPipeline("pipeline2", pipeline, pipelineStep, 1);

        List<PipelineTask> allTasks = testOperations.allPipelineTasks();
        List<PipelineTask> pipelineStepTasks = testOperations
            .pipelineTasksForPipelineStep(TEST_PIPELINE_STEP_NAME);
        PipelineTask task = testOperations
            .latestPipelineTaskForPipelineStep(TEST_PIPELINE_STEP_NAME);
        assertEquals(2, allTasks.size());
        assertEquals(TEST_PIPELINE_STEP_NAME, allTasks.get(0).getPipelineStepName());
        assertEquals(TEST_PIPELINE_STEP_NAME, allTasks.get(1).getPipelineStepName());

        assertEquals(2, pipelineStepTasks.size());
        assertEquals(TEST_PIPELINE_STEP_NAME, pipelineStepTasks.get(0).getPipelineStepName());
        assertEquals(TEST_PIPELINE_STEP_NAME, pipelineStepTasks.get(1).getPipelineStepName());

        // We will find one of the tasks, and it should be from pipeline 2.
        PipelineTask latestTask = allTasks.stream()
            .max((a, b) -> a.getCreated().after(b.getCreated()) ? 1 : -1)
            .get();
        assertEquals(latestTask, task);
        assertEquals("pipeline2", new PipelineTaskOperations().pipelineInstance(task).getName());
    }

    @Test
    public void testRetrieveTasksForInstanceNode() {
        PipelineInstance pipelineInstance = createTasksForPipeline("pipeline1", pipeline,
            pipelineStep, 2);

        List<PipelineTask> pipelineTasks = testOperations
            .pipelineTasksForInstanceNode(pipelineInstance.getPipelineInstanceNodes().get(0));
        assertEquals(2, pipelineTasks.size());
        assertEquals(1L, (long) pipelineTasks.get(0).getId());
        assertEquals(2L, (long) pipelineTasks.get(1).getId());
    }

    private PipelineStep createPipelineStep(String pipelineStepName) {
        PipelineStep pipelineStep = new PipelineStep(pipelineStepName);
        pipelineStep
            .setPipelineStepExecutorClass(new ClassWrapper<>(TestPipelineStepExecutor.class));
        return testOperations.mergePipelineStep(pipelineStep);
    }

    private Pipeline createPipeline(String pipelineName, PipelineStep... pipelineSteps) {

        Pipeline pipeline = new Pipeline(pipelineName);

        List<Integer> path = new ArrayList<>();
        List<PipelineNode> nodes = Stream.of(pipelineSteps).map(pipelineStep -> {
            PipelineNode node = new PipelineNode(pipelineStep.getName(), pipeline.getName());
            path.add(0);
            return node;
        }).collect(Collectors.toList());

        pipeline.setRootNodes(nodes);
        testOperations.persistPipeline(pipeline);
        return pipeline;
    }

    private PipelineInstance createTasksForPipeline(String instanceName, Pipeline pipeline,
        PipelineStep pipelineStep, int taskCount) {

        PipelineInstance instance = new PipelineInstance(pipeline);
        instance.setName(instanceName);
        instance = new PipelineInstanceCrud().merge(instance);

        // Note: Assuming only one root node!
        PipelineNode node = pipeline.getRootNodes().get(0);
        while (node != null) {
            PipelineInstanceNode instanceNode = new PipelineInstanceNode(node, pipelineStep);
            instanceNode = new PipelineInstanceNodeCrud().merge(instanceNode);
            instance.addPipelineInstanceNode(new PipelineInstanceCrud().merge(instanceNode));

            for (int i = 0; i < taskCount; i++) {
                PipelineTask task = new PipelineTask(instance, instanceNode, null);
                instanceNode.addPipelineTask(new PipelineTaskCrud().merge(task));
            }
            instanceNode = new PipelineInstanceNodeCrud().merge(instanceNode);

            // Assuming a sequential path of nodes!
            node = node.getNextNodes().isEmpty() ? null : node.getNextNodes().get(0);
        }

        return new PipelineInstanceCrud().merge(instance);
    }

    public static class TestPipelineStepExecutor extends PipelineStepExecutor {

        public TestPipelineStepExecutor(PipelineTask pipelineTask, RunMode runMode) {
            super(pipelineTask, runMode);
        }

        @Override
        public boolean processTask() throws PipelineException {
            return false;
        }

        @Override
        public String getPipelineStepName() {
            return TEST_PIPELINE_STEP_NAME;
        }

        @Override
        protected void restartFromBeginning() {
        }

        @Override
        protected void resumeCurrentStep() {
        }

        @Override
        protected void resubmit() {
        }

        @Override
        protected List<RunMode> restartModes() {
            return null;
        }

        @Override
        protected void runStandard() {
        }
    }

    private static class TestOperations extends DatabaseOperations {

        public List<PipelineTask> allPipelineTasks() {
            return performTransaction(() -> new PipelineTaskCrud().retrieveAll());
        }

        public void persistPipeline(Pipeline pipeline) {
            performTransaction(() -> {
                pipeline.setRootNodes(mergePipelineNodes(pipeline.getRootNodes()));
                new PipelineCrud().persist(pipeline);
            });
        }

        private List<PipelineNode> mergePipelineNodes(List<PipelineNode> nodes) {
            if (CollectionUtils.isEmpty(nodes)) {
                return nodes;
            }
            List<PipelineNode> mergedNodes = new ArrayList<>();
            for (PipelineNode node : nodes) {
                node.setNextNodes(mergePipelineNodes(node.getNextNodes()));
                mergedNodes.add(new PipelineNodeCrud().merge(node));
            }
            return mergedNodes;
        }

        public PipelineStep mergePipelineStep(PipelineStep pipelineStep) {
            return performTransaction(() -> new PipelineStepCrud().merge(pipelineStep));
        }

        public List<PipelineTask> pipelineTasksForPipelineStep(String pipelineStepName) {
            return performTransaction(
                () -> new PipelineTaskCrud().retrieveAllForPipelineStep(pipelineStepName));
        }

        public PipelineTask latestPipelineTaskForPipelineStep(String pipelineStepName) {
            return performTransaction(
                () -> new PipelineTaskCrud().retrieveLatestForPipelineStep(pipelineStepName));
        }

        public List<PipelineTask> pipelineTasksForInstanceNode(
            PipelineInstanceNode pipelineInstanceNode) {
            return performTransaction(
                () -> new PipelineTaskCrud().retrieveTasksForInstanceNode(pipelineInstanceNode));
        }
    }
}
