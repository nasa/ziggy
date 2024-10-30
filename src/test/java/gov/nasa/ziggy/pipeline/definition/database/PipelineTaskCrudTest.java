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
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseOperations;

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
    private static final String TEST_MODULE_NAME = "testModule";

    private PipelineModuleDefinition moduleDef;
    private PipelineDefinition pipelineDef;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setup() {
        moduleDef = createModule(TEST_MODULE_NAME);
        pipelineDef = createPipelineDefinition(TEST_PIPELINE_NAME, moduleDef);
    }

    /**
     * Tests that if we create no tasks we will retrieve an empty list.
     */
    @Test
    public void testRetrieveNoTasks() {
        List<PipelineTask> allTasks = testOperations.allPipelineTasks();
        List<PipelineTask> moduleTasks = testOperations.pipelineTasksForModule(TEST_MODULE_NAME);
        PipelineTask task = testOperations.latestPipelineTaskForModule(TEST_MODULE_NAME);
        assertTrue(allTasks.isEmpty());
        assertTrue(moduleTasks.isEmpty());
        assertNull(task);
    }

    /**
     * Tests that we can create a single task and then retrieve it.
     */
    @Test
    public void testRetrieveOneTask() {
        createTasksForPipeline("pipeline1", pipelineDef, moduleDef, 1);

        List<PipelineTask> allTasks = testOperations.allPipelineTasks();
        List<PipelineTask> moduleTasks = testOperations.pipelineTasksForModule(TEST_MODULE_NAME);
        PipelineTask task = testOperations.latestPipelineTaskForModule(TEST_MODULE_NAME);
        assertEquals(1, allTasks.size());
        assertEquals(TEST_MODULE_NAME, allTasks.get(0).getModuleName());

        assertEquals(1, moduleTasks.size());
        assertEquals(TEST_MODULE_NAME, moduleTasks.get(0).getModuleName());

        assertEquals(allTasks.get(0), task);
        assertEquals("pipeline1", new PipelineTaskOperations().pipelineInstance(task).getName());
    }

    /**
     * Tests that we can create a multiple tasks and then retrieve them.
     */
    @Test
    public void testRetrieveTwoTasks() {
        createTasksForPipeline("pipeline1", pipelineDef, moduleDef, 1);
        createTasksForPipeline("pipeline2", pipelineDef, moduleDef, 1);

        List<PipelineTask> allTasks = testOperations.allPipelineTasks();
        List<PipelineTask> moduleTasks = testOperations.pipelineTasksForModule(TEST_MODULE_NAME);
        PipelineTask task = testOperations.latestPipelineTaskForModule(TEST_MODULE_NAME);
        assertEquals(2, allTasks.size());
        assertEquals(TEST_MODULE_NAME, allTasks.get(0).getModuleName());
        assertEquals(TEST_MODULE_NAME, allTasks.get(1).getModuleName());

        assertEquals(2, moduleTasks.size());
        assertEquals(TEST_MODULE_NAME, moduleTasks.get(0).getModuleName());
        assertEquals(TEST_MODULE_NAME, moduleTasks.get(1).getModuleName());

        // We will find one of the tasks, and it should be from pipeline 2.
        PipelineTask latestTask = allTasks.stream()
            .max((a, b) -> a.getCreated().after(b.getCreated()) ? 1 : -1)
            .get();
        assertEquals(latestTask, task);
        assertEquals("pipeline2", new PipelineTaskOperations().pipelineInstance(task).getName());
    }

    @Test
    public void testRetrieveTasksForInstanceNode() {
        PipelineInstance pipelineInstance = createTasksForPipeline("pipeline1", pipelineDef,
            moduleDef, 2);

        List<PipelineTask> pipelineTasks = testOperations
            .pipelineTasksForInstanceNode(pipelineInstance.getPipelineInstanceNodes().get(0));
        assertEquals(2, pipelineTasks.size());
        assertEquals(1L, (long) pipelineTasks.get(0).getId());
        assertEquals(2L, (long) pipelineTasks.get(1).getId());
    }

    private PipelineModuleDefinition createModule(String moduleName) {
        PipelineModuleDefinition moduleDef = new PipelineModuleDefinition(moduleName);
        moduleDef.setPipelineModuleClass(new ClassWrapper<>(TestModule.class));
        return testOperations.mergeModuleDefinition(moduleDef);
    }

    private PipelineDefinition createPipelineDefinition(String pipelineName,
        PipelineModuleDefinition... modules) {

        PipelineDefinition pipelineDef = new PipelineDefinition(pipelineName);

        List<Integer> path = new ArrayList<>();
        List<PipelineDefinitionNode> nodes = Stream.of(modules).map(module -> {
            PipelineDefinitionNode node = new PipelineDefinitionNode(module.getName(),
                pipelineDef.getName());
            path.add(0);
            return node;
        }).collect(Collectors.toList());

        pipelineDef.setRootNodes(nodes);
        testOperations.persistPipelineDefinition(pipelineDef);
        return pipelineDef;
    }

    private PipelineInstance createTasksForPipeline(String instanceName,
        PipelineDefinition pipelineDef, PipelineModuleDefinition moduleDef, int taskCount) {

        PipelineInstance instance = new PipelineInstance(pipelineDef);
        instance.setName(instanceName);
        instance = new PipelineInstanceCrud().merge(instance);

        // Note: Assuming only one root node!
        PipelineDefinitionNode node = pipelineDef.getRootNodes().get(0);
        while (node != null) {
            PipelineInstanceNode instanceNode = new PipelineInstanceNode(node, moduleDef);
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

    public static class TestModule extends PipelineModule {

        public TestModule(PipelineTask pipelineTask, RunMode runMode) {
            super(pipelineTask, runMode);
        }

        @Override
        public boolean processTask() throws PipelineException {
            return false;
        }

        @Override
        public String getModuleName() {
            return TEST_MODULE_NAME;
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

        public void persistPipelineDefinition(PipelineDefinition pipelineDefinition) {
            performTransaction(() -> {
                pipelineDefinition
                    .setRootNodes(mergePipelineDefinitionNodes(pipelineDefinition.getRootNodes()));
                new PipelineDefinitionCrud().persist(pipelineDefinition);
            });
        }

        private List<PipelineDefinitionNode> mergePipelineDefinitionNodes(
            List<PipelineDefinitionNode> nodes) {
            if (CollectionUtils.isEmpty(nodes)) {
                return nodes;
            }
            List<PipelineDefinitionNode> mergedNodes = new ArrayList<>();
            for (PipelineDefinitionNode node : nodes) {
                node.setNextNodes(mergePipelineDefinitionNodes(node.getNextNodes()));
                mergedNodes.add(new PipelineDefinitionNodeCrud().merge(node));
            }
            return mergedNodes;
        }

        public PipelineModuleDefinition mergeModuleDefinition(
            PipelineModuleDefinition moduleDefinition) {
            return performTransaction(
                () -> new PipelineModuleDefinitionCrud().merge(moduleDefinition));
        }

        public List<PipelineTask> pipelineTasksForModule(String moduleName) {
            return performTransaction(
                () -> new PipelineTaskCrud().retrieveAllForModule(moduleName));
        }

        public PipelineTask latestPipelineTaskForModule(String moduleName) {
            return performTransaction(
                () -> new PipelineTaskCrud().retrieveLatestForModule(moduleName));
        }

        public List<PipelineTask> pipelineTasksForInstanceNode(
            PipelineInstanceNode pipelineInstanceNode) {
            return performTransaction(
                () -> new PipelineTaskCrud().retrieveTasksForInstanceNode(pipelineInstanceNode));
        }
    }
}
