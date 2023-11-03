package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.crud.SimpleCrud;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodePath;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;

/**
 * Implements unit tests for {@link PipelineTaskCrud}.
 * <p>
 * Note: As of now, 2017-06-30, the tests herein only serve to test newly-added retrieval methods.
 * If you flesh this out to cover all of the CRUD, please remove this note.
 */
public class PipelineTaskCrudTest {

    private static final String TEST_PIPELINE_NAME = "testPipeline";
    private static final String TEST_MODULE_NAME = "testModule";

    private PipelineTaskCrud pipelineTaskCrud;
    private PipelineModuleDefinition moduleDef;
    private PipelineDefinition pipelineDef;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setup() {
        pipelineTaskCrud = new PipelineTaskCrud();

        DatabaseTransactionFactory.performTransaction(() -> {
            moduleDef = createModule(TEST_MODULE_NAME);
            pipelineDef = createPipelineDefinition(TEST_PIPELINE_NAME, moduleDef);
            return null;
        });
    }

    /**
     * Tests that if we create no tasks we will retrieve an empty list.
     */
    @Test
    public void testRetrieveNoTasks() {
        DatabaseTransactionFactory.performTransaction(() -> {
            List<PipelineTask> allTasks = pipelineTaskCrud.retrieveAll();
            List<PipelineTask> moduleTasks = pipelineTaskCrud
                .retrieveAllForModule(TEST_MODULE_NAME);
            PipelineTask task = pipelineTaskCrud.retrieveLatestForModule(TEST_MODULE_NAME);
            assertTrue(allTasks.isEmpty());
            assertTrue(moduleTasks.isEmpty());
            assertNull(task);
            return null;
        });
    }

    /**
     * Tests that we can create a single task and then retrieve it.
     */
    @Test
    public void testRetrieveOneTask() {
        DatabaseTransactionFactory.performTransaction(() -> {
            createTasksForPipeline("pipeline1", pipelineDef, moduleDef, 1);
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {
            List<PipelineTask> allTasks = pipelineTaskCrud.retrieveAll();
            List<PipelineTask> moduleTasks = pipelineTaskCrud
                .retrieveAllForModule(TEST_MODULE_NAME);
            PipelineTask task = pipelineTaskCrud.retrieveLatestForModule(TEST_MODULE_NAME);
            assertEquals(1, allTasks.size());
            assertEquals(TEST_MODULE_NAME, allTasks.get(0).getModuleName());

            assertEquals(1, moduleTasks.size());
            assertEquals(TEST_MODULE_NAME, moduleTasks.get(0).getModuleName());

            assertEquals(allTasks.get(0), task);
            assertEquals("pipeline1", task.getPipelineInstance().getName());
            return null;
        });
    }

    /**
     * Tests that we can create a multiple tasks and then retrieve them.
     */
    @Test
    public void testRetrieveTwoTasks() {
        DatabaseTransactionFactory.performTransaction(() -> {
            createTasksForPipeline("pipeline1", pipelineDef, moduleDef, 1);
            createTasksForPipeline("pipeline2", pipelineDef, moduleDef, 1);
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {
            List<PipelineTask> allTasks = pipelineTaskCrud.retrieveAll();
            List<PipelineTask> moduleTasks = pipelineTaskCrud
                .retrieveAllForModule(TEST_MODULE_NAME);
            PipelineTask task = pipelineTaskCrud.retrieveLatestForModule(TEST_MODULE_NAME);
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
            assertEquals("pipeline2", task.getPipelineInstance().getName());
            return null;
        });
    }

    @Test
    public void testRetrieveStateCount() {
        DatabaseTransactionFactory.performTransaction(() -> {
            createTasksForPipeline("pipeline1", pipelineDef, moduleDef, 4);
            createTasksForPipeline("pipeline2", pipelineDef, moduleDef, 2);
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineTaskCrud.retrieve(1L).setState(PipelineTask.State.PROCESSING);
            pipelineTaskCrud.retrieve(2L).setState(PipelineTask.State.PROCESSING);
            pipelineTaskCrud.retrieve(3L).setState(PipelineTask.State.ERROR);
            pipelineTaskCrud.retrieve(4L).setState(PipelineTask.State.COMPLETED);
            pipelineTaskCrud.retrieve(5L).setState(PipelineTask.State.PROCESSING);
            pipelineTaskCrud.retrieve(6L).setState(PipelineTask.State.PROCESSING);
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {
            Integer taskCount = pipelineTaskCrud.retrieveStateCount(1L,
                PipelineTask.State.PROCESSING);
            assertEquals(2L, taskCount.longValue());
            taskCount = pipelineTaskCrud.retrieveStateCount(1L, PipelineTask.State.ERROR);
            assertEquals(1L, taskCount.longValue());
            taskCount = pipelineTaskCrud.retrieveStateCount(2L, PipelineTask.State.PROCESSING);
            assertEquals(2L, taskCount.longValue());
            return null;
        });
    }

    private PipelineModuleDefinition createModule(String moduleName) {
        PipelineModuleDefinition moduleDef = new PipelineModuleDefinition(moduleName);
        moduleDef.setPipelineModuleClass(new ClassWrapper<>(TestModule.class));
        return new PipelineModuleDefinitionCrud().merge(moduleDef);
    }

    private PipelineDefinition createPipelineDefinition(String pipelineName,
        PipelineModuleDefinition... modules) {

        PipelineDefinition pipelineDef = new PipelineDefinition(pipelineName);

        List<Integer> path = new ArrayList<>();
        List<PipelineDefinitionNode> nodes = Stream.of(modules).map(module -> {
            PipelineDefinitionNode node = new PipelineDefinitionNode(module.getName(),
                pipelineDef.getName());
            node.setUnitOfWorkGenerator(new ClassWrapper<>(new SingleUnitOfWorkGenerator()));
            path.add(0);
            node.setPath(new PipelineDefinitionNodePath(path));
            new SimpleCrud<>().persist(node);
            return node;
        }).collect(Collectors.toList());

        pipelineDef.setRootNodes(nodes);
        return new PipelineDefinitionCrud().merge(pipelineDef);
    }

    private PipelineInstance createTasksForPipeline(String instanceName,
        PipelineDefinition pipelineDef, PipelineModuleDefinition moduleDef, int taskCount) {

        PipelineInstance instance = new PipelineInstance(pipelineDef);
        instance.setName(instanceName);

        // Note: Assuming only one root node!
        PipelineDefinitionNode node = pipelineDef.getRootNodes().get(0);
        while (node != null) {
            PipelineInstanceNode instanceNode = new PipelineInstanceNode(instance, node, moduleDef);
            new PipelineInstanceCrud().persist(instanceNode);

            for (int i = 0; i < taskCount; i++) {
                PipelineTask task = new PipelineTask(instance, instanceNode);
                new PipelineTaskCrud().persist(task);
            }

            // Assuming a sequential path of nodes!
            node = node.getNextNodes().isEmpty() ? null : node.getNextNodes().get(0);
        }

        new PipelineInstanceCrud().persist(instance);
        return instance;
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
        protected void resumeMonitoring() {
        }

        @Override
        protected List<RunMode> restartModes() {
            return null;
        }

        @Override
        protected void runStandard() {
        }
    }
}
