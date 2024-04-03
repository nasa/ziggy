
package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import gov.nasa.ziggy.IntegrationTestCategory;
import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TestModuleParameters;
import gov.nasa.ziggy.pipeline.definition.TestPipelineParameters;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud.ClearStaleStateResults;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Tests for {@link PipelineInstanceCrud} and {@link PipelineTaskCrud} Tests that objects can be
 * stored, retrieved, and edited and that mapping metadata (associations, cascade rules, etc.) are
 * setup correctly and work as expected.
 *
 * @author Todd Klaus
 */
@Category(IntegrationTestCategory.class)
public class PipelineInstanceTaskCrudTest {

    private static final String TEST_PIPELINE_NAME = "Test Pipeline";
    private static final String TEST_WORKER_NAME = "TestWorker";

    private PipelineDefinitionCrud pipelineDefinitionCrud;
    private PipelineInstanceCrud pipelineInstanceCrud;
    private PipelineInstanceNodeCrud pipelineInstanceNodeCrud;
    private PipelineTaskCrud pipelineTaskCrud;
    private PipelineOperations pipelineOperations;

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private ParameterSetCrud parameterSetCrud;

    private PipelineInstance pipelineInstance;
    private PipelineInstanceNode pipelineInstanceNode1;
    private PipelineInstanceNode pipelineInstanceNode2;
    private PipelineTask pipelineTask1;
    private PipelineTask pipelineTask2;
    private PipelineTask pipelineTask3;
    private PipelineTask pipelineTask4;

    private PipelineDefinition pipelineDef;
    private PipelineDefinitionNode pipelineDefNode1;
    private PipelineDefinitionNode pipelineDefNode2;
    private ParameterSet parameterSet;
    private PipelineModuleDefinition moduleDef;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {

        pipelineDefinitionCrud = new PipelineDefinitionCrud();
        pipelineInstanceCrud = new PipelineInstanceCrud();
        pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        pipelineTaskCrud = new PipelineTaskCrud();
        pipelineOperations = new PipelineOperations();

        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        parameterSetCrud = new ParameterSetCrud();
    }

    private void populateObjects() {

        DatabaseTransactionFactory.performTransaction(() -> {

            // create a module param set def
            parameterSet = new ParameterSet("test mps1");
            parameterSet.setTypedParameters(new TestModuleParameters().getParameters());
            parameterSet = parameterSetCrud.merge(parameterSet);

            // create a module def
            moduleDef = new PipelineModuleDefinition("Test-1");
            moduleDef = pipelineModuleDefinitionCrud.merge(moduleDef);

            // Create a pipeline definition.
            pipelineDef = new PipelineDefinition(TEST_PIPELINE_NAME);

            // create some pipeline def nodes
            pipelineDefNode1 = new PipelineDefinitionNode(moduleDef.getName(),
                pipelineDef.getName());

            pipelineDefNode2 = new PipelineDefinitionNode(moduleDef.getName(),
                pipelineDef.getName());

            pipelineDef.getRootNodes().add(pipelineDefNode1);
            pipelineDefNode1.getNextNodes().add(pipelineDefNode2);

            pipelineDef = pipelineDefinitionCrud.merge(pipelineDef);

            pipelineInstance = createPipelineInstance();

            pipelineInstanceNode1 = createPipelineInstanceNode(pipelineDefNode1);
            pipelineInstanceNodeCrud.persist(pipelineInstanceNode1);

            pipelineTask1 = createPipelineTask(pipelineInstanceNode1);
            pipelineTask1.setState(PipelineTask.State.PROCESSING);
            pipelineTaskCrud.persist(pipelineTask1);

            pipelineTask2 = createPipelineTask(pipelineInstanceNode1);
            pipelineTask2.setState(PipelineTask.State.COMPLETED);
            pipelineTaskCrud.persist(pipelineTask2);

            pipelineInstanceNode2 = createPipelineInstanceNode(pipelineDefNode2);
            pipelineInstanceNodeCrud.persist(pipelineInstanceNode2);

            pipelineTask3 = createPipelineTask(pipelineInstanceNode2);
            pipelineTask3.setState(PipelineTask.State.PROCESSING);
            pipelineTaskCrud.persist(pipelineTask3);

            pipelineTask4 = createPipelineTask(pipelineInstanceNode2);
            pipelineTask4.setState(PipelineTask.State.ERROR);
            pipelineTaskCrud.persist(pipelineTask4);

            pipelineInstanceCrud.persist(pipelineInstance);

            return null;
        });
    }

    private PipelineInstance createPipelineInstance() throws PipelineException {
        PipelineInstance pipelineInstance = new PipelineInstance(pipelineDef);
        pipelineInstance.putParameterSet(new ClassWrapper<>(new TestPipelineParameters()),
            parameterSet);
        return pipelineInstance;
    }

    private PipelineInstanceNode createPipelineInstanceNode(PipelineDefinitionNode pipelineDefNode)
        throws PipelineException {
        return new PipelineInstanceNode(pipelineInstance, pipelineDefNode, moduleDef);
    }

    private PipelineTask createPipelineTask(PipelineInstanceNode parentPipelineInstanceNode)
        throws PipelineException {
        PipelineTask pipelineTask = new PipelineTask(pipelineInstance, parentPipelineInstanceNode);
        UnitOfWork uow = PipelineExecutor.generateUnitsOfWork(new SingleUnitOfWorkGenerator(), null)
            .get(0);
        pipelineTask.setUowTaskParameters(uow.getParameters());
        pipelineTask.setWorkerHost(TEST_WORKER_NAME);
        pipelineTask.setSoftwareRevision("42");
        return pipelineTask;
    }

    private int pipelineInstanceCount() {
        PipelineInstanceCrud crud = new PipelineInstanceCrud();
        return crud.uniqueResult(crud.createZiggyQuery(PipelineInstance.class, Long.class).count())
            .intValue();
    }

    private int pipelineTaskCount() {
        PipelineTaskCrud crud = new PipelineTaskCrud();
        return crud.uniqueResult(crud.createZiggyQuery(PipelineTask.class, Long.class).count())
            .intValue();
    }

    private int pipelineTaskWithErrorsCount() {
        PipelineTaskCrud crud = new PipelineTaskCrud();
        return crud.uniqueResult(crud.createZiggyQuery(PipelineTask.class, Long.class)
            .column(PipelineTask_.state)
            .in(PipelineTask.State.ERROR)
            .count()).intValue();
    }

    /**
     * Stores a new PipelineInstance in the db, then retrieves it and makes sure it matches what was
     * put in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieveInstance() throws Exception {
        populateObjects();

        // Retrieve
        PipelineInstance actualPipelineInstance = (PipelineInstance) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineInstance pi = pipelineInstanceCrud.retrieve(pipelineInstance.getId());
                ZiggyUnitTestUtils.initializePipelineInstance(pi);
                return pi;
            });

        ReflectionEquals comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.lastChangedUser");
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.assertEquals("PipelineInstance", pipelineInstance, actualPipelineInstance);

        assertEquals("PipelineInstance count", 1, pipelineInstanceCount());
        assertEquals("PipelineTask count", 4, pipelineTaskCount());
    }

    /**
     * Stores a new PipelineInstanceNode in the db, then retrieves it and makes sure it matches what
     * was put in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieveInstanceNode() throws Exception {
        populateObjects();

        // Retrieve
        PipelineInstanceNode actualPipelineInstanceNode = (PipelineInstanceNode) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineInstanceNode node = pipelineInstanceNodeCrud.retrieve(pipelineInstance,
                    pipelineDefNode1);
                ZiggyUnitTestUtils.initializePipelineInstanceNode(node);
                return node;
            });

        ReflectionEquals comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.lastChangedUser");
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.assertEquals("PipelineInstanceNode", pipelineInstanceNode1,
            actualPipelineInstanceNode);
    }

    /**
     * Stores a new PipelineInstanceNode in the db, then retrieves it using retrieveAll and makes
     * sure it matches what was put in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieveAllInstanceNodes() throws Exception {
        populateObjects();

        // Retrieve
        @SuppressWarnings("unchecked")
        List<PipelineInstanceNode> actualPipelineInstanceNodes = (List<PipelineInstanceNode>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<PipelineInstanceNode> nodes = pipelineInstanceNodeCrud
                    .retrieveAll(pipelineInstance);
                for (PipelineInstanceNode node : nodes) {
                    ZiggyUnitTestUtils.initializePipelineInstanceNode(node);
                }
                return nodes;
            });

        assertEquals("actualPipelineInstanceNodes.size() == 2", 2,
            actualPipelineInstanceNodes.size());

        ReflectionEquals comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.lastChangedUser");
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.assertEquals("PipelineInstanceNode", pipelineInstanceNode1,
            actualPipelineInstanceNodes.get(0));
    }

    /**
     * Stores a new PipelineTask in the db, then retrieves it and makes sure it matches what was put
     * in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieveTask() throws Exception {
        populateObjects();

        // Retrieve
        PipelineTask actualPipelineTask = (PipelineTask) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineTask pi = pipelineTaskCrud.retrieve(pipelineTask1.getId());
                ZiggyUnitTestUtils.initializePipelineTask(pi);
                return pi;
            });

        ReflectionEquals comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.excludeField(".*\\.classname");
        comparer.excludeField(".*\\.xmlParameters");
        comparer.assertEquals("PipelineTask", pipelineTask1, actualPipelineTask);

        assertEquals("PipelineInstance count", 1, pipelineInstanceCount());
        assertEquals("PipelineTask count", 4, pipelineTaskCount());

        List<String> nodeRevisions = pipelineTaskCrud
            .distinctSoftwareRevisions(pipelineInstanceNode1);
        assertEquals("nodeRevisions count", 1, nodeRevisions.size());

        List<String> instanceRevisions = pipelineTaskCrud
            .distinctSoftwareRevisions(pipelineInstance);
        assertEquals("instanceRevisions count", 1, instanceRevisions.size());
    }

    @Test
    public void testStoreAndRetrieveTasks() throws Exception {
        populateObjects();

        // Retrieve
        @SuppressWarnings("unchecked")
        List<PipelineTask> actualPipelineTasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> {
                Set<Long> taskIds = new HashSet<>();
                taskIds.add(pipelineTask1.getId());
                taskIds.add(pipelineTask2.getId());

                List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll(taskIds);
                for (PipelineTask task : tasks) {
                    ZiggyUnitTestUtils.initializePipelineTask(task);
                }
                return tasks;
            });

        List<PipelineTask> expectedPipelineTasks = new ArrayList<>();
        expectedPipelineTasks.add(pipelineTask1);
        expectedPipelineTasks.add(pipelineTask2);

        Assert.assertEquals(expectedPipelineTasks, actualPipelineTasks);
    }

    @Test
    public void testStoreAndRetrieveTasksEmptyInputSet() throws Exception {
        populateObjects();

        // Retrieve
        @SuppressWarnings("unchecked")
        List<PipelineTask> actualPipelineTasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> {
                Set<Long> taskIds = new HashSet<>();
                List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll(taskIds);
                for (PipelineTask task : tasks) {
                    ZiggyUnitTestUtils.initializePipelineTask(task);
                }
                return tasks;
            });

        List<PipelineTask> expectedPipelineTasks = new ArrayList<>();

        Assert.assertEquals(expectedPipelineTasks, actualPipelineTasks);
    }

    /**
     * Stores a new PipelineTask in the db, then retrieves it and makes sure it matches what was put
     * in
     *
     * @throws Exception
     */
    @Test
    public void testInstanceState() throws Exception {
        populateObjects();

        // Retrieve
        TaskCounts actualState = (TaskCounts) DatabaseTransactionFactory
            .performTransaction(() -> pipelineOperations.taskCounts(pipelineInstance));

        ReflectionEquals comparer = new ReflectionEquals();

        TaskCounts expectedState = new TaskCounts(4, 4, 1, 1);

        comparer.assertEquals("instanceState", expectedState, actualState);
    }

    @Test
    public void testEditPipelineInstance() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineInstance mpi = pipelineInstanceCrud.retrieve(pipelineInstance.getId());
            editPipelineInstance(mpi);
            return null;
        });

        // Retrieve
        PipelineInstance actualPipelineInstance = (PipelineInstance) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineInstance pi = pipelineInstanceCrud.retrieve(pipelineInstance.getId());
                ZiggyUnitTestUtils.initializePipelineInstance(pi);
                return pi;
            });

        PipelineInstance expectedPipelineInstance = createPipelineInstance();
        editPipelineInstance(expectedPipelineInstance);

        ReflectionEquals comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.id");
        comparer.excludeField(".*\\.created");
        comparer.excludeField(".*\\.lastChangedTime");

        comparer.assertEquals("PipelineInstance", expectedPipelineInstance, actualPipelineInstance);

        assertEquals("PipelineInstance count", 1, pipelineInstanceCount());
        assertEquals("PipelineTask count", 4, pipelineTaskCount());

        assertTrue("isInstanceComplete",
            actualPipelineInstance.getState() == PipelineInstance.State.COMPLETED);
    }

    /**
     * simulate modifications made by a user
     *
     * @param pipelineDef
     */
    private void editPipelineInstance(PipelineInstance pipelineInstance) {
        pipelineInstance.setState(PipelineInstance.State.COMPLETED);
    }

    @Test
    public void testEditPipelineTask() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTask pTask = pipelineTaskCrud.retrieve(pipelineTask1.getId());
            editPipelineTask(pTask);
            return null;
        });

        // Retrieve
        PipelineTask actualPipelineTask = (PipelineTask) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineTask pt = pipelineTaskCrud.retrieve(pipelineTask1.getId());
                ZiggyUnitTestUtils.initializePipelineTask(pt);
                return pt;
            });

        PipelineTask expectedPipelineTask = createPipelineTask(pipelineInstanceNode1);
        editPipelineTask(expectedPipelineTask);

        ReflectionEquals comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.id");
        comparer.excludeField(".*\\.created");
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.excludeField(".*\\.classname");
        comparer.excludeField(".*\\.xmlParameters");

        comparer.assertEquals("PipelineTask", expectedPipelineTask, actualPipelineTask);

        assertEquals("PipelineInstance count", 1, pipelineInstanceCount());
        assertEquals("PipelineTask count", 4, pipelineTaskCount());
    }

    /**
     * simulate modifications made by a user
     *
     * @param pipelineDef
     */
    private void editPipelineTask(PipelineTask pipelineTask) {
        pipelineTask.setState(PipelineTask.State.COMPLETED);
    }

    @Test
    public void testRetrieveByDateStatesTypes() {
        populateObjects();

        State[] states = { State.INITIALIZED };
        String[] types = { "foo" }; // wrong
        List<PipelineInstance> pipelineInstances = pipelineInstanceCrud.retrieve(new Date(0),
            new Date(Long.MAX_VALUE), states, types);
        assertEquals(0, pipelineInstances.size());

        states = new State[] { State.ERRORS_RUNNING }; // wrong
        types = new String[] { TEST_PIPELINE_NAME };
        pipelineInstances = pipelineInstanceCrud.retrieve(new Date(0), new Date(Long.MAX_VALUE),
            states, types);
        assertEquals(0, pipelineInstances.size());

        states = new State[] { State.INITIALIZED };
        types = new String[] { TEST_PIPELINE_NAME };
        pipelineInstances = pipelineInstanceCrud.retrieve(new Date(0), new Date(Long.MAX_VALUE),
            states, types);
        assertEquals(1, pipelineInstances.size());
        assertEquals(pipelineInstance, pipelineInstances.get(0));
    }

    @Test
    public void testClearStaleState() throws Exception {
        // Create
        populateObjects();

        // Add an end node
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineInstance.setEndNode(pipelineInstanceNode2);
            pipelineInstance = pipelineInstanceCrud.merge(pipelineInstance);
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {
            assertEquals("pipelineTaskWithErrorsCount count", 1, pipelineTaskWithErrorsCount());

            ClearStaleStateResults staleStateResults = pipelineTaskCrud.clearStaleState();

            assertEquals("stale row count", 2, staleStateResults.totalUpdatedTaskCount);
            assertEquals("unique instance ids count", 1,
                staleStateResults.uniqueInstanceIds.size());
            assertEquals("unique instance id", pipelineInstance.getId(),
                staleStateResults.uniqueInstanceIds.iterator().next());
            assertEquals("pipelineTaskWithErrorsCount count", 3, pipelineTaskWithErrorsCount());

            TaskCounts actualState = pipelineOperations.taskCounts(pipelineInstance);

            TaskCounts expectedState = new TaskCounts(4, 4, 1, 3);

            ReflectionEquals comparer = new ReflectionEquals();
            comparer.assertEquals("instanceState", expectedState, actualState);
            return null;
        });
    }

    @Test
    public void testRetrieveAllPipelineInstancesForPipelineInstanceIds() {
        populateObjects();

        PipelineInstance pipelineInstance2 = new PipelineInstance();

        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineInstanceCrud.persist(pipelineInstance2);
            return null;
        });

        Collection<Long> pipelineInstanceIds = new ArrayList<>();
        pipelineInstanceIds.add(pipelineInstance2.getId());

        List<PipelineInstance> actualPipelineInstances = pipelineInstanceCrud
            .retrieveAll(pipelineInstanceIds);

        List<PipelineInstance> expectedPipelineInstances = new ArrayList<>();
        expectedPipelineInstances.add(pipelineInstance2);

        assertEquals(expectedPipelineInstances, actualPipelineInstances);
    }

    @Test
    public void testUpdateName() {
        populateObjects();

        // The name should start out as empty or null.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineInstance instance = pipelineInstanceCrud.retrieve(1L);
            assertTrue(StringUtils.isEmpty(instance.getName()));
            return null;
        });

        // Update the name.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineInstanceCrud.updateName(1L, "test");
            return null;
        });

        // Check the name again.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineInstance instance = pipelineInstanceCrud.retrieve(1L);
            assertEquals("test", instance.getName());
            return null;
        });
    }

    @Test
    public void testInstanceIdsForModuleName() {
        populateObjects();

        // If I use the named module, I should get back 1 instance.
        DatabaseTransactionFactory.performTransaction(() -> {
            List<PipelineInstance> instances = pipelineInstanceCrud.instanceIdsForModule("Test-1");
            assertEquals(1, instances.size());
            assertEquals(Long.valueOf(1L), instances.get(0).getId());
            return null;
        });

        // If I use a module name that has no instances, I should get an empty result.
        // If I use the named module, I should get back 1 instance.
        DatabaseTransactionFactory.performTransaction(() -> {
            List<PipelineInstance> instances = pipelineInstanceCrud.instanceIdsForModule("Test-2");
            assertEquals(0, instances.size());
            return null;
        });
    }
}
