
package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.Query;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceAggregateState;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TestModuleParameters;
import gov.nasa.ziggy.pipeline.definition.TestPipelineParameters;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud.ClearStaleStateResults;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.services.security.UserCrud;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.ReflectionEquals;

/**
 * Tests for {@link PipelineInstanceCrud} and {@link PipelineTaskCrud} Tests that objects can be
 * stored, retrieved, and edited and that mapping metadata (associations, cascade rules, etc.) are
 * setup correctly and work as expected.
 *
 * @author Todd Klaus
 */
public class PipelineInstanceTaskCrudTest {

    private static final String TEST_PIPELINE_NAME = "Test Pipeline";
    private static final String TEST_WORKER_NAME = "TestWorker";

    private UserCrud userCrud;

    private User adminUser;

    private PipelineDefinitionCrud pipelineDefinitionCrud;
    private PipelineInstanceCrud pipelineInstanceCrud;
    private PipelineInstanceNodeCrud pipelineInstanceNodeCrud;
    private PipelineTaskCrud pipelineTaskCrud;

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

        userCrud = new UserCrud();

        pipelineDefinitionCrud = new PipelineDefinitionCrud();
        pipelineInstanceCrud = new PipelineInstanceCrud();
        pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        pipelineTaskCrud = new PipelineTaskCrud();

        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        parameterSetCrud = new ParameterSetCrud();
    }

    private void populateObjects() {

        DatabaseTransactionFactory.performTransaction(() -> {
            // create users
            adminUser = new User("admin", "Administrator", "admin@example.com", "x111");
            userCrud.createUser(adminUser);
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {

            // create a module param set def
            parameterSet = new ParameterSet(new AuditInfo(adminUser, new Date()), "test mps1");
            parameterSet.setParameters(new BeanWrapper<Parameters>(new TestModuleParameters()));
            parameterSetCrud.create(parameterSet);

            // create a module def
            moduleDef = new PipelineModuleDefinition(new AuditInfo(adminUser, new Date()),
                "Test-1");
            pipelineModuleDefinitionCrud.create(moduleDef);

            // Create a pipeline definition.
            pipelineDef = new PipelineDefinition(new AuditInfo(adminUser, new Date()),
                TEST_PIPELINE_NAME);

            // create some pipeline def nodes
            pipelineDefNode1 = new PipelineDefinitionNode(moduleDef.getName(),
                pipelineDef.getName().getName());
            pipelineDefNode1.setUnitOfWorkGenerator(
                new ClassWrapper<UnitOfWorkGenerator>(new SingleUnitOfWorkGenerator()));
            pipelineDefNode1.setStartNewUow(true);

            pipelineDefNode2 = new PipelineDefinitionNode(moduleDef.getName(),
                pipelineDef.getName().getName());
            pipelineDefNode2.setUnitOfWorkGenerator(
                new ClassWrapper<UnitOfWorkGenerator>(new SingleUnitOfWorkGenerator()));
            pipelineDefNode2.setStartNewUow(true);

            pipelineDef.getRootNodes().add(pipelineDefNode1);
            pipelineDefNode1.getNextNodes().add(pipelineDefNode2);

            pipelineDefinitionCrud.create(pipelineDef);

            pipelineInstance = createPipelineInstance();
            pipelineInstanceCrud.create(pipelineInstance);

            pipelineInstanceNode1 = createPipelineInstanceNode(pipelineDefNode1, 2, 2, 1, 0);
            pipelineInstanceNodeCrud.create(pipelineInstanceNode1);

            pipelineTask1 = createPipelineTask(pipelineInstanceNode1);
            pipelineTask1.setState(PipelineTask.State.PROCESSING);
            pipelineTaskCrud.create(pipelineTask1);

            pipelineTask2 = createPipelineTask(pipelineInstanceNode1);
            pipelineTask2.setState(PipelineTask.State.COMPLETED);
            pipelineTaskCrud.create(pipelineTask2);

            pipelineInstanceNode2 = createPipelineInstanceNode(pipelineDefNode2, 2, 2, 0, 1);
            pipelineInstanceNodeCrud.create(pipelineInstanceNode2);

            pipelineTask3 = createPipelineTask(pipelineInstanceNode2);
            pipelineTask3.setState(PipelineTask.State.PROCESSING);
            pipelineTaskCrud.create(pipelineTask3);

            pipelineTask4 = createPipelineTask(pipelineInstanceNode2);
            pipelineTask4.setState(PipelineTask.State.ERROR);
            pipelineTaskCrud.create(pipelineTask4);

            return null;
        });
    }

    private PipelineInstance createPipelineInstance() throws PipelineException {
        PipelineInstance pipelineInstance = new PipelineInstance(pipelineDef);
        pipelineInstance.putParameterSet(new ClassWrapper<Parameters>(new TestPipelineParameters()),
            parameterSet);
        return pipelineInstance;
    }

    private PipelineInstanceNode createPipelineInstanceNode(PipelineDefinitionNode pipelineDefNode,
        int numTasks, int numSubmittedTasks, int numCompletedTasks, int numFailedTasks)
        throws PipelineException {
        PipelineInstanceNode pipelineInstanceNode = new PipelineInstanceNode(pipelineInstance,
            pipelineDefNode, moduleDef, numTasks, numSubmittedTasks, numCompletedTasks,
            numFailedTasks);

        return pipelineInstanceNode;
    }

    private PipelineTask createPipelineTask(PipelineInstanceNode parentPipelineInstanceNode)
        throws PipelineException {
        PipelineTask pipelineTask = new PipelineTask(pipelineInstance, parentPipelineInstanceNode);
        pipelineTask.setUowTask(new BeanWrapper<>(new UnitOfWork()));
        pipelineTask.setWorkerHost(TEST_WORKER_NAME);
        pipelineTask.setSoftwareRevision("42");
        return pipelineTask;
    }

    private int pipelineInstanceCount() {
        Query q = DatabaseService.getInstance()
            .getSession()
            .createQuery("select count(*) from PipelineInstance");
        int count = ((Long) q.uniqueResult()).intValue();

        return count;
    }

    private int pipelineTaskCount() {
        Query q = DatabaseService.getInstance()
            .getSession()
            .createQuery("select count(*) from PipelineTask");
        int count = ((Long) q.uniqueResult()).intValue();

        return count;
    }

    private int pipelineTaskWithErrorsCount() {
        Query q = DatabaseService.getInstance()
            .getSession()
            .createQuery("select count(*) from PipelineTask where state = :state");
        q.setParameter("state", PipelineTask.State.ERROR);
        int count = ((Long) q.uniqueResult()).intValue();

        return count;
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
        PipelineInstanceAggregateState actualState = (PipelineInstanceAggregateState) DatabaseTransactionFactory
            .performTransaction(() -> pipelineInstanceCrud.instanceState(pipelineInstance));

        ReflectionEquals comparer = new ReflectionEquals();

        PipelineInstanceAggregateState expectedState = new PipelineInstanceAggregateState(4L, 4L,
            1L, 1L);

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

        State[] states = new State[] { State.INITIALIZED };
        String[] types = new String[] { "foo" }; // wrong
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

        assertEquals("pipelineTaskWithErrorsCount count", 1, pipelineTaskWithErrorsCount());

        ClearStaleStateResults staleStateResults = pipelineTaskCrud
            .clearStaleState(TEST_WORKER_NAME);

        assertEquals("stale row count", 2, staleStateResults.totalUpdatedTaskCount);
        assertEquals("unique instance ids count", 1, staleStateResults.uniqueInstanceIds.size());
        assertEquals("unique instance id", pipelineInstance.getId(),
            staleStateResults.uniqueInstanceIds.iterator().next().intValue());
        assertEquals("pipelineTaskWithErrorsCount count", 3, pipelineTaskWithErrorsCount());

        PipelineInstanceAggregateState actualState = pipelineInstanceCrud
            .instanceState(pipelineInstance);

        PipelineInstanceAggregateState expectedState = new PipelineInstanceAggregateState(4L, 4L,
            1L, 3L);

        ReflectionEquals comparer = new ReflectionEquals();
        comparer.assertEquals("instanceState", expectedState, actualState);
    }

    @Test
    public void testRetrieveAllPipelineInstancesForPipelineInstanceIds() {
        populateObjects();

        PipelineInstance pipelineInstance2 = new PipelineInstance();

        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineInstanceCrud.create(pipelineInstance2);
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
}
