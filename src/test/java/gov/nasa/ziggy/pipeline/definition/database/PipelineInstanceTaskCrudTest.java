
package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
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
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData_;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCountsTest;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations.ClearStaleStateResults;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.PipelineException;

// TODO Break up into multiple <Class>OperationsTest classes

/**
 * Tests for {@link PipelineInstanceCrud} and {@link PipelineTaskCrud} Tests that objects can be
 * stored, retrieved, and edited and that mapping metadata (associations, cascade rules, etc.) are
 * setup correctly and work as expected.
 * <p>
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
@Category(IntegrationTestCategory.class)
public class PipelineInstanceTaskCrudTest {

    private static final String TEST_PIPELINE_NAME = "Test Pipeline";
    // TODO Remove annotation when constant used in modifyPipelineTask()
    @SuppressWarnings("unused")
    private static final String TEST_WORKER_NAME = "TestWorker";

    private PipelineCrud pipelineCrud;
    private PipelineInstanceCrud pipelineInstanceCrud;
    private PipelineInstanceNodeCrud pipelineInstanceNodeCrud;
    private PipelineTaskCrud pipelineTaskCrud;
    private PipelineStepCrud pipelineStepCrud;
    private ParameterSetCrud parameterSetCrud;
    private PipelineTaskOperations pipelineTaskOperations;
    private PipelineTaskDataOperations pipelineTaskDataOperations;
    private PipelineInstanceOperations pipelineInstanceOperations;

    private PipelineInstance pipelineInstance;
    private PipelineInstanceNode pipelineInstanceNode1;
    private PipelineInstanceNode pipelineInstanceNode2;
    private PipelineTask pipelineTask1;
    private PipelineTask pipelineTask2;
    private PipelineTask pipelineTask3;
    private PipelineTask pipelineTask4;

    private Pipeline pipeline;
    private PipelineNode pipelineDefNode1;
    private PipelineNode pipelineDefNode2;
    private ParameterSet parameterSet;
    private PipelineStep pipelineStep;
    private TestOperations testOperations;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {

        pipelineCrud = new PipelineCrud();
        pipelineInstanceCrud = new PipelineInstanceCrud();
        pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        pipelineTaskCrud = new PipelineTaskCrud();
        pipelineStepCrud = new PipelineStepCrud();
        parameterSetCrud = new ParameterSetCrud();
        pipelineTaskOperations = new PipelineTaskOperations();
        pipelineTaskDataOperations = new PipelineTaskDataOperations();
        pipelineInstanceOperations = new PipelineInstanceOperations();
        testOperations = new TestOperations();
    }

    private PipelineInstance createPipelineInstance() throws PipelineException {
        PipelineInstance pipelineInstance = new PipelineInstance(pipeline);
        pipelineInstance.addParameterSet(parameterSet);
        return pipelineInstance;
    }

    private PipelineInstanceNode createPipelineInstanceNode(PipelineNode pipelineDefNode)
        throws PipelineException {
        return new PipelineInstanceNode(pipelineDefNode, pipelineStep);
    }

    private PipelineTask createPipelineTask(PipelineInstanceNode parentPipelineInstanceNode)
        throws PipelineException {
        UnitOfWork unitOfWork = PipelineExecutor
            .generateUnitsOfWork(new SingleUnitOfWorkGenerator(), null)
            .get(0);
        PipelineTask pipelineTask = new PipelineTask(pipelineInstance, parentPipelineInstanceNode,
            unitOfWork);
        parentPipelineInstanceNode.addPipelineTask(pipelineTask);
        return pipelineTask;
    }

    private void modifyPipelineTask(PipelineTask pipelineTask, ProcessingStep processingStep) {
        modifyPipelineTask(pipelineTask, processingStep, false);
    }

    private void modifyPipelineTask(PipelineTask pipelineTask, ProcessingStep processingStep,
        boolean error) {
        pipelineTaskDataOperations.createPipelineTaskData(pipelineTask, processingStep);
        pipelineTaskDataOperations.setError(pipelineTask, error);
        // TODO Add, when the time comes
        // pipelineTask.setWorkerHost(TEST_WORKER_NAME);
        // pipelineTask.setSoftwareRevision("42");
    }

    // TODO Move transactionless CRUD methods to TestOperations
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

    private int pipelineTasksWithErrorsCount() {
        PipelineTaskDataCrud crud = new PipelineTaskDataCrud();
        return crud.uniqueResult(crud.createZiggyQuery(PipelineTaskData.class, Long.class)
            .column(PipelineTaskData_.error)
            .in(true)
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
        testOperations.populateObjects();

        // Retrieve
        PipelineInstance actualPipelineInstance = testOperations
            .retrieveAndInitializePipelineInstance(pipelineInstance.getId());

        ReflectionEquals comparer = createInstanceComparer();
        comparer.assertEquals("PipelineInstance", pipelineInstance, actualPipelineInstance);

        assertEquals("PipelineInstance count", 1, pipelineInstanceCount());
        assertEquals("PipelineTask count", 4, pipelineTaskCount());
    }

    private ReflectionEquals createInstanceComparer() {
        ReflectionEquals comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.created");
        comparer.excludeField(".*\\.id");
        comparer.excludeField(".*\\.inputDataFileTypes");
        comparer.excludeField(".*\\.modelTypes");
        comparer.excludeField(".*\\.nextNodes");
        comparer.excludeField(".*\\.outputDataFileTypes");
        comparer.excludeField(".*\\.parameterSets");
        comparer.excludeField(".*\\.pipelineTasks");
        return comparer;
    }

    /**
     * Stores a new PipelineInstanceNode in the db, then retrieves it and makes sure it matches what
     * was put in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieveInstanceNode() throws Exception {
        testOperations.populateObjects();

        // Retrieve
        PipelineInstanceNode actualPipelineInstanceNode = testOperations
            .retrieveAndInitializePipelineInstanceNode(pipelineInstanceNode1.getId());

        ReflectionEquals comparer = createInstanceComparer();
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
        testOperations.populateObjects();

        // Retrieve
        List<PipelineInstanceNode> actualPipelineInstanceNodes = testOperations
            .retrieveAndInitializeAllPipelineInstanceNodes(pipelineInstance);

        assertEquals("actualPipelineInstanceNodes.size() == 2", 2,
            actualPipelineInstanceNodes.size());

        ReflectionEquals comparer = createInstanceComparer();

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
        testOperations.populateObjects();

        // Retrieve
        PipelineTask actualPipelineTask = pipelineTaskOperations
            .pipelineTask(pipelineTask1.getId());

        ReflectionEquals comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.excludeField(".*\\.classname");
        comparer.excludeField(".*\\.xmlParameters");
        comparer.assertEquals("PipelineTask", pipelineTask1, actualPipelineTask);

        assertEquals("PipelineInstance count", 1, pipelineInstanceCount());
        assertEquals("PipelineTask count", 4, pipelineTaskCount());
    }

    @Test
    public void testStoreAndRetrieveTasks() throws Exception {
        testOperations.populateObjects();

        // Retrieve
        List<PipelineTask> actualPipelineTasks = pipelineTaskOperations
            .pipelineTasks(Set.of(pipelineTask1.getId(), pipelineTask2.getId()));

        List<PipelineTask> expectedPipelineTasks = new ArrayList<>();
        expectedPipelineTasks.add(pipelineTask1);
        expectedPipelineTasks.add(pipelineTask2);

        Assert.assertEquals(expectedPipelineTasks, actualPipelineTasks);
    }

    @Test
    public void testStoreAndRetrieveTasksEmptyInputSet() throws Exception {
        testOperations.populateObjects();

        // Retrieve
        List<PipelineTask> actualPipelineTasks = pipelineTaskOperations
            .pipelineTasks(new HashSet<>());

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
        testOperations.populateObjects();
        TaskCountsTest.testTaskCounts(4, 0, 1, 1,
            pipelineInstanceOperations.taskCounts(pipelineInstance));
    }

    @Test
    public void testEditPipelineInstance() throws Exception {
        // Create
        testOperations.populateObjects();

        // Retrieve & Edit
        testOperations.retrieveAndEditPipelineInstance(pipelineInstance.getId());

        // Retrieve
        PipelineInstance actualPipelineInstance = testOperations
            .retrieveAndInitializePipelineInstance(pipelineInstance.getId());

        PipelineInstance expectedPipelineInstance = createPipelineInstance();
        editPipelineInstance(expectedPipelineInstance);
        expectedPipelineInstance.addPipelineInstanceNode(pipelineInstanceNode1);
        expectedPipelineInstance.addPipelineInstanceNode(pipelineInstanceNode2);

        ReflectionEquals comparer = createInstanceComparer();
        comparer.assertEquals("PipelineInstance", expectedPipelineInstance, actualPipelineInstance);

        assertEquals("PipelineInstance count", 1, pipelineInstanceCount());
        assertEquals("PipelineTask count", 4, pipelineTaskCount());

        assertTrue("isInstanceComplete",
            actualPipelineInstance.getState() == PipelineInstance.State.COMPLETED);
    }

    /**
     * Simulate modifications made by a user.
     */
    private void editPipelineInstance(PipelineInstance pipelineInstance) {
        pipelineInstance.setState(PipelineInstance.State.COMPLETED);
    }

    @Test
    public void testClearStaleState() throws Exception {
        // Create
        testOperations.populateObjects();

        // Add an end node
        pipelineInstance = testOperations.setPipelineInstanceEndNode(pipelineInstance.getId(),
            pipelineInstanceNode2);

        assertEquals("pipelineTaskWithErrorsCount count", 1, pipelineTasksWithErrorsCount());

        ClearStaleStateResults staleStateResults = pipelineTaskOperations.clearStaleTaskStates();

        assertEquals("stale row count", 2, staleStateResults.totalUpdatedTaskCount);
        assertEquals("unique instance ids count", 1, staleStateResults.uniqueInstanceIds.size());
        assertEquals("unique instance id", pipelineInstance.getId(),
            staleStateResults.uniqueInstanceIds.iterator().next());
        assertEquals("pipelineTaskWithErrorsCount count", 3, pipelineTasksWithErrorsCount());
        TaskCountsTest.testTaskCounts(4, 0, 1, 3,
            pipelineInstanceOperations.taskCounts(pipelineInstance));
    }

    @Test
    public void testRetrieveAllPipelineInstancesForPipelineInstanceIds() {
        testOperations.populateObjects();

        PipelineInstance pipelineInstance2 = testOperations.newPipelineInstance();
        new PipelineInstance();

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
        testOperations.populateObjects();

        // The name should start out as empty or null.
        PipelineInstance instance = testOperations.retrieveAndInitializePipelineInstance(1L);
        assertTrue(StringUtils.isBlank(instance.getName()));

        // Update the name.
        testOperations.updatePipelineInstanceName(1L, "test");

        // Check the name again.
        instance = testOperations.retrieveAndInitializePipelineInstance(1L);
        assertEquals("test", instance.getName());
    }

    @Test
    public void testInstanceIdsForPipelineStepName() {
        testOperations.populateObjects();

        // If I use the named pipeline step, I should get back 1 instance.
        List<PipelineInstance> instances = testOperations
            .pipelineInstancesForPipelineStepName("Test-1");
        assertEquals(1, instances.size());
        assertEquals(Long.valueOf(1L), instances.get(0).getId());

        // If I use a pipeline step name that has no instances, I should get an empty result.
        instances = testOperations.pipelineInstancesForPipelineStepName("Test-2");
        assertEquals(0, instances.size());
    }

    private class TestOperations extends DatabaseOperations {

        public void populateObjects() {

            performTransaction(() -> {
                // Create an algorithm parameter set.
                parameterSet = new ParameterSet("test mps1");
                parameterSet.getParameters()
                    .add(new Parameter("value", "42", ZiggyDataType.ZIGGY_INT));
                parameterSet = parameterSetCrud.merge(parameterSet);

                // Create a pipeline step.
                pipelineStep = new PipelineStep("Test-1");
                pipelineStep = pipelineStepCrud.merge(pipelineStep);

                // Create a pipeline.
                pipeline = new Pipeline(TEST_PIPELINE_NAME);

                // Create some pipeline nodes.
                pipelineDefNode1 = new PipelineNode(pipelineStep.getName(), pipeline.getName());

                pipelineDefNode2 = new PipelineNode(pipelineStep.getName(), pipeline.getName());

                pipeline.getRootNodes().add(pipelineDefNode1);
                pipelineDefNode1.getNextNodes().add(pipelineDefNode2);

                pipeline = pipelineCrud.merge(pipeline);

                pipelineInstance = pipelineInstanceCrud.merge(createPipelineInstance());
                pipelineInstanceNode1 = pipelineInstanceNodeCrud
                    .merge(createPipelineInstanceNode(pipelineDefNode1));
                pipelineInstance.addPipelineInstanceNode(pipelineInstanceNode1);
                pipelineInstance = pipelineInstanceCrud.merge(pipelineInstance);

                pipelineTask1 = createPipelineTask(pipelineInstanceNode1);
                pipelineTaskCrud.persist(pipelineTask1);
                modifyPipelineTask(pipelineTask1, ProcessingStep.EXECUTING);

                pipelineTask2 = createPipelineTask(pipelineInstanceNode1);
                pipelineTaskCrud.persist(pipelineTask2);
                modifyPipelineTask(pipelineTask2, ProcessingStep.COMPLETE);

                pipelineInstanceNode2 = pipelineInstanceNodeCrud
                    .merge(createPipelineInstanceNode(pipelineDefNode2));
                pipelineInstance.addPipelineInstanceNode(pipelineInstanceNode2);

                pipelineTask3 = createPipelineTask(pipelineInstanceNode2);
                pipelineTaskCrud.persist(pipelineTask3);
                modifyPipelineTask(pipelineTask3, ProcessingStep.EXECUTING);

                pipelineTask4 = createPipelineTask(pipelineInstanceNode2);
                pipelineTaskCrud.persist(pipelineTask4);
                modifyPipelineTask(pipelineTask4, ProcessingStep.EXECUTING, true);
            });
        }

        public void retrieveAndEditPipelineInstance(long pipelineInstanceId) {
            performTransaction(() -> {
                PipelineInstance mpi = pipelineInstanceCrud.retrieve(pipelineInstanceId);
                editPipelineInstance(mpi);
                pipelineInstanceCrud.merge(mpi);
            });
        }

        public PipelineInstance retrieveAndInitializePipelineInstance(long pipelineInstanceId) {
            return performTransaction(() -> {
                PipelineInstance pi = pipelineInstanceCrud.retrieve(pipelineInstanceId);
                ZiggyUnitTestUtils.initializePipelineInstance(pi);
                return pi;
            });
        }

        public PipelineInstanceNode retrieveAndInitializePipelineInstanceNode(
            long pipelineInstanceNodeId) {
            return performTransaction(() -> {
                PipelineInstanceNode node = pipelineInstanceNodeCrud
                    .retrieve(pipelineInstanceNode1.getId());
                ZiggyUnitTestUtils.initializePipelineInstanceNode(node);
                return node;
            });
        }

        public List<PipelineInstanceNode> retrieveAndInitializeAllPipelineInstanceNodes(
            PipelineInstance pipelineInstance) {
            return performTransaction(() -> {
                List<PipelineInstanceNode> nodes = pipelineInstanceNodeCrud
                    .retrieveAll(pipelineInstance);
                for (PipelineInstanceNode node : nodes) {
                    ZiggyUnitTestUtils.initializePipelineInstanceNode(node);
                }
                return nodes;
            });
        }

        public PipelineInstance setPipelineInstanceEndNode(long pipelineInstanceId,
            PipelineInstanceNode endNode) {
            return performTransaction(() -> {
                PipelineInstance pipelineInstance = pipelineInstanceCrud
                    .retrieve(pipelineInstanceId);
                pipelineInstance.setEndNode(endNode);
                return pipelineInstanceCrud.merge(pipelineInstance);
            });
        }

        public PipelineInstance newPipelineInstance() {
            return performTransaction(() -> pipelineInstanceCrud.merge(new PipelineInstance()));
        }

        public void updatePipelineInstanceName(long pipelineInstanceId, String name) {
            performTransaction(() -> {
                PipelineInstance instance = pipelineInstanceCrud.retrieve(pipelineInstanceId);
                instance.setName(name);
                pipelineInstanceCrud.merge(instance);
            });
        }

        public List<PipelineInstance> pipelineInstancesForPipelineStepName(
            String pipelineStepName) {
            return performTransaction(
                () -> pipelineInstanceCrud.pipelineInstancesForPipelineStep(pipelineStepName));
        }
    }
}
