package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.crud.SimpleCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.util.PipelineException;

// TODO Rename to PipelineOperationsTest and adjust
//
// Only Operations classes should extend DatabaseOperations classes. CRUD classes should not be
// tested directly, but indirectly through their associated Operations class.
//
// If there is test code that isn't appropriate for a production operations class, move it to an
// inner class called TestOperations that extends DatabaseOperations.

/**
 * Tests for {@link PipelineCrud} Tests that objects can be stored, retrieved, and edited and that
 * mapping metadata (associations, cascade rules, etc.) are setup correctly and work as expected.
 * <p>
 *
 * @author Todd Klaus
 */
public class PipelineCrudTest {
    private static final String TEST_PIPELINE_NAME_1 = "Test Pipeline 1";

    private PipelineCrud pipelineCrud;

    private PipelineStepCrud pipelineStepCrud;
    private ParameterSetCrud parameterSetCrud;

    private PipelineStep expectedPipelineStep1;
    private PipelineStep expectedPipelineStep2;
    private PipelineStep expectedPipelineStep3;
    private ReflectionEquals comparer;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineCrud = new PipelineCrud();
        pipelineStepCrud = new PipelineStepCrud();
        parameterSetCrud = new ParameterSetCrud();
        comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.excludeField(".*\\.lastChangedUser.created");
        comparer.excludeField(".*\\.uowProperties.instance");
        comparer.excludeField(".*\\.nodesAndParamSets");
        comparer.excludeField(".*\\.childNodeNames");
        comparer.excludeField(".*\\.rootNodeNames");
        comparer.excludeField(".*\\.pipelineInstances");
        comparer.excludeField(".*\\.pipelineInstanceNodes");
        comparer.excludeField(".*\\.parameterSetNames");
        comparer.excludeField(".*\\.obsoletePipelineParameterSetNames");
    }

    private Pipeline populateObjects() {

        // Create a parameter set.
        ParameterSet expectedParamSet = new ParameterSet("test mps1");
        expectedParamSet.getParameters().add(new Parameter("value", "42", ZiggyDataType.ZIGGY_INT));
        expectedParamSet = testOperations.merge(expectedParamSet);

        // Create a few pipeline steps.
        expectedPipelineStep1 = new PipelineStep("Test-1");
        expectedPipelineStep1 = testOperations.merge(expectedPipelineStep1);

        expectedPipelineStep2 = new PipelineStep("Test-2");
        expectedPipelineStep2 = testOperations.merge(expectedPipelineStep2);

        expectedPipelineStep3 = new PipelineStep("Test-3");
        expectedPipelineStep3 = testOperations.merge(expectedPipelineStep3);

        // Create a pipeline.
        Pipeline pipeline = createPipeline();
        return testOperations.merge(pipeline);
    }

    private Pipeline createPipeline() {
        Pipeline pipeline = new Pipeline(TEST_PIPELINE_NAME_1);
        PipelineNode pipelineNode1 = new PipelineNode(expectedPipelineStep1.getName(),
            pipeline.getName());
        PipelineNode pipelineNode2 = new PipelineNode(expectedPipelineStep2.getName(),
            pipeline.getName());
        pipelineNode1.getNextNodes().add(pipelineNode2);

        pipeline.addRootNode(pipelineNode1);

        return pipeline;
    }

    private int pipelineNodeCount() {
        ZiggyQuery<PipelineNode, Long> query = pipelineCrud.createZiggyQuery(PipelineNode.class,
            Long.class);
        Long count = pipelineCrud.uniqueResult(query.count());
        return count.intValue();
    }

    private int pipelineStepCount() {
        ZiggyQuery<PipelineStep, Long> query = pipelineStepCrud.createZiggyQuery(PipelineStep.class,
            Long.class);
        Long count = pipelineStepCrud.uniqueResult(query.count());
        return count.intValue();
    }

    private int parameterSetCount() {
        ZiggyQuery<ParameterSet, Long> query = parameterSetCrud.createZiggyQuery(ParameterSet.class,
            Long.class);
        Long count = parameterSetCrud.uniqueResult(query.count());
        return count.intValue();
    }

    /**
     * Returns a new instance that is an exact copy of the original {@link Pipeline}.
     */
    private Pipeline copyOf(Pipeline original) throws NoSuchFieldException, SecurityException,
        IllegalArgumentException, IllegalAccessException {
        Pipeline copy = new Pipeline();
        copy.setName(original.getName());
        copy.setDescription(original.getDescription());
        setOptimisticLockValue(copy, original.getOptimisticLockValue());
        setVersion(copy, original.getId(), original.getVersion(), original.isLocked());
        List<PipelineNode> rootNodes = copy.getRootNodes();
        for (PipelineNode originalNode : original.getRootNodes()) {
            rootNodes.add(new PipelineNodeOperations().deepCopy(originalNode));
        }
        return copy;
    }

    /**
     * Sets the values of the version and locked fields. This allows us to create an exact copy of
     * an instance of {@link Pipeline}, which we need for these tests but not in normal operation.
     */
    private void setVersion(Pipeline pipeline, long id, int version, boolean locked)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException {
        Field versionField = pipeline.getClass().getSuperclass().getDeclaredField("version");
        versionField.setAccessible(true);
        versionField.set(pipeline, version);
        Field lockedField = pipeline.getClass().getSuperclass().getDeclaredField("locked");
        lockedField.setAccessible(true);
        lockedField.set(pipeline, locked);
        Field idField = pipeline.getClass().getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(pipeline, id);
    }

    /**
     * Simulate modifications made by a user.
     */
    private void editPipeline(Pipeline pipeline) {
        pipeline.setDescription("new description");
        pipeline.updateAuditInfo();
    }

    /**
     * Sets the value of private field optimisticLockValue. This allows the test to emulate
     * something that would ordinarily be done by the database, which is why it's okay to use
     * reflection to do it.
     */
    private void setOptimisticLockValue(Pipeline pipeline, int dirtyValue)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException {
        Field dirtyField = pipeline.getClass()
            .getSuperclass()
            .getDeclaredField("optimisticLockValue");
        dirtyField.setAccessible(true);
        dirtyField.set(pipeline, dirtyValue);
    }

    /**
     * Stores a new Pipeline in the db, then retrieves it and makes sure it matches what was put in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieve() throws Exception {

        Pipeline expectedPipeline = populateObjects();

        // Retrieve
        Pipeline actualPipeline = testOperations.pipeline(TEST_PIPELINE_NAME_1);
        comparer.assertEquals("Pipeline", expectedPipeline, actualPipeline);

        List<Pipeline> latestVersions = pipelineCrud.retrieveLatestVersions();
        assertEquals("latestVersions count", 1, latestVersions.size());
        comparer.assertEquals("latest version", expectedPipeline, latestVersions.get(0));

        assertEquals("PipelineNode count", 2, pipelineNodeCount());
        assertEquals("PipelineStep count", 3, pipelineStepCount());
        assertEquals("ParameterSet count", 1, parameterSetCount());
    }

    @Test
    public void testEditPipeline() throws Exception {
        // Create
        populateObjects();

        assertEquals("PipelineNode count", 2, pipelineNodeCount());

        // Retrieve & Edit
        testOperations.updatePipeline(TEST_PIPELINE_NAME_1);

        // Retrieve
        Pipeline actualPipeline = testOperations.pipeline(TEST_PIPELINE_NAME_1);

        // Create & Edit
        Pipeline expectedPipeline = testOperations.updatePipelineAndSetLockValue();

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("Pipeline", expectedPipeline, actualPipeline);

        // The retrieved object should have its Hibernate-side version number incremented.
        assertEquals(1, expectedPipeline.getOptimisticLockValue());

        assertEquals("PipelineNode count", 2, pipelineNodeCount());
        assertEquals("PipelineStep count", 3, pipelineStepCount());
        assertEquals("ParameterSet count", 1, parameterSetCount());
    }

    /**
     * Demonstrates optimistic locking of Pipeline instances: once the database version of an object
     * has been changed, the original (unchanged) version cannot be stored because it's out of date.
     */
    @Test(expected = PipelineException.class)
    public void testOptimisticLocking() {
        // Create
        Pipeline pipeline = populateObjects();

        assertEquals("PipelineNode count", 2, pipelineNodeCount());

        // Retrieve & Edit
        testOperations.updatePipeline(TEST_PIPELINE_NAME_1);

        // Attempting to commit the original version should be blocked by the
        // opportunistic locking system.
        testOperations.merge(pipeline);
    }

    @Test
    public void testCreateOrUpdateDuplicateInstance() throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {

        Pipeline pipeline = populateObjects();
        pipeline.setInstancePriority(PipelineInstance.Priority.HIGHEST);

        // Executing with the same object but changed content does the right thing
        testOperations.merge(pipeline);

        List<Pipeline> pipelines0 = testOperations.pipelines(pipeline.getName());

        assertEquals(1, pipelines0.size());
        assertEquals(PipelineInstance.Priority.HIGHEST, pipelines0.get(0).getInstancePriority());

        // Use the same content but a new object.
        testOperations.merge(copyOf(pipelines0.get(0)));

        List<Pipeline> pipelines = testOperations.pipelines(pipeline.getName());
        assertEquals(1, pipelines.size());
    }

    @Test
    public void testEditPipelineAddNextNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.addNextNode(TEST_PIPELINE_NAME_1);

        // Retrieve
        Pipeline actualPipeline = testOperations.pipeline(TEST_PIPELINE_NAME_1);

        Pipeline expectedPipeline = createPipeline();
        editPipelineAddNextNode(expectedPipeline);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("Pipeline", expectedPipeline, actualPipeline);

        assertEquals("PipelineNode count", 3, pipelineNodeCount());
        assertEquals("PipelineStep count", 3, pipelineStepCount());
        assertEquals("ParameterSet count", 1, parameterSetCount());
    }

    /**
     * Simulate modifications made by a user add a new node after the last node: N1 -> N2 ->
     * N3(new).
     */
    private PipelineNode editPipelineAddNextNode(Pipeline pipeline) {
        PipelineNode newPipelineNode = new PipelineNode(expectedPipelineStep3.getName(),
            pipeline.getName());
        pipeline.getRootNodes().get(0).getNextNodes().get(0).getNextNodes().add(newPipelineNode);
        return newPipelineNode;
    }

    @Test
    public void testEditPipelineAddBranchNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.addBranchNode(TEST_PIPELINE_NAME_1);

        // Retrieve
        Pipeline actualPipeline = testOperations.pipeline(TEST_PIPELINE_NAME_1);

        Pipeline expectedPipeline = createPipeline();
        editPipelineAddBranchNode(expectedPipeline);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("Pipeline", expectedPipeline, actualPipeline);

        assertEquals("PipelineNode count", 3, pipelineNodeCount());
        assertEquals("PipelineStep count", 3, pipelineStepCount());
        assertEquals("ParameterSet count", 1, parameterSetCount());
    }

    /**
     * Simulate modifications made by a user add a new node branch off the second node: N1 -> N2 \>
     * N3(new).
     */
    private PipelineNode editPipelineAddBranchNode(Pipeline pipeline) {
        PipelineNode newPipelineNode = new PipelineNode(expectedPipelineStep3.getName(),
            pipeline.getName());
        pipeline.getRootNodes().get(0).getNextNodes().add(newPipelineNode);
        return newPipelineNode;
    }

    @Test
    public void testEditPipelineChangeNodeStep() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.changeNodeStep(TEST_PIPELINE_NAME_1);

        // Retrieve
        Pipeline actualPipeline = testOperations.pipeline(TEST_PIPELINE_NAME_1);

        Pipeline expectedPipeline = createPipeline();
        editPipelineChangeNodeStep(expectedPipeline);
        expectedPipeline.populateXmlFields();

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("Pipeline", expectedPipeline, actualPipeline);

        assertEquals("PipelineNode count", 2, pipelineNodeCount());
        assertEquals("PipelineStep count", 3, pipelineStepCount());
        assertEquals("ParameterSet count", 1, parameterSetCount());
    }

    /**
     * Simulate modifications made by a user to change the node's name (not that they are allowed to
     * do that).
     */
    private void editPipelineChangeNodeStep(Pipeline pipeline) {
        pipeline.getRootNodes().get(0).setPipelineStepName(expectedPipelineStep3.getName());
    }

    @Test
    public void testEditPipelineDeleteLastNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.deleteLastNode(TEST_PIPELINE_NAME_1);

        // Retrieve
        Pipeline actualPipeline = testOperations.pipeline(TEST_PIPELINE_NAME_1);

        Pipeline expectedPipeline = createPipeline();
        editPipelineDeleteLastNode(expectedPipeline);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("Pipeline", expectedPipeline, actualPipeline);

        assertEquals("PipelineNode count", 1, pipelineNodeCount());
        assertEquals("PipelineStep count", 3, pipelineStepCount());
        assertEquals("ParameterSet count", 1, parameterSetCount());
    }

    /**
     * Simulate modifications made by a user delete last node.
     */
    private void editPipelineDeleteLastNode(Pipeline pipeline) {
        List<PipelineNode> nextNodes = pipeline.getRootNodes().get(0).getNextNodes();

        for (PipelineNode nextNode : nextNodes) {
            pipelineCrud.remove(nextNode);
        }
        nextNodes.clear();
    }

    @Test
    public void testRetrievePipelineNamesInUse() throws Exception {
        // No pipelines at all. Should be empty.
        assertEquals(0, pipelineCrud.retrievePipelineNamesInUse().size());

        // Add a pipeline, but without an associated pipeline
        // instances. Should return an empty list.
        Pipeline pipeline = populateObjects();

        assertEquals(0, pipelineCrud.retrievePipelineNamesInUse().size());

        // Now, lock the pipeline. Should return a single item.
        testOperations.lockPipeline(pipeline.getName());
        List<String> pipelines = testOperations.pipelineNames();
        assertEquals(1, pipelines.size());
        String name = pipelines.get(0);
        assertEquals(TEST_PIPELINE_NAME_1, name);
    }

    @Test
    public void testNewInstance() {
        Pipeline pipeline = populateObjects();
        Pipeline pipelineCopy = pipeline.newInstance();

        assertNotEquals(pipeline, pipelineCopy);

        assertEquals("Copy of " + pipeline, pipelineCopy.getName());
        assertEquals(0, pipelineCopy.getVersion());
        assertEquals(false, pipelineCopy.isLocked());
        assertEquals(null, pipelineCopy.getId());

        assertEquals(null, pipelineCopy.getId());
        assertEquals(pipeline.getDescription(), pipelineCopy.getDescription());
        assertEquals(pipeline.getInstancePriority(), pipelineCopy.getInstancePriority());
        assertEquals(pipeline.getParameterSetNames(), pipelineCopy.getParameterSetNames());
        assertEquals(pipeline.getRootNodes().size(), pipelineCopy.getRootNodes().size());

        // Compare node names only due to implementation of PipelineNode.equals().
        for (int i = 0; i < pipeline.getRootNodes().size(); i++) {
            assertEquals(pipeline.getRootNodes().get(i).getPipelineStepName(),
                pipelineCopy.getRootNodes().get(i).getPipelineStepName());
        }
    }

    private class TestOperations extends DatabaseOperations {

        public ParameterSet merge(ParameterSet parameterSet) {
            return performTransaction(() -> new ParameterSetCrud().merge(parameterSet));
        }

        public PipelineStep merge(PipelineStep pipelineStep) {
            return performTransaction(() -> new PipelineStepCrud().merge(pipelineStep));
        }

        public Pipeline merge(Pipeline pipeline) {
            return performTransaction(() -> new PipelineCrud().merge(pipeline));
        }

        public Pipeline pipeline(String pipelineName) {
            return performTransaction(() -> {
                Pipeline pipeline = new PipelineCrud().retrieveLatestVersionForName(pipelineName);
                ZiggyUnitTestUtils.initializePipeline(pipeline);
                return pipeline;
            });
        }

        public void updatePipeline(String pipelineName) {
            performTransaction(() -> {
                Pipeline pipeline = new PipelineCrud().retrieveLatestVersionForName(pipelineName);
                editPipeline(pipeline);
            });
        }

        public Pipeline updatePipelineAndSetLockValue() {
            return performTransaction(() -> {
                Pipeline pipeline = createPipeline();
                editPipeline(pipeline);
                setOptimisticLockValue(pipeline, 1);
                ZiggyUnitTestUtils.initializePipeline(pipeline);
                return pipeline;
            });
        }

        public List<Pipeline> pipelines(String pipelineName) {
            return performTransaction(
                () -> new PipelineCrud().retrieveAllVersionsForName(pipelineName));
        }

        public void addNextNode(String pipelineName) {
            performTransaction(() -> {
                Pipeline pipeline = new PipelineCrud().retrieveLatestVersionForName(pipelineName);
                PipelineNode newNode = editPipelineAddNextNode(pipeline);
                new SimpleCrud<>().persist(newNode);
                new PipelineCrud().merge(pipeline);
            });
        }

        public void addBranchNode(String pipelineName) {
            performTransaction(() -> {
                Pipeline pipeline = new PipelineCrud().retrieveLatestVersionForName(pipelineName);
                PipelineNode newPipelineNode = editPipelineAddBranchNode(pipeline);
                new SimpleCrud<>().persist(newPipelineNode);
                new PipelineCrud().merge(pipeline);
            });
        }

        public void changeNodeStep(String pipelineName) {
            performTransaction(() -> {
                Pipeline pipeline = new PipelineCrud().retrieveLatestVersionForName(pipelineName);
                editPipelineChangeNodeStep(pipeline);
                new PipelineCrud().merge(pipeline);
            });
        }

        public void deleteLastNode(String pipelineName) {
            performTransaction(() -> {
                Pipeline pipeline = new PipelineCrud().retrieveLatestVersionForName(pipelineName);
                editPipelineDeleteLastNode(pipeline);
                new PipelineCrud().merge(pipeline);
            });
        }

        public void lockPipeline(String pipelineName) {
            performTransaction(() -> {
                Pipeline pipeline = new PipelineCrud().retrieveLatestVersionForName(pipelineName);
                pipeline.lock();
                new PipelineCrud().merge(pipeline);
            });
        }

        public List<String> pipelineNames() {
            return performTransaction(() -> new PipelineCrud().retrievePipelineNamesInUse());
        }
    }
}
