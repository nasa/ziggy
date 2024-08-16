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
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.services.database.DatabaseOperations;

// TODO Rename to PipelineDefinitionOperationsTest and adjust
//
// Only Operations classes should extend DatabaseOperations classes. CRUD classes should not be
// tested directly, but indirectly through their associated Operations class.
//
// If there is test code that isn't appropriate for a production operations class, move it to an
// inner class called TestOperations that extends DatabaseOperations.

/**
 * Tests for {@link PipelineDefinitionCrud} Tests that objects can be stored, retrieved, and edited
 * and that mapping metadata (associations, cascade rules, etc.) are setup correctly and work as
 * expected.
 * <p>
 *
 * @author Todd Klaus
 */
public class PipelineDefinitionCrudTest {
    private static final String TEST_PIPELINE_NAME_1 = "Test Pipeline 1";

    private PipelineDefinitionCrud pipelineDefinitionCrud;

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private ParameterSetCrud parameterSetCrud;

    private PipelineModuleDefinition expectedModuleDef1;
    private PipelineModuleDefinition expectedModuleDef2;
    private PipelineModuleDefinition expectedModuleDef3;
    private ReflectionEquals comparer;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineDefinitionCrud = new PipelineDefinitionCrud();
        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
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

    private PipelineDefinition populateObjects() {

        // create a module param set def
        ParameterSet expectedParamSet = new ParameterSet("test mps1");
        expectedParamSet.getParameters().add(new Parameter("value", "42", ZiggyDataType.ZIGGY_INT));
        expectedParamSet = testOperations.merge(expectedParamSet);

        // create a few module defs
        expectedModuleDef1 = new PipelineModuleDefinition("Test-1");
        expectedModuleDef1 = testOperations.merge(expectedModuleDef1);

        expectedModuleDef2 = new PipelineModuleDefinition("Test-2");
        expectedModuleDef2 = testOperations.merge(expectedModuleDef2);

        expectedModuleDef3 = new PipelineModuleDefinition("Test-3");
        expectedModuleDef3 = testOperations.merge(expectedModuleDef3);

        // create a pipeline def
        PipelineDefinition pd = createPipelineDefinition();
        return testOperations.merge(pd);
    }

    private PipelineDefinition createPipelineDefinition() {
        PipelineDefinition pipelineDef = new PipelineDefinition(TEST_PIPELINE_NAME_1);
        PipelineDefinitionNode pipelineNode1 = new PipelineDefinitionNode(
            expectedModuleDef1.getName(), pipelineDef.getName());
        PipelineDefinitionNode pipelineNode2 = new PipelineDefinitionNode(
            expectedModuleDef2.getName(), pipelineDef.getName());
        pipelineNode1.getNextNodes().add(pipelineNode2);

        pipelineDef.addRootNode(pipelineNode1);

        return pipelineDef;
    }

    private int pipelineNodeCount() {
        ZiggyQuery<PipelineDefinitionNode, Long> query = pipelineDefinitionCrud
            .createZiggyQuery(PipelineDefinitionNode.class, Long.class);
        Long count = pipelineDefinitionCrud.uniqueResult(query.count());
        return count.intValue();
    }

    private int pipelineModuleDefinitionCount() {
        ZiggyQuery<PipelineModuleDefinition, Long> query = pipelineModuleDefinitionCrud
            .createZiggyQuery(PipelineModuleDefinition.class, Long.class);
        Long count = pipelineModuleDefinitionCrud.uniqueResult(query.count());
        return count.intValue();
    }

    private int pipelineModuleParamSetCount() {
        ZiggyQuery<ParameterSet, Long> query = parameterSetCrud.createZiggyQuery(ParameterSet.class,
            Long.class);
        Long count = parameterSetCrud.uniqueResult(query.count());
        return count.intValue();
    }

    /**
     * Returns a new instance that is an exact copy of the original {@link PipelineDefinition}.
     */
    private PipelineDefinition copyOf(PipelineDefinition original) throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {
        PipelineDefinition copy = new PipelineDefinition();
        copy.setName(original.getName());
        copy.setDescription(original.getDescription());
        setOptimisticLockValue(copy, original.getOptimisticLockValue());
        setVersion(copy, original.getId(), original.getVersion(), original.isLocked());
        List<PipelineDefinitionNode> rootNodes = copy.getRootNodes();
        for (PipelineDefinitionNode originalNode : original.getRootNodes()) {
            rootNodes.add(new PipelineDefinitionNodeOperations().deepCopy(originalNode));
        }
        return copy;
    }

    /**
     * Sets the values of the version and locked fields. This allows us to create an exact copy of
     * an instance of {@link PipelineDefinition}, which we need for these tests but not in normal
     * operation.
     */
    private void setVersion(PipelineDefinition pipelineDefinition, long id, int version,
        boolean locked) throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException {
        Field versionField = pipelineDefinition.getClass()
            .getSuperclass()
            .getDeclaredField("version");
        versionField.setAccessible(true);
        versionField.set(pipelineDefinition, version);
        Field lockedField = pipelineDefinition.getClass()
            .getSuperclass()
            .getDeclaredField("locked");
        lockedField.setAccessible(true);
        lockedField.set(pipelineDefinition, locked);
        Field idField = pipelineDefinition.getClass().getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(pipelineDefinition, id);
    }

    /**
     * simulate modifications made by a user
     *
     * @param pipelineDef
     */
    private void editPipelineDef(PipelineDefinition pipelineDef) {
        pipelineDef.setDescription("new description");
        pipelineDef.updateAuditInfo();
    }

    /**
     * Sets the value of private field optimisticLockValue. This allows the test to emulate
     * something that would ordinarily be done by the database, which is why it's okay to use
     * reflection to do it.
     */
    private void setOptimisticLockValue(PipelineDefinition pipelineDefinition, int dirtyValue)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException {
        Field dirtyField = pipelineDefinition.getClass()
            .getSuperclass()
            .getDeclaredField("optimisticLockValue");
        dirtyField.setAccessible(true);
        dirtyField.set(pipelineDefinition, dirtyValue);
    }

    /**
     * Stores a new PipelineDefinition in the db, then retrieves it and makes sure it matches what
     * was put in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieve() throws Exception {

        PipelineDefinition expectedPipelineDef = populateObjects();

        // Retrieve
        PipelineDefinition actualPipelineDef = testOperations
            .pipelineDefinition(TEST_PIPELINE_NAME_1);
        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        List<PipelineDefinition> latestVersions = pipelineDefinitionCrud.retrieveLatestVersions();
        assertEquals("latestVersions count", 1, latestVersions.size());
        comparer.assertEquals("latest version", expectedPipelineDef, latestVersions.get(0));

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    @Test
    public void testEditPipelineDefinition() throws Exception {
        // Create
        populateObjects();

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());

        // Retrieve & Edit
        testOperations.editPipelineDefinition(TEST_PIPELINE_NAME_1);

        // Retrieve
        PipelineDefinition actualPipelineDef = testOperations
            .pipelineDefinition(TEST_PIPELINE_NAME_1);

        // Create & Edit
        PipelineDefinition expectedPipelineDef = testOperations.editAndSetLockValue();

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        // The retrieved object should have its Hibernate-side version number incremented.
        assertEquals(1, expectedPipelineDef.getOptimisticLockValue());

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * Demonstrates optimistic locking of PipelineDefinition instances: once the database version of
     * an object has been changed, the original (unchanged) version cannot be stored because it's
     * out of date.
     */
    @Test(expected = PipelineException.class)
    public void testOptimisticLocking() {
        // Create
        PipelineDefinition pd = populateObjects();

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());

        // Retrieve & Edit
        testOperations.editPipelineDefinition(TEST_PIPELINE_NAME_1);

        // Attempting to commit the original version should be blocked by the
        // opportunistic locking system.
        testOperations.merge(pd);
    }

    @Test
    public void testCreateOrUpdateDuplicateInstance() throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {

        PipelineDefinition pd = populateObjects();
        pd.setInstancePriority(PipelineInstance.Priority.HIGHEST);

        // Executing with the same object but changed content does the right thing
        testOperations.merge(pd);

        List<PipelineDefinition> pipelineDefinitions0 = testOperations
            .pipelineDefinitions(pd.getName());

        assertEquals(1, pipelineDefinitions0.size());
        assertEquals(PipelineInstance.Priority.HIGHEST,
            pipelineDefinitions0.get(0).getInstancePriority());

        // Use the same content but a new object.
        testOperations.merge(copyOf(pipelineDefinitions0.get(0)));

        List<PipelineDefinition> pipelineDefinitions = testOperations
            .pipelineDefinitions(pd.getName());
        assertEquals(1, pipelineDefinitions.size());
    }

    @Test
    public void testEditPipelineDefinitionAddNextNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.addNextNode(TEST_PIPELINE_NAME_1);

        // Retrieve
        PipelineDefinition actualPipelineDef = testOperations
            .pipelineDefinition(TEST_PIPELINE_NAME_1);

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefAddNextNode(expectedPipelineDef);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 3, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user add a new node after the last node: N1 -> N2 -> N3(new)
     *
     * @param pipelineDef
     * @throws PipelineException
     */
    private PipelineDefinitionNode editPipelineDefAddNextNode(PipelineDefinition pipelineDef) {
        PipelineDefinitionNode newPipelineNode = new PipelineDefinitionNode(
            expectedModuleDef3.getName(), pipelineDef.getName());
        pipelineDef.getRootNodes().get(0).getNextNodes().get(0).getNextNodes().add(newPipelineNode);
        return newPipelineNode;
    }

    @Test
    public void testEditPipelineDefinitionAddBranchNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.addBranchNode(TEST_PIPELINE_NAME_1);

        // Retrieve
        PipelineDefinition actualPipelineDef = testOperations
            .pipelineDefinition(TEST_PIPELINE_NAME_1);

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefAddBranchNode(expectedPipelineDef);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 3, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user add a new node branch off the second node: N1 -> N2 \>
     * N3(new)
     *
     * @param pipelineDef
     * @throws PipelineException
     */
    private PipelineDefinitionNode editPipelineDefAddBranchNode(PipelineDefinition pipelineDef) {
        PipelineDefinitionNode newPipelineNode = new PipelineDefinitionNode(
            expectedModuleDef3.getName(), pipelineDef.getName());
        pipelineDef.getRootNodes().get(0).getNextNodes().add(newPipelineNode);
        return newPipelineNode;
    }

    @Test
    public void testEditPipelineDefinitionChangeNodeModule() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.changeNodeModule(TEST_PIPELINE_NAME_1);
        // Retrieve
        PipelineDefinition actualPipelineDef = testOperations
            .pipelineDefinition(TEST_PIPELINE_NAME_1);

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefChangeNodeModule(expectedPipelineDef);
        expectedPipelineDef.populateXmlFields();

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user change node module for first node from module1 to
     * module3
     *
     * @param pipelineDef
     */
    private void editPipelineDefChangeNodeModule(PipelineDefinition pipelineDef) {
        pipelineDef.getRootNodes().get(0).setModuleName(expectedModuleDef3.getName());
    }

    @Test
    public void testEditPipelineDefinitionDeleteLastNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.deleteLastNode(TEST_PIPELINE_NAME_1);

        // Retrieve
        PipelineDefinition actualPipelineDef = testOperations
            .pipelineDefinition(TEST_PIPELINE_NAME_1);

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefDeleteLastNode(expectedPipelineDef);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 1, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user delete last node
     *
     * @param pipelineDef
     */
    private void editPipelineDefDeleteLastNode(PipelineDefinition pipelineDef) {
        List<PipelineDefinitionNode> nextNodes = pipelineDef.getRootNodes().get(0).getNextNodes();

        for (PipelineDefinitionNode nextNode : nextNodes) {
            pipelineDefinitionCrud.remove(nextNode);
        }
        nextNodes.clear();
    }

    @Test
    public void testEditPipelineDefinitionDeleteAllNodes() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        testOperations.deleteAllNodes(TEST_PIPELINE_NAME_1);

        // Retrieve
        PipelineDefinition actualPipelineDef = testOperations
            .pipelineDefinition(TEST_PIPELINE_NAME_1);

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefDeleteAllNodes(expectedPipelineDef);
        setOptimisticLockValue(expectedPipelineDef, 1);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 0, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user delete all nodes
     *
     * @param pipelineDef
     */
    private void editPipelineDefDeleteAllNodes(PipelineDefinition pipelineDef) {
        pipelineDefinitionCrud.deleteAllPipelineNodes(pipelineDef);
    }

    @Test
    public void testRetrievePipelineDefinitionNamesInUse() throws Exception {
        // No pipeline definitions at all. Should be empty.
        assertEquals(0, pipelineDefinitionCrud.retrievePipelineDefinitionNamesInUse().size());

        // Add a pipeline definition, but without an associated pipeline
        // instances. Should return an empty list.
        // Create
        PipelineDefinition pipelineDefinition = populateObjects();

        assertEquals(0, pipelineDefinitionCrud.retrievePipelineDefinitionNamesInUse().size());

        // Now, lock the pipeline definition. Should return a single item.
        testOperations.lockPipelineDefinition(pipelineDefinition.getName());
        List<String> pipelineDefinitions = testOperations.pipelineDefinitionNames();
        assertEquals(1, pipelineDefinitions.size());
        String name = pipelineDefinitions.get(0);
        assertEquals(TEST_PIPELINE_NAME_1, name);
    }

    @Test
    public void testNewInstance() {
        PipelineDefinition pipelineDefinition = populateObjects();
        PipelineDefinition pipelineDefinitionCopy = pipelineDefinition.newInstance();

        assertNotEquals(pipelineDefinition, pipelineDefinitionCopy);

        assertEquals("Copy of " + pipelineDefinition, pipelineDefinitionCopy.getName());
        assertEquals(0, pipelineDefinitionCopy.getVersion());
        assertEquals(false, pipelineDefinitionCopy.isLocked());
        assertEquals(null, pipelineDefinitionCopy.getId());

        assertEquals(null, pipelineDefinitionCopy.getId());
        assertEquals(pipelineDefinition.getDescription(), pipelineDefinitionCopy.getDescription());
        assertEquals(pipelineDefinition.getInstancePriority(),
            pipelineDefinitionCopy.getInstancePriority());
        assertEquals(pipelineDefinition.getParameterSetNames(),
            pipelineDefinitionCopy.getParameterSetNames());
        assertEquals(pipelineDefinition.getRootNodes().size(),
            pipelineDefinitionCopy.getRootNodes().size());

        // Compare modules names only due to implementation of PipelineDefinitionNode.equals().
        for (int i = 0; i < pipelineDefinition.getRootNodes().size(); i++) {
            assertEquals(pipelineDefinition.getRootNodes().get(i).getModuleName(),
                pipelineDefinitionCopy.getRootNodes().get(i).getModuleName());
        }
    }

    private class TestOperations extends DatabaseOperations {

        public ParameterSet merge(ParameterSet parameterSet) {
            return performTransaction(() -> new ParameterSetCrud().merge(parameterSet));
        }

        public PipelineModuleDefinition merge(PipelineModuleDefinition pipelineModuleDefinition) {
            return performTransaction(
                () -> new PipelineModuleDefinitionCrud().merge(pipelineModuleDefinition));
        }

        public PipelineDefinition merge(PipelineDefinition pipelineDefinition) {
            return performTransaction(() -> new PipelineDefinitionCrud().merge(pipelineDefinition));
        }

        public PipelineDefinition pipelineDefinition(String pipelineDefinitionName) {
            return performTransaction(() -> {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineDefinitionName);
                ZiggyUnitTestUtils.initializePipelineDefinition(pipelineDefinition);
                return pipelineDefinition;
            });
        }

        public void editPipelineDefinition(String pipelineDefinitionName) {
            performTransaction(() -> {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineDefinitionName);
                editPipelineDef(pipelineDefinition);
            });
        }

        public PipelineDefinition editAndSetLockValue() {
            return performTransaction(() -> {
                PipelineDefinition pipelineDefinition = createPipelineDefinition();
                editPipelineDef(pipelineDefinition);
                setOptimisticLockValue(pipelineDefinition, 1);
                ZiggyUnitTestUtils.initializePipelineDefinition(pipelineDefinition);
                return pipelineDefinition;
            });
        }

        public List<PipelineDefinition> pipelineDefinitions(String pipelineName) {
            return performTransaction(
                () -> new PipelineDefinitionCrud().retrieveAllVersionsForName(pipelineName));
        }

        public void addNextNode(String pipelineDefinitionName) {
            performTransaction(() -> {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineDefinitionName);
                PipelineDefinitionNode newNode = editPipelineDefAddNextNode(pipelineDefinition);
                new SimpleCrud<>().persist(newNode);
                new PipelineDefinitionCrud().merge(pipelineDefinition);
            });
        }

        public void addBranchNode(String pipelineDefinitionName) {
            performTransaction(() -> {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineDefinitionName);
                PipelineDefinitionNode newPipelineDefNode = editPipelineDefAddBranchNode(
                    pipelineDefinition);
                new SimpleCrud<>().persist(newPipelineDefNode);
                new PipelineDefinitionCrud().merge(pipelineDefinition);
            });
        }

        public void changeNodeModule(String pipelineDefinitionName) {
            performTransaction(() -> {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineDefinitionName);
                editPipelineDefChangeNodeModule(pipelineDefinition);
                new PipelineDefinitionCrud().merge(pipelineDefinition);
            });
        }

        public void deleteLastNode(String pipelineDefinitionName) {
            performTransaction(() -> {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineDefinitionName);
                editPipelineDefDeleteLastNode(pipelineDefinition);
                new PipelineDefinitionCrud().merge(pipelineDefinition);
            });
        }

        public void deleteAllNodes(String pipelineDefinitionName) {
            performTransaction(() -> {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineDefinitionName);
                editPipelineDefDeleteAllNodes(pipelineDefinition);
                new PipelineDefinitionCrud().merge(pipelineDefinition);
            });
        }

        public void lockPipelineDefinition(String pipelineDefinitionName) {
            performTransaction(() -> {
                PipelineDefinition pipelineDefinition = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineDefinitionName);
                pipelineDefinition.lock();
                new PipelineDefinitionCrud().merge(pipelineDefinition);
            });
        }

        public List<String> pipelineDefinitionNames() {
            return performTransaction(
                () -> new PipelineDefinitionCrud().retrievePipelineDefinitionNamesInUse());
        }
    }
}
