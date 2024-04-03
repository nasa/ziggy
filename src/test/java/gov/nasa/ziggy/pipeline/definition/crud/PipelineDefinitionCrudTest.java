package gov.nasa.ziggy.pipeline.definition.crud;

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
import gov.nasa.ziggy.crud.SimpleCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.TestModuleParameters;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Tests for {@link PipelineDefinitionCrud} Tests that objects can be stored, retrieved, and edited
 * and that mapping metadata (associations, cascade rules, etc.) are setup correctly and work as
 * expected.
 *
 * @author Todd Klaus
 */
public class PipelineDefinitionCrudTest {
    private static final String TEST_PIPELINE_NAME_1 = "Test Pipeline 1";

    private PipelineDefinitionCrud pipelineDefinitionCrud;

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private ParameterSetCrud parameterSetCrud;

    private ParameterSet expectedParamSet;
    private PipelineModuleDefinition expectedModuleDef1;
    private PipelineModuleDefinition expectedModuleDef2;
    private PipelineModuleDefinition expectedModuleDef3;
    private ReflectionEquals comparer;

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
    }

    private PipelineDefinition populateObjects() {

        return (PipelineDefinition) DatabaseTransactionFactory.performTransaction(() -> {

            // create a module param set def
            expectedParamSet = new ParameterSet("test mps1");
            expectedParamSet.setTypedParameters(new TestModuleParameters().getParameters());
            expectedParamSet = parameterSetCrud.merge(expectedParamSet);

            // create a few module defs
            expectedModuleDef1 = new PipelineModuleDefinition("Test-1");
            expectedModuleDef1 = pipelineModuleDefinitionCrud.merge(expectedModuleDef1);

            expectedModuleDef2 = new PipelineModuleDefinition("Test-2");
            expectedModuleDef2 = pipelineModuleDefinitionCrud.merge(expectedModuleDef2);

            expectedModuleDef3 = new PipelineModuleDefinition("Test-3");
            expectedModuleDef3 = pipelineModuleDefinitionCrud.merge(expectedModuleDef3);

            // create a pipeline def
            PipelineDefinition pd = createPipelineDefinition();
            return pipelineDefinitionCrud.merge(pd);
        });
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
            rootNodes.add(new PipelineDefinitionNode(originalNode));
        }
        return copy;
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
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });
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
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDef(modifiedPipelineDef);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

        // Create & Edit
        PipelineDefinition expectedPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition epd = createPipelineDefinition();
                editPipelineDef(epd);
                setOptimisticLockValue(epd, 1);
                ZiggyUnitTestUtils.initializePipelineDefinition(epd);
                return epd;
            });
        // flush changes

        // databaseService.closeCurrentSession(); // clear the cache ,
        // detach
        // the objects

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
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDef(modifiedPipelineDef);
            return null;
        });

        // Attempting to commit the original version should be blocked by the
        // opportunistic locking system.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineDefinitionCrud.merge(pd);
            return null;
        });
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

    @Test
    public void testCreateOrUpdateDuplicateInstance() {

        PipelineDefinition pd = populateObjects();
        pd.setInstancePriority(PipelineInstance.Priority.HIGHEST);

        // Executing with the same object but changed content does the right thing
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineDefinitionCrud.merge(pd);
            return null;
        });

        @SuppressWarnings("unchecked")
        List<PipelineDefinition> pipelineDefinitions0 = (List<PipelineDefinition>) DatabaseTransactionFactory
            .performTransaction(
                () -> pipelineDefinitionCrud.retrieveAllVersionsForName(pd.getName()));

        assertEquals(1, pipelineDefinitions0.size());
        assertEquals(PipelineInstance.Priority.HIGHEST,
            pipelineDefinitions0.get(0).getInstancePriority());

        // Use the same content but a new object.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineDefinitionCrud.merge(copyOf(pipelineDefinitions0.get(0)));
            return null;
        });

        @SuppressWarnings("unchecked")
        List<PipelineDefinition> pipelineDefinitions = (List<PipelineDefinition>) DatabaseTransactionFactory
            .performTransaction(
                () -> pipelineDefinitionCrud.retrieveAllVersionsForName(pd.getName()));

        assertEquals(1, pipelineDefinitions.size());
    }

    @Test
    public void testEditPipelineDefinitionAddNextNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            PipelineDefinitionNode newNode = editPipelineDefAddNextNode(modifiedPipelineDef);
            new SimpleCrud<>().persist(newNode);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

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
        DatabaseTransactionFactory.performTransaction(() -> {

            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            PipelineDefinitionNode newPipelineDefNode = editPipelineDefAddBranchNode(
                modifiedPipelineDef);
            new SimpleCrud<>().persist(newPipelineDefNode);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

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
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDefChangeNodeModule(modifiedPipelineDef);
            return null;
        });
        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

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
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDefDeleteLastNode(modifiedPipelineDef);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

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
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDefDeleteAllNodes(modifiedPipelineDef);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

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

        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineDefinitionCrud.retrieveLatestVersionForName(pipelineDefinition.getName())
                .lock();
            return null;
        });

        List<String> pipelineDefinitions = pipelineDefinitionCrud
            .retrievePipelineDefinitionNamesInUse();

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
        assertEquals(pipelineDefinition.getPipelineParameterSetNames(),
            pipelineDefinitionCopy.getPipelineParameterSetNames());
        assertEquals(pipelineDefinition.getRootNodes().size(),
            pipelineDefinitionCopy.getRootNodes().size());

        // Compare modules names only due to implementation of PipelineDefinitionNode.equals().
        for (int i = 0; i < pipelineDefinition.getRootNodes().size(); i++) {
            assertEquals(pipelineDefinition.getRootNodes().get(i).getModuleName(),
                pipelineDefinitionCopy.getRootNodes().get(i).getModuleName());
        }

        DatabaseTransactionFactory
            .performTransaction(() -> pipelineDefinitionCrud.merge(pipelineDefinitionCopy));
    }
}
