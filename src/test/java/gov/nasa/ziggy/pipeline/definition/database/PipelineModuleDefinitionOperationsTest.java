package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.data.management.DataReceiptPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleExecutionResources;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.DataReceiptUnitOfWorkGenerator;

/**
 * Unit tests for {@link PipelineModuleDefinitionOperations} and
 * {@link PipelineModuleDefinitionCrud}.
 */
public class PipelineModuleDefinitionOperationsTest {

    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations;
    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private TestOperations testOperations = new TestOperations();
    private ParameterSetCrud parameterSetCrud;
    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private static final String TEST_MODULE_NAME_1 = "Test Module 1";
    private static final String TEST_PARAM_SET_NAME_1 = "Test MPS-1";
    private static final String MISSING_MODULE = "I DONT EXIST";
    private ReflectionEquals comparer;
    private ParametersOperations parametersOperations = new ParametersOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();
        parameterSetCrud = new ParameterSetCrud();
        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        parameterSetCrud = new ParameterSetCrud();
        comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.id");
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.excludeField(".*\\.lastChangedUser.created");
        comparer.excludeField(".*\\.xmlParameters");
        comparer.excludeField(".*\\.pipelineInstanceNodes");
        comparer.excludeField(".*\\.pipelineDefinitionNodes");
    }

    @Test
    public void testLatestModuleVersionForName() {
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        PipelineModuleDefinition pipelineModuleDefinition = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition("module1");
        assertEquals("description", pipelineModuleDefinition.getDescription());

        // Make a change, persist, and see if the retrieved version is correct.
        pipelineModuleDefinition.setDescription("Updated version");
        testOperations.merge(pipelineModuleDefinition);
        PipelineModuleDefinition pipelineModuleDefinition2 = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition("module1");
        assertEquals("Updated version", pipelineModuleDefinition2.getDescription());
        assertFalse(pipelineModuleDefinition2.isLocked());
        assertEquals(0, pipelineModuleDefinition2.getVersion());

        // Lock the current version.
        pipelineModuleDefinition2.lock();
        PipelineModuleDefinition pipelineModuleDefinition3 = testOperations
            .merge(pipelineModuleDefinition2);

        // Make a change, persist, and see if the retrieved version is correct (i.e., is unlocked
        // and has its version number incremented).
        pipelineModuleDefinition3.setDescription("Even more updated version");
        testOperations.merge(pipelineModuleDefinition3);
        PipelineModuleDefinition pipelineModuleDefinition4 = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition("module1");
        assertEquals("Even more updated version", pipelineModuleDefinition4.getDescription());
        assertFalse(pipelineModuleDefinition4.isLocked());
        assertEquals(1, pipelineModuleDefinition4.getVersion());
    }

    @Test
    public void testCreateDataReceiptPipelineModule() {
        pipelineModuleDefinitionOperations.createDataReceiptPipelineModule();
        PipelineModuleDefinition dataReceiptModule = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition(DataReceiptPipelineModule.DATA_RECEIPT_MODULE_NAME);
        assertNotNull(dataReceiptModule);
        assertEquals(DataReceiptUnitOfWorkGenerator.class,
            dataReceiptModule.getUnitOfWorkGenerator().getClazz());
    }

    @Test
    public void testPipelineModuleExecutionResources() {
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        PipelineModuleExecutionResources resources = new PipelineModuleExecutionResources();
        resources.setPipelineModuleName("module1");
        resources.setExeTimeoutSeconds(100);
        resources.setMinMemoryMegabytes(10);
        testOperations.persist(resources);
        PipelineModuleExecutionResources databaseResources = pipelineModuleDefinitionOperations
            .pipelineModuleExecutionResources(
                pipelineOperationsTestUtils.pipelineModuleDefinition());
        assertEquals(100, databaseResources.getExeTimeoutSeconds());
        assertEquals(10, databaseResources.getMinMemoryMegabytes());
        assertEquals("module1", databaseResources.getPipelineModuleName());
    }

    private PipelineModuleDefinition populateObjects() {
        ParameterSet paramSet = createParameterSet(TEST_PARAM_SET_NAME_1);
        testOperations.mergeParameterSet(paramSet);
        PipelineModuleDefinition pmd = createPipelineModuleDefinition();
        return testOperations.mergeModuleDefinition(pmd);
    }

    private ParameterSet createParameterSet(String name) {
        ParameterSet parameterSet = new ParameterSet(name);
        Parameter parameter = new Parameter("value", "42", ZiggyDataType.ZIGGY_INT);
        parameterSet.getParameters().add(parameter);
        return parameterSet;
    }

    private PipelineModuleDefinition createPipelineModuleDefinition() {
        return new PipelineModuleDefinition(TEST_MODULE_NAME_1);
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

    PipelineModuleDefinition copy(PipelineModuleDefinition original) throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {
        PipelineModuleDefinition copy = new PipelineModuleDefinition(original.getName());
        copy.setDescription(original.getDescription());
        copy.setPipelineModuleClass(original.getPipelineModuleClass());
        copy.setExeTimeoutSecs(original.getExeTimeoutSecs());
        Field versionField = original.getClass().getSuperclass().getDeclaredField("version");
        versionField.setAccessible(true);
        versionField.set(copy, original.getVersion());
        setOptimisticLockValue(copy, original.getOptimisticLockValue());
        Field idField = original.getClass().getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(copy, original.getId());
        return copy;
    }

    private void setOptimisticLockValue(PipelineModuleDefinition pipelineModuleDefinition,
        int dirty) throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException {
        Field dirtyField = pipelineModuleDefinition.getClass()
            .getSuperclass()
            .getDeclaredField("optimisticLockValue");
        dirtyField.setAccessible(true);
        dirtyField.set(pipelineModuleDefinition, dirty);
    }

    private void setOptimisticLockValue(ParameterSet parameterSet, int dirty)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException {
        Field dirtyField = parameterSet.getClass()
            .getSuperclass()
            .getDeclaredField("optimisticLockValue");
        dirtyField.setAccessible(true);
        dirtyField.set(parameterSet, dirty);
    }

    private PipelineModuleDefinition editModuleDef(PipelineModuleDefinition moduleDef) {
        return editModuleDef(moduleDef, true);
    }

    /**
     * simulate modifications made by a user
     *
     * @param moduleDef
     */
    private PipelineModuleDefinition editModuleDef(PipelineModuleDefinition moduleDef,
        boolean merge) {
        if (merge) {
            return testOperations.updatePipelineModuleDefinition(moduleDef.getName());
        }
        moduleDef.setDescription("new description");
        moduleDef.updateAuditInfo();
        return moduleDef;
    }

    @Test
    public void testStoreAndRetrieve() throws Exception {
        PipelineModuleDefinition expectedModuleDef = populateObjects();

        // Retrieve
        PipelineModuleDefinition actualModuleDef = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition(TEST_MODULE_NAME_1);

        comparer.assertEquals("PipelineModuleDefinition", expectedModuleDef, actualModuleDef);

        assertEquals("PipelineModuleDefinition count", 1, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
        assertEquals("ParameterSetName count", 1, paramSetNameCount());
    }

    @Test
    public void testRetrieveMissing() {
        PipelineModuleDefinition moduleDef = pipelineModuleDefinitionCrud
            .retrieveLatestVersionForName(MISSING_MODULE);

        assertNull("missing module", moduleDef);
    }

    @Test(expected = PipelineException.class)
    public void testOptimisticLocking() {
        // Create
        PipelineModuleDefinition pmd = populateObjects();

        // Retrieve & Edit
        PipelineModuleDefinition modifiedPipelineModDef = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition(pmd.getName());
        modifiedPipelineModDef = editModuleDef(modifiedPipelineModDef);

        // Attempting to save the original version will fail because the save of a
        // modified version causes the original to be out of date.
        testOperations.mergeModuleDefinition(pmd);
    }

    @Test
    public void testEditPipelineModuleDefinition() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        PipelineModuleDefinition pmd = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition(TEST_MODULE_NAME_1);
        editModuleDef(pmd);

        // Retrieve
        PipelineModuleDefinition actualModuleDef = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition(TEST_MODULE_NAME_1);

        createParameterSet(TEST_PARAM_SET_NAME_1);
        PipelineModuleDefinition expectedModuleDef = createPipelineModuleDefinition();
        expectedModuleDef = editModuleDef(expectedModuleDef, false);
        setOptimisticLockValue(expectedModuleDef, 1);

        comparer.assertEquals("PipelineModuleDefinition", expectedModuleDef, actualModuleDef);
        assertEquals(1, actualModuleDef.getOptimisticLockValue());

        assertEquals("PipelineModuleDefinition count", 1, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
        assertEquals("ParameterSetName count", 1, paramSetNameCount());
    }

    @Test
    public void testEditPipelineModuleParameterSetChangeParam() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        List<ParameterSet> modifiedParamSets = testOperations
            .allParameterSetVersions(TEST_PARAM_SET_NAME_1);
        assertEquals("paramSets size", 1, modifiedParamSets.size());
        ParameterSet modifiedParamSet = modifiedParamSets.get(0);
        editParamSetChangeParam(modifiedParamSet);

        // Retrieve
        List<ParameterSet> actualParamSets = testOperations
            .allParameterSetVersions(TEST_PARAM_SET_NAME_1);
        assertEquals("paramSets size", 1, actualParamSets.size());
        ParameterSet actualParamSet = actualParamSets.get(0);

        ParameterSet expectedParamSet = createParameterSet(TEST_PARAM_SET_NAME_1);
        editParamSetChangeParam(expectedParamSet, false);
        setOptimisticLockValue(expectedParamSet, 1);

        comparer.assertEquals("ParameterSet", expectedParamSet, actualParamSet);

        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
        assertEquals("ParameterSetName count", 1, paramSetNameCount());
    }

    private ParameterSet editParamSetChangeParam(ParameterSet paramSet) {
        return editParamSetChangeParam(paramSet, true);
    }

    /**
     * simulate modifications made by a user
     *
     * @param moduleDef
     * @return
     * @throws PipelineException
     */
    private ParameterSet editParamSetChangeParam(ParameterSet paramSet, boolean merge) {
        if (merge) {
            return testOperations.updateTestModuleParameters(paramSet.getName());
        }
        Parameter parameter = new Parameter("value", "100", ZiggyDataType.ZIGGY_INT);
        paramSet.getParameters().clear();
        paramSet.getParameters().add(parameter);
        return paramSet;
    }

    @Test
    public void testCreateOrUpdateDuplicateInstance() throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {
        PipelineModuleDefinition pmd = populateObjects();
        pmd.setDescription("New description");
        testOperations.mergeModuleDefinition(pmd);

        List<PipelineModuleDefinition> pipelineModuleDefinitions0 = testOperations
            .allPipelineModuleDefinitions(pmd.getName());
        assertEquals(1, pipelineModuleDefinitions0.size());
        assertEquals("New description", pipelineModuleDefinitions0.get(0).getDescription());

        // Executing with a different object does the wrong thing
        testOperations.mergeModuleDefinition(copy(pipelineModuleDefinitions0.get(0)));

        List<PipelineModuleDefinition> pipelineModuleDefinitions = testOperations
            .allPipelineModuleDefinitions(pmd.getName());
        if (pipelineModuleDefinitions.size() == 2) {
            assertEquals(pipelineModuleDefinitions.get(0).getName(),
                pipelineModuleDefinitions.get(1).getName());
            assertEquals(pipelineModuleDefinitions.get(0).getVersion(),
                pipelineModuleDefinitions.get(1).getVersion());
            assertFalse(pipelineModuleDefinitions.get(0).isLocked());
            assertFalse(pipelineModuleDefinitions.get(1).isLocked());
        }

        // Here's where we fail, because this should not have created a duplicate object
        assertEquals(1, pipelineModuleDefinitions.size());
    }

    @Test
    public void testDeletePipelineModuleParameterSet() throws Exception {
        // Create
        populateObjects();

        assertEquals("ParameterSetName count", 1, paramSetNameCount());

        PipelineModuleDefinition deletedModuleDef = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition(TEST_MODULE_NAME_1);
        testOperations.removeModuleDefinition(deletedModuleDef);

        ParameterSet deletedParamSet = parametersOperations.parameterSet(TEST_PARAM_SET_NAME_1);
        testOperations.removeParameterSet(deletedParamSet);

        assertEquals("ParameterSet count", 0, pipelineModuleParamSetCount());
        // verify CascadeType.DELETE_ORPHAN functionality
        assertEquals("ParameterSetName count", 0, paramSetNameCount());
    }

    @Test
    public void testDeletePipelineModule() throws Exception {
        // Create
        populateObjects();

        assertEquals("ModuleName count", 1, moduleNameCount());
        PipelineModuleDefinition deletedModuleDef = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition(TEST_MODULE_NAME_1);
        testOperations.removeModuleDefinition(deletedModuleDef);

        assertEquals("PipelineModuleDefinition count", 0, pipelineModuleDefinitionCount());
        // verify CascadeType.DELETE_ORPHAN functionality
        assertEquals("ModuleName count", 0, moduleNameCount());
    }

    // For some reason, I'm not able to do the following two count queries as proper queries
    // using count(), but I'm able to retrieve all distinct names and return the size of the
    // resulting list.
    private int paramSetNameCount() {
        return testOperations.parameterSetNameCount();
    }

    private int moduleNameCount() {
        return testOperations.moduleDefinitionNameCount();
    }

    @Test
    public void testLock() {
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        PipelineModuleDefinition pipelineModuleDefinition = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition("module1");
        assertFalse(pipelineModuleDefinition.isLocked());
        pipelineModuleDefinitionOperations.lock(pipelineModuleDefinition);
        pipelineModuleDefinition = pipelineModuleDefinitionOperations
            .pipelineModuleDefinition("module1");
        assertTrue(pipelineModuleDefinition.isLocked());
    }

    private class TestOperations extends DatabaseOperations {

        public PipelineModuleDefinition merge(PipelineModuleDefinition pipelineModuleDefinition) {
            return performTransaction(
                () -> new PipelineModuleDefinitionCrud().merge(pipelineModuleDefinition));
        }

        public void persist(PipelineModuleExecutionResources resources) {
            performTransaction(() -> new PipelineModuleDefinitionCrud().persist(resources));
        }

        public List<ParameterSet> allParameterSetVersions(String name) {
            return performTransaction(
                () -> new ParameterSetCrud().retrieveAllVersionsForName(name));
        }

        public void removeModuleDefinition(PipelineModuleDefinition moduleDefinition) {
            performTransaction(() -> new PipelineModuleDefinitionCrud().remove(moduleDefinition));
        }

        public void removeParameterSet(ParameterSet parameterSet) {
            performTransaction(() -> new ParameterSetCrud().remove(parameterSet));
        }

        public List<PipelineModuleDefinition> allPipelineModuleDefinitions(String name) {
            return performTransaction(
                () -> new PipelineModuleDefinitionCrud().retrieveAllVersionsForName(name));
        }

        public PipelineModuleDefinition mergeModuleDefinition(
            PipelineModuleDefinition moduleDefinition) {
            return performTransaction(
                () -> new PipelineModuleDefinitionCrud().merge(moduleDefinition));
        }

        // Apparently this is the only way to get the Hibernate version to update:
        // the instance getting the update cannot become detached between its
        // retrieval and its persistence.
        public ParameterSet updateTestModuleParameters(String parameterSetName) {
            return performTransaction(() -> {
                ParameterSet databaseParameterSet = new ParameterSetCrud()
                    .retrieveLatestVersionForName(parameterSetName);
                Parameter parameter = new Parameter("value", "100", ZiggyDataType.ZIGGY_INT);
                databaseParameterSet.getParameters().clear();
                databaseParameterSet.getParameters().add(parameter);
                return new ParameterSetCrud().merge(databaseParameterSet);
            });
        }

        public PipelineModuleDefinition updatePipelineModuleDefinition(String moduleName) {
            return performTransaction(() -> {
                PipelineModuleDefinition moduleDefinition = new PipelineModuleDefinitionCrud()
                    .retrieveLatestVersionForName(moduleName);
                moduleDefinition.setDescription("new description");
                moduleDefinition.updateAuditInfo();
                return new PipelineModuleDefinitionCrud().merge(moduleDefinition);
            });
        }

        public int parameterSetNameCount() {
            return performTransaction(() -> new ParameterSetCrud().retrieveNames().size());
        }

        public int moduleDefinitionNameCount() {
            return performTransaction(
                () -> new PipelineModuleDefinitionCrud().retrieveNames().size());
        }

        public ParameterSet mergeParameterSet(ParameterSet parameterSet) {
            return performTransaction(() -> new ParameterSetCrud().merge(parameterSet));
        }
    }
}
