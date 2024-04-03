package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.TestModuleParameters;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Tests for {@link PipelineModuleDefinitionCrud} Tests that objects can be stored, retrieved, and
 * edited and that mapping metadata (associations, cascade rules, etc.) are setup correctly and work
 * as expected.
 *
 * @author Todd Klaus
 */
public class PipelineModuleDefinitionCrudTest {
    private static final String TEST_MODULE_NAME_1 = "Test Module 1";

    private static final String TEST_PARAM_SET_NAME_1 = "Test MPS-1";

    private static final String MISSING_MODULE = "I DONT EXIST";

    private ReflectionEquals comparer;

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private ParameterSetCrud parameterSetCrud;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        parameterSetCrud = new ParameterSetCrud();
        comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.id");
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.excludeField(".*\\.lastChangedUser.created");
        comparer.excludeField(".*\\.xmlParameters");
    }

    private PipelineModuleDefinition populateObjects() {
        return (PipelineModuleDefinition) DatabaseTransactionFactory.performTransaction(() -> {

            ParameterSet paramSet = createParameterSet(TEST_PARAM_SET_NAME_1);
            parameterSetCrud.persist(paramSet);

            PipelineModuleDefinition pmd = createPipelineModuleDefinition();
            return pipelineModuleDefinitionCrud.merge(pmd);
        });
    }

    private ParameterSet createParameterSet(String name) {
        ParameterSet parameterSet = new ParameterSet(name);
        parameterSet.populateFromParametersInstance(new TestModuleParameters(1));
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

    // For some reason, I'm not able to do the following two count queries as proper queries
    // using count(), but I'm able to retrieve all distinct names and return the size of the
    // resulting list.
    private int paramSetNameCount() {
        return parameterSetCrud.retrieveNames().size();
    }

    private int moduleNameCount() {
        return pipelineModuleDefinitionCrud.retrieveNames().size();
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

    /**
     * Stores a new PipelineModuleDefinition in the db, then retrieves it and makes sure it matches
     * what was put in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieve() throws Exception {
        PipelineModuleDefinition expectedModuleDef = populateObjects();

        // Retrieve
        PipelineModuleDefinition actualModuleDef = (PipelineModuleDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineModuleDefinition amd = pipelineModuleDefinitionCrud
                    .retrieveLatestVersionForName(TEST_MODULE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineModuleDefinition(amd);
                return amd;
            });

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
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineModuleDefinition modifiedPipelineModDef = pipelineModuleDefinitionCrud
                .retrieveLatestVersionForName(pmd.getName());
            editModuleDef(modifiedPipelineModDef);
            return null;
        });

        // Attempting to save the original version will fail because the save of a
        // modified version causes the original to be out of date.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineModuleDefinitionCrud.merge(pmd);
            return null;
        });
    }

    @Test
    public void testEditPipelineModuleDefinition() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineModuleDefinition pmd = pipelineModuleDefinitionCrud
                .retrieveLatestVersionForName(TEST_MODULE_NAME_1);

            editModuleDef(pmd);
            return null;
        });

        // Retrieve
        PipelineModuleDefinition actualModuleDef = (PipelineModuleDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineModuleDefinition amd = pipelineModuleDefinitionCrud
                    .retrieveLatestVersionForName(TEST_MODULE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineModuleDefinition(amd);
                return amd;
            });

        createParameterSet(TEST_PARAM_SET_NAME_1);
        PipelineModuleDefinition expectedModuleDef = createPipelineModuleDefinition();
        editModuleDef(expectedModuleDef);
        setOptimisticLockValue(expectedModuleDef, 1);

        comparer.assertEquals("PipelineModuleDefinition", expectedModuleDef, actualModuleDef);
        assertEquals(1, actualModuleDef.getOptimisticLockValue());

        assertEquals("PipelineModuleDefinition count", 1, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
        assertEquals("ParameterSetName count", 1, paramSetNameCount());
    }

    /**
     * simulate modifications made by a user
     *
     * @param moduleDef
     */
    private void editModuleDef(PipelineModuleDefinition moduleDef) {
        // moduleDef.setName(TEST_MODULE_NAME_2);
        moduleDef.setDescription("new description");
        moduleDef.updateAuditInfo();
    }

    @Test
    public void testEditPipelineModuleParameterSetChangeParam() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            List<ParameterSet> modifiedParamSets = parameterSetCrud
                .retrieveAllVersionsForName(TEST_PARAM_SET_NAME_1);

            assertEquals("paramSets size", 1, modifiedParamSets.size());

            ParameterSet modifiedParamSet = modifiedParamSets.get(0);

            editParamSetChangeParam(modifiedParamSet);
            return null;
        });

        // Retrieve
        ParameterSet actualParamSet = (ParameterSet) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<ParameterSet> actualParamSets = parameterSetCrud
                    .retrieveAllVersionsForName(TEST_PARAM_SET_NAME_1);
                assertEquals("paramSets size", 1, actualParamSets.size());
                return actualParamSets.get(0);
            });

        ParameterSet expectedParamSet = createParameterSet(TEST_PARAM_SET_NAME_1);
        editParamSetChangeParam(expectedParamSet);
        setOptimisticLockValue(expectedParamSet, 1);

        comparer.assertEquals("ParameterSet", expectedParamSet, actualParamSet);

        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
        assertEquals("ParameterSetName count", 1, paramSetNameCount());
    }

    /**
     * simulate modifications made by a user
     *
     * @param moduleDef
     * @return
     * @throws PipelineException
     */
    private void editParamSetChangeParam(ParameterSet paramSet) {
        TestModuleParameters moduleParams = paramSet.parametersInstance();
        moduleParams.setValue(100);
        paramSet.setTypedParameters(moduleParams.getParameters());
    }

    @Test
    public void testCreateOrUpdateDuplicateInstance() {
        PipelineModuleDefinition pmd = populateObjects();
        pmd.setDescription("New description");

        // Executing with the same object but changed content does the right thing
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineModuleDefinitionCrud.merge(pmd);
            return null;
        });

        @SuppressWarnings("unchecked")
        List<PipelineModuleDefinition> pipelineModuleDefinitions0 = (List<PipelineModuleDefinition>) DatabaseTransactionFactory
            .performTransaction(
                () -> pipelineModuleDefinitionCrud.retrieveAllVersionsForName(pmd.getName()));
        assertEquals(1, pipelineModuleDefinitions0.size());
        assertEquals("New description", pipelineModuleDefinitions0.get(0).getDescription());

        // Executing with a different object does the wrong thing
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineModuleDefinitionCrud.merge(copy(pipelineModuleDefinitions0.get(0)));
            return null;
        });

        @SuppressWarnings("unchecked")
        List<PipelineModuleDefinition> pipelineModuleDefinitions = (List<PipelineModuleDefinition>) DatabaseTransactionFactory
            .performTransaction(
                () -> pipelineModuleDefinitionCrud.retrieveAllVersionsForName(pmd.getName()));

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

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineModuleDefinition deletedModuleDef = pipelineModuleDefinitionCrud
                .retrieveLatestVersionForName(TEST_MODULE_NAME_1);
            pipelineModuleDefinitionCrud.remove(deletedModuleDef);

            ParameterSet deletedParamSet = parameterSetCrud
                .retrieveLatestVersionForName(TEST_PARAM_SET_NAME_1);
            parameterSetCrud.remove(deletedParamSet);
            return null;
        });

        assertEquals("ParameterSet count", 0, pipelineModuleParamSetCount());
        // verify CascadeType.DELETE_ORPHAN functionality
        assertEquals("ParameterSetName count", 0, paramSetNameCount());
    }

    // @Test(expected=ConstraintViolationException.class)
    public void testFailedDeletePipelineModuleParameterSet() throws Exception {
        // Create
        populateObjects();

        assertEquals("ParameterSetName count", 1, paramSetNameCount());

        DatabaseTransactionFactory.performTransaction(() -> {
            /*
             * Should fail with ConstraintViolationException because there is still a
             * PipelineModuleDefinition pointing at this ParameterSetName
             */
            ParameterSet deletedParamSet = parameterSetCrud
                .retrieveLatestVersionForName(TEST_PARAM_SET_NAME_1);
            parameterSetCrud.remove(deletedParamSet);
            return null;
        });
    }

    @Test
    public void testDeletePipelineModule() throws Exception {
        // Create
        populateObjects();

        assertEquals("ModuleName count", 1, moduleNameCount());

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineModuleDefinition deletedModuleDef = pipelineModuleDefinitionCrud
                .retrieveLatestVersionForName(TEST_MODULE_NAME_1);
            pipelineModuleDefinitionCrud.remove(deletedModuleDef);
            return null;
        });

        assertEquals("PipelineModuleDefinition count", 0, pipelineModuleDefinitionCount());
        // verify CascadeType.DELETE_ORPHAN functionality
        assertEquals("ModuleName count", 0, moduleNameCount());
    }
}
