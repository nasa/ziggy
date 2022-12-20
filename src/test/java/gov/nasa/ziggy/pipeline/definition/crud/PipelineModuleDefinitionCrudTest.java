package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.TestModuleParameters;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.services.security.UserCrud;

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

    private UserCrud userCrud;

    private User adminUser;
    private User operatorUser;
    private ReflectionEquals comparer;

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private ParameterSetCrud parameterSetCrud;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        userCrud = new UserCrud();
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

            // create users
            adminUser = new User("admin", "Administrator", "admin@example.com", "x111");
            userCrud.createUser(adminUser);

            operatorUser = new User("ops", "Operator", "ops@example.com", "x112");
            userCrud.createUser(operatorUser);

            ParameterSet paramSet = createParameterSet(TEST_PARAM_SET_NAME_1);
            parameterSetCrud.create(paramSet);

            PipelineModuleDefinition pmd = createPipelineModuleDefinition();
            pipelineModuleDefinitionCrud.create(pmd);

            return pmd;
        });
    }

    private ParameterSet createParameterSet(String name) {
        ParameterSet parameterSet = new ParameterSet(new AuditInfo(adminUser, new Date()), name);
        parameterSet.setParameters(new BeanWrapper<Parameters>(new TestModuleParameters(1)));
        return parameterSet;
    }

    private PipelineModuleDefinition createPipelineModuleDefinition() {
        return new PipelineModuleDefinition(new AuditInfo(adminUser, new Date()),
            TEST_MODULE_NAME_1);
    }

    private int pipelineModuleDefinitionCount() {
        Query q = DatabaseService.getInstance()
            .getSession()
            .createQuery("select count(*) from PipelineModuleDefinition");
        return ((Long) q.uniqueResult()).intValue();
    }

    private int pipelineModuleParamSetCount() {
        Query q = DatabaseService.getInstance()
            .getSession()
            .createQuery("select count(*) from ParameterSet");
        return ((Long) q.uniqueResult()).intValue();
    }

    private int paramSetNameCount() {
        Query q = DatabaseService.getInstance()
            .getSession()
            .createQuery("select count(*) from ParameterSetName");
        return ((Long) q.uniqueResult()).intValue();
    }

    private int moduleNameCount() {
        Query q = DatabaseService.getInstance()
            .getSession()
            .createQuery("select count(*) from ModuleName");
        return ((Long) q.uniqueResult()).intValue();
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
        expectedModuleDef.setDirty(1);

        comparer.assertEquals("PipelineModuleDefinition", expectedModuleDef, actualModuleDef);

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
        moduleDef.getAuditInfo().setLastChangedTime(new Date());
        moduleDef.getAuditInfo().setLastChangedUser(operatorUser);
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
                ParameterSet parameterSet = actualParamSets.get(0);
                ZiggyUnitTestUtils.initializeUser(parameterSet.getAuditInfo().getLastChangedUser());
                return actualParamSets.get(0);
            });

        ParameterSet expectedParamSet = createParameterSet(TEST_PARAM_SET_NAME_1);
        editParamSetChangeParam(expectedParamSet);
        expectedParamSet.setDirty(1);

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
        paramSet.getParameters().populateFromInstance(moduleParams);
    }

    @Test
    public void testDeletePipelineModuleParameterSet() throws Exception {
        // Create
        populateObjects();

        assertEquals("ParameterSetName count", 1, paramSetNameCount());

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineModuleDefinition deletedModuleDef = pipelineModuleDefinitionCrud
                .retrieveLatestVersionForName(TEST_MODULE_NAME_1);
            pipelineModuleDefinitionCrud.delete(deletedModuleDef);

            ParameterSet deletedParamSet = parameterSetCrud
                .retrieveLatestVersionForName(TEST_PARAM_SET_NAME_1);
            parameterSetCrud.delete(deletedParamSet);
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
            parameterSetCrud.delete(deletedParamSet);
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
            pipelineModuleDefinitionCrud.delete(deletedModuleDef);
            return null;
        });

        assertEquals("PipelineModuleDefinition count", 0, pipelineModuleDefinitionCount());
        // verify CascadeType.DELETE_ORPHAN functionality
        assertEquals("ModuleName count", 0, moduleNameCount());
    }
}
