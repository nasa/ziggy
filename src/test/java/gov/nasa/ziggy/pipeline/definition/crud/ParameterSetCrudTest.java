package gov.nasa.ziggy.pipeline.definition.crud;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParameterTestClasses;
import gov.nasa.ziggy.parameters.ParameterTestClasses.TestParametersFoo;
import gov.nasa.ziggy.pipeline.PipelineConfigurator;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

public class ParameterSetCrudTest {

    private static final String TEST_PARAMETERS_FOO = "testParametersFoo";

    private ParameterTestClasses.TestParametersFoo testParametersFoo;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Before
    public void setUp() {
        PipelineConfigurator pc = new PipelineConfigurator();
        testParametersFoo = new TestParametersFoo();
        pc.createParamSet(TEST_PARAMETERS_FOO, testParametersFoo);
    }

    @Test
    public void testCreateOrUpdateDuplicateInstance() throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {

        ParameterSet ps = (ParameterSet) DatabaseTransactionFactory.performTransaction(
            () -> new ParameterSetCrud().retrieveLatestVersionForName(TEST_PARAMETERS_FOO));

        ps.setDescription("New description");
        DatabaseTransactionFactory.performTransaction(() -> {
            new ParameterSetCrud().merge(ps);
            return null;
        });

        // When using an instance that was actually retrieved from the database, createOrUpdate
        // works correctly.
        @SuppressWarnings("unchecked")
        List<ParameterSet> parameterSets0 = (List<ParameterSet>) DatabaseTransactionFactory
            .performTransaction(
                () -> new ParameterSetCrud().retrieveAllVersionsForName(TEST_PARAMETERS_FOO));

        assertEquals(1, parameterSets0.size());
        assertEquals("New description", parameterSets0.get(0).getDescription());

        @SuppressWarnings("unchecked")
        List<ParameterSet> parameterSets = (List<ParameterSet>) DatabaseTransactionFactory
            .performTransaction(
                () -> new ParameterSetCrud().retrieveAllVersionsForName(TEST_PARAMETERS_FOO));

        assertEquals(1, parameterSets.size());
    }

    @Test(expected = PipelineException.class)
    public void testOptimisticLocking() {

        ParameterSet ps = (ParameterSet) DatabaseTransactionFactory.performTransaction(
            () -> new ParameterSetCrud().retrieveLatestVersionForName(TEST_PARAMETERS_FOO));

        // Retrieve and edit
        DatabaseTransactionFactory.performTransaction(() -> {
            ParameterSet psMod = new ParameterSetCrud().retrieveLatestVersionForName(ps.getName());
            psMod.setDescription("New description");
            return null;
        });

        // An attempt to save the original ps will fail because its optimistic locking
        // version is now out of date.
        DatabaseTransactionFactory.performTransaction(() -> {
            new ParameterSetCrud().merge(ps);
            return null;
        });
    }

    @Test
    public void testUpdateUnlockedParameterSet() {

        ParameterSetCrud parameterSetCrud = new ParameterSetCrud();
        ParameterSet paramSet = (ParameterSet) DatabaseTransactionFactory.performTransaction(() -> {
            ParameterSet ps = parameterSetCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO);
            assertFalse(ps.isLocked());
            assertEquals(0, ps.getVersion());
            return ps;
        });

        TestParametersFoo paramInstance = paramSet.parametersInstance();
        int foo1 = paramInstance.getFoo1();
        foo1++;
        paramInstance.setFoo1(foo1);
        paramSet.populateFromParametersInstance(paramInstance);

        DatabaseTransactionFactory.performTransaction(() -> {
            parameterSetCrud.merge(paramSet);
            return null;
        });

        ParameterSet psMerged = (ParameterSet) DatabaseTransactionFactory.performTransaction(
            () -> new ParameterSetCrud().retrieveLatestVersionForName(TEST_PARAMETERS_FOO));

        assertFalse(psMerged.isLocked());
        assertEquals(0, psMerged.getVersion());
        paramInstance = psMerged.parametersInstance();
        assertEquals(foo1, paramInstance.getFoo1());
    }

    @Test
    public void testUpdateLockedParameterSet() {

        ParameterSetCrud parameterSetCrud = new ParameterSetCrud();
        DatabaseTransactionFactory.performTransaction(() -> {
            ParameterSet ps = parameterSetCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO);
            ps.lock();
            return null;
        });

        ParameterSet paramSet = (ParameterSet) DatabaseTransactionFactory.performTransaction(() -> {
            ParameterSet ps = parameterSetCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO);
            assertTrue(ps.isLocked());
            assertEquals(0, ps.getVersion());
            return ps;
        });

        TestParametersFoo paramInstance = paramSet.parametersInstance();
        int foo1 = paramInstance.getFoo1();
        foo1++;
        paramInstance.setFoo1(foo1);
        paramSet.populateFromParametersInstance(paramInstance);

        DatabaseTransactionFactory.performTransaction(() -> {
            parameterSetCrud.merge(paramSet);
            return null;
        });

        ParameterSet psMerged = (ParameterSet) DatabaseTransactionFactory.performTransaction(
            () -> new ParameterSetCrud().retrieveLatestVersionForName(TEST_PARAMETERS_FOO));

        assertFalse(psMerged.isLocked());
        assertEquals(1, psMerged.getVersion());
        paramInstance = psMerged.parametersInstance();
        assertEquals(foo1, paramInstance.getFoo1());
    }

    @Test
    public void testLockedUnchangedParameterSet() {

        ParameterSetCrud parameterSetCrud = new ParameterSetCrud();
        DatabaseTransactionFactory.performTransaction(() -> {
            ParameterSet ps = parameterSetCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO);
            ps.lock();
            return null;
        });

        ParameterSet paramSet = (ParameterSet) DatabaseTransactionFactory.performTransaction(() -> {
            ParameterSet ps = parameterSetCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO);
            assertTrue(ps.isLocked());
            assertEquals(0, ps.getVersion());
            return ps;
        });

        ParameterSet mergedParamSet = (ParameterSet) DatabaseTransactionFactory
            .performTransaction(() -> parameterSetCrud.merge(paramSet));

        assertEquals(0, mergedParamSet.getVersion());
    }
}
