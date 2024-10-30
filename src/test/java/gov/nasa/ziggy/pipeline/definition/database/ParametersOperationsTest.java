package gov.nasa.ziggy.pipeline.definition.database;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import gov.nasa.ziggy.IntegrationTestCategory;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Test the {@link ParametersOperations} class.
 *
 * @author Todd Klaus
 */
@Category(IntegrationTestCategory.class)
public class ParametersOperationsTest {
    public static final String TEST_PARAMETERS_FOO = "testParametersFoo";
    public static final String TEST_PARAMETERS_BAR = "testParametersBar";

    private ParameterSet parameterSetFoo;
    private ParameterSet parameterSetBar;

    private TestOperations testOperations = new TestOperations();
    private ParametersOperations parametersOperations;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Before
    public void setUp() {
        parametersOperations = new ParametersOperations();
    }

    public void setUpParameterSets() {

        // Put two parameter sets into the database.
        setParameterSetFoo(testOperations.merge(parameterSet(TEST_PARAMETERS_FOO,
            Set.of(new Parameter("p1", "77", ZiggyDataType.ZIGGY_INT),
                new Parameter("p2", "Bauhaus", ZiggyDataType.ZIGGY_STRING)))));
        setParameterSetBar(testOperations.merge(parameterSet(TEST_PARAMETERS_BAR,
            Set.of(new Parameter("p3", "105.3", ZiggyDataType.ZIGGY_FLOAT),
                new Parameter("p4", "true", ZiggyDataType.ZIGGY_BOOLEAN)))));
    }

    /**
     * Constructs an instance of {@link ParameterSet}.
     */
    private static ParameterSet parameterSet(String name, Set<Parameter> parameters) {
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setName(name);
        if (!CollectionUtils.isEmpty(parameters)) {
            parameterSet.getParameters().addAll(parameters);
        }
        return parameterSet;
    }

    @Test
    public void testParameterSetRetrieval() {
        setUpParameterSets();
        ParameterSet databaseParameterSet = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);
        assertTrue(getParameterSetFoo().totalEquals(databaseParameterSet));
        assertEquals(0, databaseParameterSet.getVersion());
        assertFalse(databaseParameterSet.isLocked());
    }

    @Test
    public void testBindParameterSets() {
        setUpParameterSets();
        Set<String> parameterSetNames = Set.of(TEST_PARAMETERS_FOO, TEST_PARAMETERS_BAR);
        Set<ParameterSet> parameterSets = new HashSet<>();
        parametersOperations.bindParameterSets(parameterSetNames, parameterSets);
        Map<String, ParameterSet> parameterSetsByName = ParameterSet
            .parameterSetByName(parameterSets);
        assertTrue(parameterSetsByName.containsKey(TEST_PARAMETERS_FOO));
        assertTrue(parameterSetsByName.get(TEST_PARAMETERS_FOO).totalEquals(getParameterSetFoo()));
        assertTrue(parameterSetsByName.containsKey(TEST_PARAMETERS_BAR));
        assertTrue(parameterSetsByName.get(TEST_PARAMETERS_BAR).totalEquals(getParameterSetBar()));
        assertEquals(2, parameterSets.size());
    }

    @Test
    public void testCompareParameterSets() {
        setUpParameterSets();
        assertFalse(getParameterSetFoo().totalEquals(getParameterSetBar()));
    }

    @Test
    public void testUpdateParameterSet() {
        setUpParameterSets();
        getParameterSetFoo().getParameters()
            .add(new Parameter("p5", "10, 20", ZiggyDataType.ZIGGY_INT, false));
        testOperations.merge(getParameterSetFoo());
        ParameterSet databaseParameterSet = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);
        Map<String, Parameter> parametersByName = new HashMap<>();
        for (Parameter parameter : databaseParameterSet.getParameters()) {
            parametersByName.put(parameter.getName(), parameter);
        }
        Parameter parameter = parametersByName.get("p1");
        assertNotNull(parameter);
        assertEquals("77", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_INT, parameter.getDataType());
        assertTrue(parameter.isScalar());

        parameter = parametersByName.get("p2");
        assertNotNull(parameter);
        assertEquals("Bauhaus", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_STRING, parameter.getDataType());
        assertTrue(parameter.isScalar());

        parameter = parametersByName.get("p5");
        assertNotNull(parameter);
        assertEquals("10,20", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_INT, parameter.getDataType());
        assertFalse(parameter.isScalar());
    }

    @Test
    public void testRetrieveLatestParameterSet() {
        setUpParameterSets();
        parametersOperations.lock(TEST_PARAMETERS_FOO);
        ParameterSet databaseParameterSet = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);
        assertTrue(databaseParameterSet.isLocked());
        assertEquals(0, databaseParameterSet.getVersion());
        databaseParameterSet.getParameters()
            .add(new Parameter("p5", "10, 20", ZiggyDataType.ZIGGY_INT, false));
        testOperations.merge(databaseParameterSet);
        ParameterSet newVersionParameterSet = parametersOperations
            .parameterSet(TEST_PARAMETERS_FOO);
        assertFalse(newVersionParameterSet.isLocked());
        assertEquals(1, newVersionParameterSet.getVersion());
        assertEquals(3, newVersionParameterSet.getParameters().size());
    }

    // And now for a bunch of tests that exercise all the error cases

    @Test
    public void testCreateOrUpdateDuplicateInstance() throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {
        setUpParameterSets();
        ParameterSet ps = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);

        ps.setDescription("New description");
        testOperations.merge(ps);

        // When using an instance that was actually retrieved from the database, createOrUpdate
        // works correctly.
        List<ParameterSet> parameterSets0 = testOperations
            .allParameterSetVersions(TEST_PARAMETERS_FOO);

        assertEquals(1, parameterSets0.size());
        assertEquals("New description", parameterSets0.get(0).getDescription());

        List<ParameterSet> parameterSets = testOperations
            .allParameterSetVersions(TEST_PARAMETERS_FOO);

        assertEquals(1, parameterSets.size());
    }

    @Test(expected = PipelineException.class)
    public void testOptimisticLocking() {
        setUpParameterSets();
        ParameterSet ps = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);

        // Retrieve and edit
        ParameterSet psMod = parametersOperations.parameterSet(ps.getName());
        psMod.setDescription("New description");
        testOperations.merge(psMod);

        // An attempt to save the original ps will fail because its optimistic locking
        // version is now out of date.
        testOperations.merge(ps);
    }

    @SuppressWarnings("unused")
    @Test
    public void testUpdateUnlockedParameterSet() {
        setUpParameterSets();
        ParameterSet paramSet = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);
        assertFalse(paramSet.isLocked());
        assertEquals(0, paramSet.getVersion());

        paramSet.parameterByName().get("p1").setValue(100);

        // During ZIGGY-452, a bizarre Hibernate behavior was observed, to wit: in
        // order for the merge below to work, two things had to happen: first, the
        // Set<Parameter> values (detached) had to be replaced with (transient)
        // (see ParameterSetCrud.merge()). Second, the merged parameter set had to
        // be returned from the merge() method. If you take out either of these things,
        // the test of parameter p1 value, below, will fail -- the value will be 77,
        // which is the pre-merge value!
        ParameterSet z = testOperations.merge(paramSet);

        ParameterSet psMerged = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);

        assertFalse(psMerged.isLocked());
        assertEquals(0, psMerged.getVersion());
        assertEquals("100", psMerged.parameterByName().get("p1").getString());
    }

    @Test
    public void testUpdateLockedParameterSet() {
        setUpParameterSets();
        parametersOperations.lock(TEST_PARAMETERS_FOO);

        ParameterSet paramSet = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);
        assertTrue(paramSet.isLocked());
        assertEquals(0, paramSet.getVersion());

        paramSet.parameterByName().get("p1").setValue(100);
        testOperations.merge(paramSet);

        ParameterSet psMerged = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);
        assertFalse(psMerged.isLocked());
        assertEquals(1, psMerged.getVersion());
        assertEquals("100", psMerged.parameterByName().get("p1").getString());
    }

    @Test
    public void testLockedUnchangedParameterSet() {
        setUpParameterSets();
        ParameterSet ps = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);
        ps.lock();
        testOperations.merge(ps);

        ParameterSet paramSet = parametersOperations.parameterSet(TEST_PARAMETERS_FOO);
        assertTrue(ps.isLocked());
        assertEquals(0, ps.getVersion());

        ParameterSet mergedParamSet = testOperations.merge(paramSet);

        assertEquals(0, mergedParamSet.getVersion());
    }

    public ParameterSet getParameterSetFoo() {
        return parameterSetFoo;
    }

    public void setParameterSetFoo(ParameterSet parameterSetFoo) {
        this.parameterSetFoo = parameterSetFoo;
    }

    public ParameterSet getParameterSetBar() {
        return parameterSetBar;
    }

    public void setParameterSetBar(ParameterSet parameterSetBar) {
        this.parameterSetBar = parameterSetBar;
    }

    private static class TestOperations extends DatabaseOperations {

        public ParameterSet merge(ParameterSet parameterSet) {
            return performTransaction(() -> new ParameterSetCrud().merge(parameterSet));
        }

        public List<ParameterSet> allParameterSetVersions(String name) {
            return performTransaction(
                () -> new ParameterSetCrud().retrieveAllVersionsForName(name));
        }
    }
}
