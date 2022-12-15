package gov.nasa.ziggy.parameters;

import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_HOME_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.parameters.ParameterTestClasses.TestParameters;
import gov.nasa.ziggy.parameters.ParameterTestClasses.TestParametersBar;
import gov.nasa.ziggy.parameters.ParameterTestClasses.TestParametersBaz;
import gov.nasa.ziggy.parameters.ParameterTestClasses.TestParametersFoo;
import gov.nasa.ziggy.pipeline.PipelineConfigurator;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.util.io.Filenames;
import jakarta.xml.bind.UnmarshalException;

/**
 * @author Todd Klaus
 */
public class ParametersOperationsTest {
    private static final String TEST_PARAMETERS = "testParameters";
    private static final String TEST_PARAMETERS_FOO = "testParametersFoo";
    private static final String TEST_PARAMETERS_BAR = "testParametersBar";
    private static final String TEST_PARAMETERS_BAZ = "testParametersBaz";

    private File libraryFile;

    private ParameterTestClasses.TestParameters testParameters;
    private ParameterTestClasses.TestParametersFoo testParametersFoo;
    private ParameterTestClasses.TestParametersBar testParametersBar;
    private ParameterTestClasses.TestParametersBaz testParametersBaz;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_HOME_DIR_PROP_NAME, Paths.get(System.getProperty("user.dir"), "build").toString());

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        libraryFile = directoryRule.directory()
            .resolve("param-lib")
            .resolve("param-lib.xml")
            .toFile();
    }

    private void createLibrary() {
        PipelineConfigurator pc = new PipelineConfigurator();

        testParameters = new TestParameters();
        testParametersFoo = new TestParametersFoo();
        testParametersBar = new TestParametersBar();
        testParametersBaz = new TestParametersBaz();

        pc.createParamSet(TEST_PARAMETERS_FOO, testParametersFoo);
        pc.createParamSet(TEST_PARAMETERS_BAR, testParametersBar);
        pc.createParamSet(TEST_PARAMETERS_BAZ, testParametersBaz);
    }

    private void modifyLibrary() {
        // create a new param set to trigger LIBRARY_ONLY
        PipelineConfigurator pc = new PipelineConfigurator();
        pc.createParamSet(TEST_PARAMETERS, testParameters);

        // delete a param set to trigger CREATE
        ParameterSetCrud paramCrud = new ParameterSetCrud();
        ParameterSet fooPs = paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO);
        paramCrud.delete(fooPs);

        // modify a param set to trigger UPDATE
        ParameterSet barPs = paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_BAR);
        TestParametersBar bar = barPs.parametersInstance();
        bar.setBar1(1.1F);
        PipelineOperations pipelineOps = new PipelineOperations();
        pipelineOps.updateParameterSet(barPs, bar, false);
    }

    private void exportLibrary(List<String> excludeList) throws IOException {
        if (libraryFile.getParentFile().exists()) {
            FileUtils.cleanDirectory(libraryFile.getParentFile());
        } else {
            Files.createDirectories(libraryFile.getParentFile().toPath());
        }
        ParametersOperations paramOps = new ParametersOperations();
        paramOps.exportParameterLibrary(libraryFile.getAbsolutePath(), excludeList,
            ParamIoMode.STANDARD);
    }

    @Test
    public void testRoundTrip() throws Exception {
        ParametersOperations paramOps = new ParametersOperations();

        DatabaseTransactionFactory.performTransaction(() -> {
            // create a param library
            createLibrary();
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {
            // export the library
            exportLibrary(null);

            // make some changes to the library
            modifyLibrary();
            return null;
        });

        // Read the library back in and persist it
        @SuppressWarnings("unchecked")
        List<ParameterSetDescriptor> actualResults = (List<ParameterSetDescriptor>) DatabaseTransactionFactory
            .performTransaction(() -> paramOps.importParameterLibrary(libraryFile.getAbsolutePath(),
                null, ParamIoMode.STANDARD));

        // verify results
        ReflectionEquals comparator = new ReflectionEquals();

        // Check the existence and state of each parameter set -- the TEST_PARAMETERS set should be
        // LIBRARY_ONLY (created in the modifyLibrary() call), TEST_PARAMETERS_FOO should be CREATE
        // (it got deleted in the modifyLibrary() call), TEST_PARAMETERS_BAR should be UPDATE
        // (its modified value got overwritten in the import), TEST_PARAMETERS_BAZ should be SAME
        // (untouched by the modifyLibrary() operation).
        List<ParameterSetDescriptor> expectedResults = new LinkedList<>();
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_BAR,
            TestParametersBar.class.getName(), ParameterSetDescriptor.State.UPDATE));
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_BAZ,
            TestParametersBaz.class.getName(), ParameterSetDescriptor.State.SAME));
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_FOO,
            TestParametersFoo.class.getName(), ParameterSetDescriptor.State.CREATE));
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS,
            TestParameters.class.getName(), ParameterSetDescriptor.State.LIBRARY_ONLY));

        comparator.excludeField(".*\\.libraryProps");
        comparator.excludeField(".*\\.fileProps");
        comparator.excludeField(".*\\.libraryParamSet");
        comparator.excludeField(".*\\.importedParamsBean");
        comparator.assertEquals("results", expectedResults, actualResults);

        ParameterSetCrud paramCrud = new ParameterSetCrud();

        // Compare the database values for parameters with the ORIGINAL values for foo, bar, and
        // baz, and the values for TEST_PARAMETERS created in the modifyLibrary() operation. This
        // confirms that value updates got overwritten when the library was re-imported.
        comparator.assertEquals("TEST_PARAMETERS_BAR", new BeanWrapper<>(testParametersBar),
            paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_BAR).getParameters());
        comparator.assertEquals("TEST_PARAMETERS_BAZ", new BeanWrapper<>(testParametersBaz),
            paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_BAZ).getParameters());
        comparator.assertEquals("TEST_PARAMETERS_FOO", new BeanWrapper<>(testParametersFoo),
            paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO).getParameters());
        comparator.assertEquals("TEST_PARAMETERS", new BeanWrapper<>(testParameters),
            paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS).getParameters());
    }

    @Test
    public void testRoundTripWithDryRun() throws Exception {
        ParametersOperations paramOps = new ParametersOperations();
        DatabaseTransactionFactory.performTransaction(() -> {
            // create a param library
            createLibrary();
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {

            // export the library
            exportLibrary(null);

            // make some changes to the library
            modifyLibrary();
            return null;
        });

        // import the library
        @SuppressWarnings("unchecked")
        List<ParameterSetDescriptor> actualResults = (List<ParameterSetDescriptor>) DatabaseTransactionFactory
            .performTransaction(() -> paramOps.importParameterLibrary(libraryFile.getAbsolutePath(),
                null, ParamIoMode.DRYRUN));

        // verify results
        ReflectionEquals comparator = new ReflectionEquals();

        List<ParameterSetDescriptor> expectedResults = new LinkedList<>();
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_BAR,
            TestParametersBar.class.getName(), ParameterSetDescriptor.State.UPDATE));
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_BAZ,
            TestParametersBaz.class.getName(), ParameterSetDescriptor.State.SAME));
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_FOO,
            TestParametersFoo.class.getName(), ParameterSetDescriptor.State.CREATE));
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS,
            TestParameters.class.getName(), ParameterSetDescriptor.State.LIBRARY_ONLY));

        comparator.excludeField(".*\\.libraryProps");
        comparator.excludeField(".*\\.fileProps");
        comparator.excludeField(".*\\.libraryParamSet");
        comparator.excludeField(".*\\.importedParamsBean");
        comparator.assertEquals("results", expectedResults, actualResults);

        ParameterSetCrud paramCrud = new ParameterSetCrud();

        TestParametersBar modifiedBar = new TestParametersBar();
        modifiedBar.setBar1(1.1F);
        comparator.assertEquals("TEST_PARAMETERS_BAR", new BeanWrapper<>(modifiedBar),
            paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_BAR).getParameters());
        comparator.assertEquals("TEST_PARAMETERS_BAZ", new BeanWrapper<>(testParametersBaz),
            paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_BAZ).getParameters());
        comparator.assertEquals("TEST_PARAMETERS", new BeanWrapper<>(testParameters),
            paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS).getParameters());
    }

    @Test
    public void testExportWithExclusions() throws Exception {
        ParametersOperations paramOps = new ParametersOperations();

        DatabaseTransactionFactory.performTransaction(() -> {

            // create a param library
            createLibrary();
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {

            // export the library
            List<String> excludeList = new LinkedList<>();
            excludeList.add(TEST_PARAMETERS_FOO);
            excludeList.add(TEST_PARAMETERS_BAZ);
            exportLibrary(excludeList);
            return null;
        });

        // import the library
        @SuppressWarnings("unchecked")
        List<ParameterSetDescriptor> actualResults = (List<ParameterSetDescriptor>) DatabaseTransactionFactory
            .performTransaction(() -> paramOps.importParameterLibrary(libraryFile.getAbsolutePath(),
                null, ParamIoMode.DRYRUN));

        // verify results
        ReflectionEquals comparator = new ReflectionEquals();

        List<ParameterSetDescriptor> expectedResults = new LinkedList<>();
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_BAR,
            TestParametersBar.class.getName(), ParameterSetDescriptor.State.SAME));
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_BAZ,
            TestParametersBaz.class.getName(), ParameterSetDescriptor.State.LIBRARY_ONLY));
        expectedResults.add(new ParameterSetDescriptor(TEST_PARAMETERS_FOO,
            TestParametersFoo.class.getName(), ParameterSetDescriptor.State.LIBRARY_ONLY));

        comparator.excludeField(".*\\.libraryProps");
        comparator.excludeField(".*\\.fileProps");
        comparator.excludeField(".*\\.libraryParamSet");
        comparator.excludeField(".*\\.importedParamsBean");
        comparator.assertEquals("results", expectedResults, actualResults);
    }

    @Test
    public void testExportToInvalidExistingFile() throws Exception {
        expectedException.expectCause(IsInstanceOf.<Throwable> instanceOf(PipelineException.class));
        ParametersOperations paramOps = new ParametersOperations();

        DatabaseTransactionFactory.performTransaction(() -> {

            // create a param library
            createLibrary();
            return null;
        });

        DatabaseTransactionFactory.performTransaction(() -> {

            // export the library
            File invalidExportDir = new File(Filenames.BUILD_TEST, "invalid-param-lib");
            FileUtils.forceMkdir(invalidExportDir);
            // should throw IllegalArgumentException
            paramOps.exportParameterLibrary(invalidExportDir.getAbsolutePath(), null,
                ParamIoMode.STANDARD);
            return null;
        });
    }

    /**
     * Tests that importing a parameter library containing a parameter not in the parameter set will
     * fail.
     *
     * @throws Exception if there is a problem reading a test data file
     */
    @Test(expected = UnmarshalException.class)
    public void testNonexistentParameter() throws Exception {
        ParametersOperations ops = new ParametersOperations();
        ops.importParameterLibrary("src/test/resources/bad-parameter-library.xml",
            Collections.emptyList(), ParamIoMode.STANDARD);
    }

    @Test
    public void testImportFromFile() throws Exception {
        ParametersOperations ops = new ParametersOperations();
        List<ParameterSetDescriptor> paramsDescriptors = ops
            .importParameterLibrary("test/data/paramlib/test.xml", null, ParamIoMode.NODB);
        assertEquals(4, paramsDescriptors.size());
        for (ParameterSetDescriptor descriptor : paramsDescriptors) {
            assertEquals(ParameterSetDescriptor.State.CREATE, descriptor.getState());
        }

        // Check the contents of the descriptors
        Map<String, ParameterSetDescriptor> nameToParameterSetDescriptor = nameToParameterSetDescriptor(
            paramsDescriptors);

        // Start with the RemoteParameters instance in which everything is a String; remember that
        // for parameters that have a Java-side class description, we don't save the data types
        // because the data types are defined as part of the class
        ParameterSetDescriptor descriptor = nameToParameterSetDescriptor.get("Remote Hyperion L1");
        assertEquals("gov.nasa.ziggy.module.remote.RemoteParameters", descriptor.getClassName());
        Set<TypedParameter> typedProperties = descriptor.getImportedParamsBean()
            .getTypedProperties();
        assertEquals(5, typedProperties.size());
        Map<String, TypedParameter> nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "enabled", "false",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "gigsPerSubtask", "0.1",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "subtaskMaxWallTimeHours", "2.1",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "subtaskTypicalWallTimeHours",
            "2.1", ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "optimizer", "COST",
            ZiggyDataType.ZIGGY_STRING, true));

        // sample classless parameter set
        descriptor = nameToParameterSetDescriptor.get("Sample classless parameter set");
        assertEquals("gov.nasa.ziggy.parameters.DefaultParameters", descriptor.getClassName());
        typedProperties = descriptor.getImportedParamsBean().getTypedProperties();
        assertEquals(3, typedProperties.size());
        nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z1", "100",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z2", "28.56,57.12",
            ZiggyDataType.ZIGGY_FLOAT, false));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z3", "some text",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set
        descriptor = nameToParameterSetDescriptor.get("ISOFIT module parameters");
        assertEquals("gov.nasa.ziggy.parameters.DefaultParameters", descriptor.getClassName());
        typedProperties = descriptor.getImportedParamsBean().getTypedProperties();
        assertEquals(3, typedProperties.size());
        nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "n_cores", "4",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
    }

    private Map<String, ParameterSetDescriptor> nameToParameterSetDescriptor(
        Collection<ParameterSetDescriptor> descriptors) {
        Map<String, ParameterSetDescriptor> nameToParameterSet = new HashMap<>();
        for (ParameterSetDescriptor descriptor : descriptors) {
            nameToParameterSet.put(descriptor.getName(), descriptor);
        }
        return nameToParameterSet;

    }

    private Map<String, TypedParameter> nameToTypedPropertyMap(
        Set<TypedParameter> typedProperties) {
        Map<String, TypedParameter> nameToTypedProperty = new HashMap<>();
        for (TypedParameter property : typedProperties) {
            nameToTypedProperty.put(property.getName(), property);
        }
        return nameToTypedProperty;
    }

    @Test
    public void testImportOverrideFromFile() {

        // Import the initial parameter library and persist to the database
        DatabaseTransactionFactory.performTransaction(() -> {
            ParametersOperations ops = new ParametersOperations();
            ops.importParameterLibrary("test/data/paramlib/test.xml", null, ParamIoMode.STANDARD);
            return null;
        });

        // Now import the overrides and persist them to the database
        @SuppressWarnings("unchecked")
        List<ParameterSetDescriptor> descriptors = (List<ParameterSetDescriptor>) DatabaseTransactionFactory
            .performTransaction(() -> {
                ParametersOperations ops = new ParametersOperations();
                return ops.importParameterLibrary("test/data/paramlib/pl-overrides.xml", null,
                    ParamIoMode.STANDARD);
            });

        // Check the descriptor states
        Map<String, ParameterSetDescriptor> nameToDescriptor = nameToParameterSetDescriptor(
            descriptors);
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("Remote Hyperion L1").getState());
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("Sample classless parameter set").getState());
        assertEquals(ParameterSetDescriptor.State.LIBRARY_ONLY,
            nameToDescriptor.get("ISOFIT module parameters").getState());

        // Retrieve the parameter sets from the database and check their values
        ParameterSetCrud paramCrud = new ParameterSetCrud();
        List<ParameterSet> parameterSets = paramCrud.retrieveLatestVersions();
        assertEquals(4, parameterSets.size());
        Map<String, ParameterSet> nameToParameterSet = nameToParameterSet(parameterSets);

        // The Hyperion L1 dataset has its gigs per subtask value changed
        Set<TypedParameter> typedProperties = nameToParameterSet.get("Remote Hyperion L1")
            .getParameters()
            .getTypedProperties();
        assertEquals(14, typedProperties.size());
        Map<String, TypedParameter> nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "enabled", "false",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "gigsPerSubtask", "1.0",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "subtaskMaxWallTimeHours", "2.1",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "subtaskTypicalWallTimeHours",
            "2.1", ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "optimizer", "COST",
            ZiggyDataType.ZIGGY_STRING, true));

        // The sample classless parameter set no changed parameters
        typedProperties = nameToParameterSet.get("Sample classless parameter set")
            .getParameters()
            .getTypedProperties();
        assertEquals(3, typedProperties.size());
        nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z1", "100",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z2", "28.56,57.12",
            ZiggyDataType.ZIGGY_FLOAT, false));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z3", "some text",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set has no changes
        typedProperties = nameToParameterSet.get("ISOFIT module parameters")
            .getParameters()
            .getTypedProperties();
        assertEquals(3, typedProperties.size());
        nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "n_cores", "4",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

    }

    @Test
    public void importReplacementParameterSetsFromFile() {

        // Import the initial parameter library and persist to the database
        DatabaseTransactionFactory.performTransaction(() -> {
            ParametersOperations ops = new ParametersOperations();
            ops.importParameterLibrary("test/data/paramlib/test.xml", null, ParamIoMode.STANDARD);
            return null;
        });

        // Import the replacement parameter library and persist to the database
        @SuppressWarnings("unchecked")
        List<ParameterSetDescriptor> descriptors = (List<ParameterSetDescriptor>) DatabaseTransactionFactory
            .performTransaction(() -> {
                ParametersOperations ops = new ParametersOperations();
                return ops.importParameterLibrary(
                    "test/data/paramlib/pl-replacement-param-sets.xml", null, ParamIoMode.STANDARD);
            });

        assertEquals(5, descriptors.size());

        // Check the descriptor states
        Map<String, ParameterSetDescriptor> nameToDescriptor = nameToParameterSetDescriptor(
            descriptors);
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("Remote Hyperion L1").getState());
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("Sample classless parameter set").getState());
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("ISOFIT module parameters").getState());
        assertEquals(ParameterSetDescriptor.State.CREATE,
            nameToDescriptor.get("All-new parameters").getState());

        // Retrieve the parameter sets from the database and check their values
        ParameterSetCrud paramCrud = new ParameterSetCrud();
        List<ParameterSet> parameterSets = paramCrud.retrieveLatestVersions();
        assertEquals(5, parameterSets.size());
        Map<String, ParameterSet> nameToParameterSet = nameToParameterSet(parameterSets);

        // The Hyperion L1 dataset has only its gigsPerSubtask set to a non-default value
        Set<TypedParameter> typedProperties = nameToParameterSet.get("Remote Hyperion L1")
            .getParameters()
            .getTypedProperties();
        assertEquals(14, typedProperties.size());
        Map<String, TypedParameter> nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "enabled", "false",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "gigsPerSubtask", "3.0",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "subtaskMaxWallTimeHours", "0.0",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "subtaskTypicalWallTimeHours",
            "0.0", ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "optimizer", "CORES",
            ZiggyDataType.ZIGGY_STRING, true));

        // The sample classless parameter set now has only 1 parameter
        typedProperties = nameToParameterSet.get("Sample classless parameter set")
            .getParameters()
            .getTypedProperties();
        assertEquals(1, typedProperties.size());
        nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z1", "100",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set has no changes
        typedProperties = nameToParameterSet.get("ISOFIT module parameters")
            .getParameters()
            .getTypedProperties();
        assertEquals(3, typedProperties.size());
        nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "n_cores", "4",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // All-new parameters has 1 parameter
        typedProperties = nameToParameterSet.get("All-new parameters")
            .getParameters()
            .getTypedProperties();
        assertEquals(1, typedProperties.size());
        nameToTypedProperty = nameToTypedPropertyMap(typedProperties);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "parameter", "4",
            ZiggyDataType.ZIGGY_INT, true));
    }

    // And now for a bunch of tests that exercise all the error cases

    // Test import of a parameter library with a mismatched parameter
    @Test(expected = IllegalArgumentException.class)
    public void testMismatchedParameterImportException() throws Exception {
        ParametersOperations ops = new ParametersOperations();
        ops.importParameterLibrary("test/data/paramlib/params-mismatch.xml", null,
            ParamIoMode.STANDARD);
    }

    // Test an override that tries to change the parameters in a DefaultParameters instance
    @Test(expected = IllegalArgumentException.class)
    public void testOverrideWithNewParamException() throws Exception {

        // Import the initial parameter library and persist to the database
        DatabaseTransactionFactory.performTransaction(() -> {
            ParametersOperations ops = new ParametersOperations();
            ops.importParameterLibrary("test/data/paramlib/test.xml", null, ParamIoMode.STANDARD);
            return null;
        });

        ParametersOperations ops = new ParametersOperations();
        ops.importParameterLibrary("test/data/paramlib/pl-override-mismatch.xml", null,
            ParamIoMode.STANDARD);
    }

    // Test an override that changes the type of a DefaultParameter instance
    @Test(expected = IllegalStateException.class)
    public void testOverrideWithBadTypeException() throws Exception {

        // Import the initial parameter library and persist to the database
        DatabaseTransactionFactory.performTransaction(() -> {
            ParametersOperations ops = new ParametersOperations();
            ops.importParameterLibrary("test/data/paramlib/test.xml", null, ParamIoMode.STANDARD);
            return null;
        });

        ParametersOperations ops = new ParametersOperations();
        ops.importParameterLibrary("test/data/paramlib/pl-override-bad-type.xml", null,
            ParamIoMode.STANDARD);
    }

    // Test an override that has a parameter set that's not already in the database
    @Test(expected = PipelineException.class)
    public void testOverrideWithNewParamSetException() throws Exception {

        // Import the initial parameter library and persist to the database
        DatabaseTransactionFactory.performTransaction(() -> {
            ParametersOperations ops = new ParametersOperations();
            ops.importParameterLibrary("test/data/paramlib/test.xml", null, ParamIoMode.STANDARD);
            return null;
        });

        ParametersOperations ops = new ParametersOperations();
        ops.importParameterLibrary("test/data/paramlib/pl-override-new-param-set.xml", null,
            ParamIoMode.STANDARD);
    }

    private Map<String, ParameterSet> nameToParameterSet(Collection<ParameterSet> parameterSets) {
        Map<String, ParameterSet> nameToParameterSet = new HashMap<>();
        for (ParameterSet parameterSet : parameterSets) {
            nameToParameterSet.put(parameterSet.getName().getName(), parameterSet);
        }
        return nameToParameterSet;
    }

    private boolean checkTypedPropertyValues(Map<String, TypedParameter> nameToTypedProperty,
        String name, String value, ZiggyDataType type, boolean scalar) {
        boolean goodProperty = nameToTypedProperty.containsKey(name);
        if (goodProperty) {
            TypedParameter property = nameToTypedProperty.get(name);
            goodProperty = goodProperty && property.getString().equals(value);
            goodProperty = goodProperty && property.getDataType().equals(type);
            goodProperty = goodProperty && scalar == property.isScalar();
        }

        return goodProperty;
    }

    /**
     * Implements a test bean that has one parameter.
     */
    public static class TestBean implements Parameters {

        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }

}
