package gov.nasa.ziggy.pipeline.xml;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.pipeline.definition.database.ParametersOperationsTest.TEST_PARAMETERS_BAR;
import static gov.nasa.ziggy.pipeline.definition.database.ParametersOperationsTest.TEST_PARAMETERS_FOO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperationsTest;
import gov.nasa.ziggy.pipeline.xml.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Test the {@link ParameterImportExportOperations} class.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class ParameterImportExportOperationsTest {

    private File libraryFile;

    private ParameterImportExportOperations parameterImportExportOperations;
    private ParametersOperations parametersOperations;
    private ParametersOperationsTest parametersOperationsTest;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {
        libraryFile = directoryRule.directory()
            .resolve("param-lib")
            .resolve("param-lib.xml")
            .toFile();
        parameterImportExportOperations = new ParameterImportExportOperations();
        parametersOperations = new ParametersOperations();
        parametersOperationsTest = new ParametersOperationsTest();
    }

    @Test
    public void testRoundTrip() throws Exception {

        // Create a param library.
        parametersOperationsTest.setUpParameterSets();

        // Export the library.
        exportLibrary(null);

        // Make some changes to the library.
        modifyLibrary();

        // Read the library back in and persist it.
        List<ParameterSetDescriptor> actualResults = parameterImportExportOperations
            .importParameterLibrary(libraryFile.getAbsolutePath(), null, ParamIoMode.STANDARD);

        // Verify results.
        ReflectionEquals comparator = new ReflectionEquals();

        // Check the existence and state of each parameter set -- the TEST_PARAMETERS set should be
        // LIBRARY_ONLY (created in the modifyLibrary() call), TEST_PARAMETERS_FOO should be CREATE
        // (it got deleted in the modifyLibrary() call), TEST_PARAMETERS_BAR should be UPDATE
        // (its modified value got overwritten in the import), TEST_PARAMETERS_BAZ should be SAME
        // (untouched by the modifyLibrary() operation).
        List<ParameterSetDescriptor> expectedResults = new LinkedList<>();
        expectedResults.add(new ParameterSetDescriptor(
            parametersOperationsTest.getParameterSetBar(), ParameterSetDescriptor.State.UPDATE));
        expectedResults.add(new ParameterSetDescriptor(
            parametersOperationsTest.getParameterSetFoo(), ParameterSetDescriptor.State.CREATE));

        comparator.excludeField(".*\\.libraryProps");
        comparator.excludeField(".*\\.fileProps");
        comparator.excludeField(".*\\.parameterSet");
        comparator.assertEquals("results", sortDescriptorsByName(expectedResults),
            sortDescriptorsByName(actualResults));

        ParameterSetCrud paramCrud = new ParameterSetCrud();

        // Compare the database values for parameters with the ORIGINAL values for foo and bar. This
        // confirms that value updates got overwritten when the library was re-imported. That is to
        // say, values were changed in the database and then those changes got reversed when the
        // (pre-change) library was read back in.
        comparator.assertEquals("TEST_PARAMETERS_BAR",
            parametersOperationsTest.getParameterSetBar().getParameters(), new TreeSet<>(
                paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_BAR).getParameters()));
        comparator.assertEquals("TEST_PARAMETERS_FOO",
            parametersOperationsTest.getParameterSetFoo().getParameters(), new TreeSet<>(
                paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO).getParameters()));
    }

    private void exportLibrary(List<String> excludeList) throws IOException {
        if (libraryFile.getParentFile().exists()) {
            FileUtils.cleanDirectory(libraryFile.getParentFile());
        } else {
            Files.createDirectories(libraryFile.getParentFile().toPath());
        }
        parameterImportExportOperations.exportParameterLibrary(libraryFile.getAbsolutePath(),
            excludeList, ParamIoMode.STANDARD);
    }

    private void modifyLibrary() {

        // Delete a param set to trigger CREATE.
        ParameterSetCrud paramCrud = new ParameterSetCrud();
        ParameterSet fooPs = paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_FOO);
        testOperations.remove(fooPs);

        // Modify a param set to trigger UPDATE.
        ParameterSet barPs = paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_BAR);
        ParameterSet barPsUpdated = new ParameterSet(barPs);
        barPsUpdated.parameterByName().get("p3").setValue(98.5);
        parametersOperations.updateParameterSet(barPs, barPsUpdated, false);
    }

    private List<ParameterSetDescriptor> sortDescriptorsByName(
        List<ParameterSetDescriptor> unsorted) {
        return unsorted.stream().sorted().collect(Collectors.toList());
    }

    @Test
    public void testRoundTripWithDryRun() throws Exception {
        // Create a param library.
        parametersOperationsTest.setUpParameterSets();

        // Export the library.
        exportLibrary(null);

        // Make some changes to the library.
        modifyLibrary();

        // Import the library.
        List<ParameterSetDescriptor> actualResults = parameterImportExportOperations
            .importParameterLibrary(libraryFile.getAbsolutePath(), null, ParamIoMode.DRYRUN);

        // Verify results.
        ReflectionEquals comparator = new ReflectionEquals();

        List<ParameterSetDescriptor> expectedResults = new LinkedList<>();
        expectedResults.add(new ParameterSetDescriptor(
            parametersOperationsTest.getParameterSetBar(), ParameterSetDescriptor.State.UPDATE));
        expectedResults.add(new ParameterSetDescriptor(
            parametersOperationsTest.getParameterSetFoo(), ParameterSetDescriptor.State.CREATE));

        comparator.excludeField(".*\\.libraryProps");
        comparator.excludeField(".*\\.fileProps");
        comparator.excludeField(".*\\.parameterSet");
        comparator.assertEquals("results", sortDescriptorsByName(expectedResults),
            sortDescriptorsByName(actualResults));

        ParameterSetCrud paramCrud = new ParameterSetCrud();

        ParameterSet barPsUpdated = new ParameterSet(parametersOperationsTest.getParameterSetBar());
        barPsUpdated.parameterByName().get("p3").setValue(98.5);

        comparator.assertEquals("TEST_PARAMETERS_BAR", barPsUpdated.getParameters(), new TreeSet<>(
            paramCrud.retrieveLatestVersionForName(TEST_PARAMETERS_BAR).getParameters()));
    }

    @Test
    public void testExportWithExclusions() throws Exception {

        // create a param library
        parametersOperationsTest.setUpParameterSets();
        // export the library
        List<String> excludeList = new LinkedList<>();
        excludeList.add(TEST_PARAMETERS_FOO);
        exportLibrary(excludeList);

        // import the library
        List<ParameterSetDescriptor> actualResults = parameterImportExportOperations
            .importParameterLibrary(libraryFile.getAbsolutePath(), null, ParamIoMode.DRYRUN);

        // verify results
        ReflectionEquals comparator = new ReflectionEquals();

        List<ParameterSetDescriptor> expectedResults = new LinkedList<>();
        expectedResults.add(new ParameterSetDescriptor(
            parametersOperationsTest.getParameterSetBar(), ParameterSetDescriptor.State.SAME));
        expectedResults
            .add(new ParameterSetDescriptor(parametersOperationsTest.getParameterSetFoo(),
                ParameterSetDescriptor.State.LIBRARY_ONLY));

        comparator.excludeField(".*\\.libraryProps");
        comparator.excludeField(".*\\.fileProps");
        comparator.excludeField(".*\\.parameterSet");
        comparator.assertEquals("results", sortDescriptorsByName(expectedResults),
            sortDescriptorsByName(actualResults));
    }

    @Test
    public void testExportToInvalidExistingFile() throws Exception {
        Path invalidExportDir = directoryRule.directory()
            .resolve("invalid-param-lib")
            .toAbsolutePath();
        File invalidExportFile = directoryRule.directory().resolve("invalid-param-lib").toFile();
        PipelineException exception = assertThrows(PipelineException.class, () -> {

            // create a param library
            parametersOperationsTest.setUpParameterSets();

            // export the library
            FileUtils.forceMkdir(invalidExportFile);
            // should throw IllegalArgumentException
            parameterImportExportOperations.exportParameterLibrary(
                invalidExportFile.getAbsolutePath(), null, ParamIoMode.STANDARD);
        });
        String expectedMessage = "Unable to marshal to " + invalidExportDir.toString();
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testImportFromFile() throws Exception {
        List<ParameterSetDescriptor> paramsDescriptors = parameterImportExportOperations
            .importParameterLibrary(TEST_DATA.resolve("paramlib").resolve("test.xml").toString(),
                null, ParamIoMode.NODB);
        assertEquals(3, paramsDescriptors.size());
        for (ParameterSetDescriptor descriptor : paramsDescriptors) {
            assertEquals(ParameterSetDescriptor.State.CREATE, descriptor.getState());
        }

        // Check the contents of the descriptors
        Map<String, ParameterSetDescriptor> nameToParameterSetDescriptor = nameToParameterSetDescriptor(
            paramsDescriptors);

        // sample classless parameter set
        ParameterSetDescriptor descriptor = nameToParameterSetDescriptor
            .get("Sample classless parameter set");
        Set<Parameter> parameters = descriptor.getImportedProperties();
        assertEquals(3, parameters.size());
        Map<String, Parameter> nameToTypedProperty = nameToTypedPropertyMap(parameters);
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z1", "100",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z2", "28.56,57.12",
            ZiggyDataType.ZIGGY_FLOAT, false));
        assertTrue(checkTypedPropertyValues(nameToTypedProperty, "z3", "some text",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set
        descriptor = nameToParameterSetDescriptor.get("ISOFIT module parameters");
        parameters = descriptor.getImportedProperties();
        assertEquals(3, parameters.size());
        nameToTypedProperty = nameToTypedPropertyMap(parameters);
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

    private Map<String, Parameter> nameToTypedPropertyMap(Set<Parameter> parameters) {
        Map<String, Parameter> nameToTypedProperty = new HashMap<>();
        for (Parameter property : parameters) {
            nameToTypedProperty.put(property.getName(), property);
        }
        return nameToTypedProperty;
    }

    private boolean checkTypedPropertyValues(Map<String, Parameter> parametersByName, String name,
        String value, ZiggyDataType type, boolean scalar) {
        boolean goodProperty = parametersByName.containsKey(name);
        if (goodProperty) {
            Parameter parameter = parametersByName.get(name);
            goodProperty = goodProperty && parameter.getString().equals(value);
            goodProperty = goodProperty && parameter.getDataType().equals(type);
            goodProperty = goodProperty && scalar == parameter.isScalar();
        }

        return goodProperty;
    }

    // Test an override that tries to change the parameters in a Parameters instance
    @Test(expected = IllegalArgumentException.class)
    public void testOverrideWithNewParamException() throws Exception {

        // Import the initial parameter library and persist to the database
        parameterImportExportOperations.importParameterLibrary(
            TEST_DATA.resolve("paramlib").resolve("test.xml").toString(), null,
            ParamIoMode.STANDARD);

        parameterImportExportOperations.importParameterLibrary(
            TEST_DATA.resolve("paramlib").resolve("pl-override-mismatch.xml").toString(), null,
            ParamIoMode.STANDARD);
    }

    // Test an override that changes the type of a Parameters instance
    @Test(expected = NumberFormatException.class)
    public void testOverrideWithBadTypeException() throws Exception {

        // Import the initial parameter library and persist to the database
        parameterImportExportOperations.importParameterLibrary(
            TEST_DATA.resolve("paramlib").resolve("test.xml").toString(), null,
            ParamIoMode.STANDARD);

        parameterImportExportOperations.importParameterLibrary(
            TEST_DATA.resolve("paramlib").resolve("pl-override-bad-type.xml").toString(), null,
            ParamIoMode.STANDARD);
    }

    // Test an override that has a parameter set that's not already in the database
    @Test(expected = PipelineException.class)
    public void testOverrideWithNewParamSetException() throws Exception {

        // Import the initial parameter library and persist to the database
        parameterImportExportOperations.importParameterLibrary(
            TEST_DATA.resolve("paramlib").resolve("test.xml").toString(), null,
            ParamIoMode.STANDARD);

        parameterImportExportOperations.importParameterLibrary(
            TEST_DATA.resolve("paramlib").resolve("pl-override-new-param-set.xml").toString(), null,
            ParamIoMode.STANDARD);
    }

    @Test
    public void testImportOverrideFromFile() {

        // Import the initial parameter library and persist to the database
        parameterImportExportOperations.importParameterLibrary(
            TEST_DATA.resolve("paramlib").resolve("test.xml").toString(), null,
            ParamIoMode.STANDARD);

        // Now import the overrides and persist them to the database
        List<ParameterSetDescriptor> descriptors = parameterImportExportOperations
            .importParameterLibrary(
                TEST_DATA.resolve("paramlib").resolve("pl-overrides.xml").toString(), null,
                ParamIoMode.STANDARD);

        // Check the descriptor states
        Map<String, ParameterSetDescriptor> nameToDescriptor = nameToParameterSetDescriptor(
            descriptors);
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("Sample classless parameter set").getState());
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("ISOFIT module parameters").getState());
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("Multiple subtask configuration").getState());

        // Retrieve the parameter sets from the database and check their values
        List<ParameterSet> parameterSets = parametersOperations.parameterSets();
        assertEquals(3, parameterSets.size());
        Map<String, ParameterSet> nameToParameterSet = nameToParameterSet(parameterSets);

        // The sample classless parameter set no changed parameters
        Set<Parameter> parameters = nameToParameterSet.get("Sample classless parameter set")
            .getParameters();
        assertEquals(3, parameters.size());
        Map<String, Parameter> parametersByName = nameToTypedPropertyMap(parameters);
        assertTrue(
            checkTypedPropertyValues(parametersByName, "z1", "100", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "z2", "28.56,57.12",
            ZiggyDataType.ZIGGY_FLOAT, false));
        assertTrue(checkTypedPropertyValues(parametersByName, "z3", "some text",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set has one changed parameter
        parameters = nameToParameterSet.get("ISOFIT module parameters").getParameters();
        assertEquals(3, parameters.size());
        parametersByName = nameToTypedPropertyMap(parameters);
        assertTrue(checkTypedPropertyValues(parametersByName, "n_cores", "8",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // The TaskConfigurationParameters set has new values for its reprocessing tasks exclude
        // parameter.
        parameters = nameToParameterSet.get("Multiple subtask configuration").getParameters();
        assertEquals(5, parameters.size());
        parametersByName = nameToTypedPropertyMap(parameters);
        assertTrue(checkTypedPropertyValues(parametersByName, "taskDirectoryRegex",
            "set-([0-9]{1})", ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "reprocessingTasksExclude", "1,2,3",
            ZiggyDataType.ZIGGY_INT, false));
        assertTrue(checkTypedPropertyValues(parametersByName, "singleSubtask", "false",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "maxFailedSubtaskCount", "0",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "reprocess", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
    }

    private Map<String, ParameterSet> nameToParameterSet(Collection<ParameterSet> parameterSets) {
        Map<String, ParameterSet> nameToParameterSet = new HashMap<>();
        for (ParameterSet parameterSet : parameterSets) {
            nameToParameterSet.put(parameterSet.getName(), parameterSet);
        }
        return nameToParameterSet;
    }

    @Test
    public void importReplacementParameterSetsFromFile() {

        // Import the initial parameter library and persist to the database
        parameterImportExportOperations.importParameterLibrary(
            TEST_DATA.resolve("paramlib").resolve("test.xml").toString(), null,
            ParamIoMode.STANDARD);

        // Import the replacement parameter library and persist to the database
        List<ParameterSetDescriptor> descriptors = parameterImportExportOperations
            .importParameterLibrary(
                TEST_DATA.resolve("paramlib").resolve("pl-replacement-param-sets.xml").toString(),
                null, ParamIoMode.STANDARD);

        assertEquals(4, descriptors.size());

        // Check the descriptor states
        Map<String, ParameterSetDescriptor> nameToDescriptor = nameToParameterSetDescriptor(
            descriptors);
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("Sample classless parameter set").getState());
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("ISOFIT module parameters").getState());
        assertEquals(ParameterSetDescriptor.State.CREATE,
            nameToDescriptor.get("All-new parameters").getState());
        assertEquals(ParameterSetDescriptor.State.LIBRARY_ONLY,
            nameToDescriptor.get("Multiple subtask configuration").getState());

        // Retrieve the parameter sets from the database and check their values
        List<ParameterSet> parameterSets = parametersOperations.parameterSets();
        assertEquals(4, parameterSets.size());
        Map<String, ParameterSet> parameterSetsByName = nameToParameterSet(parameterSets);

        // The sample classless parameter set now has only 1 parameter
        Set<Parameter> parameters = parameterSetsByName.get("Sample classless parameter set")
            .getParameters();
        assertEquals(1, parameters.size());
        Map<String, Parameter> parametersByName = nameToTypedPropertyMap(parameters);
        assertTrue(checkTypedPropertyValues(parametersByName, "z1", "100",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set has no changes
        parameters = parameterSetsByName.get("ISOFIT module parameters").getParameters();
        assertEquals(3, parameters.size());
        parametersByName = nameToTypedPropertyMap(parameters);
        assertTrue(checkTypedPropertyValues(parametersByName, "n_cores", "4",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // All-new parameters has 1 parameter
        parameters = parameterSetsByName.get("All-new parameters").getParameters();
        assertEquals(1, parameters.size());
        parametersByName = nameToTypedPropertyMap(parameters);
        assertTrue(checkTypedPropertyValues(parametersByName, "parameter", "4",
            ZiggyDataType.ZIGGY_INT, true));

        // The TaskConfigurationParameters set has no changes.
        parameters = parameterSetsByName.get("Multiple subtask configuration").getParameters();
        assertEquals(5, parameters.size());
        parametersByName = nameToTypedPropertyMap(parameters);
        assertTrue(checkTypedPropertyValues(parametersByName, "taskDirectoryRegex",
            "set-([0-9]{1})", ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "reprocessingTasksExclude", "0",
            ZiggyDataType.ZIGGY_INT, false));
        assertTrue(checkTypedPropertyValues(parametersByName, "singleSubtask", "false",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "maxFailedSubtaskCount", "0",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkTypedPropertyValues(parametersByName, "reprocess", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
    }

    private static class TestOperations extends DatabaseOperations {

        public void remove(ParameterSet parameterSet) {
            performTransaction(() -> new ParameterSetCrud().remove(parameterSet));
        }
    }
}
