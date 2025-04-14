package gov.nasa.ziggy.pipeline.definition.importer;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.pipeline.definition.database.ParametersOperationsTest.TEST_PARAMETERS_BAR;
import static gov.nasa.ziggy.pipeline.definition.database.ParametersOperationsTest.TEST_PARAMETERS_FOO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionExporter;
import gov.nasa.ziggy.pipeline.definition.database.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperationsTest;
import gov.nasa.ziggy.pipeline.xml.ParameterSetDescriptor;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.util.PipelineException;

/** Unit tests for the {@link ParameterSetImportConditioner} class. */
public class ParameterSetImportConditionerTest {

    private ParameterSetImportConditioner parameterSetImportConditioner;
    private PipelineImportOperations pipelineImportOperations = new PipelineImportOperations();
    private ParametersOperations parametersOperations = new ParametersOperations();
    private ParametersOperationsTest parametersOperationsTest = new ParametersOperationsTest();
    private TestOperations testOperations = new TestOperations();
    private Path libraryFile;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {
        parameterSetImportConditioner = new ParameterSetImportConditioner();
        libraryFile = directoryRule.directory().resolve("param-lib").resolve("param-lib.xml");
    }

    @Test
    public void testParameterSetRoundTrip() throws Exception {

        // Create a param library.
        parametersOperationsTest.setUpParameterSets();

        // Export the library.
        exportParameterSets(null);

        // Make some changes to the library.
        modifyParameterSets();

        // Read the library back in and persist it.

        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(libraryFile), true);
        pipelineImportOperations.persistParameterSets(actualResults);
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

    private List<ParameterSet> unmarshalParameterSets(Path libraryFile) {
        PipelineDefinitionImporter pipelineDefinitionImporter = new PipelineDefinitionImporter(
            List.of(libraryFile));
        return pipelineDefinitionImporter.getParameterSets();
    }

    private void exportParameterSets(List<String> excludeList) throws IOException {
        if (Files.exists(libraryFile.getParent())) {
            FileUtils.cleanDirectory(libraryFile.getParent().toFile());
        } else {
            Files.createDirectories(libraryFile.getParent());
        }
        new PipelineDefinitionExporter().exportPipelineConfiguration(null,
            parametersOperations.parameterSets(), libraryFile.toAbsolutePath().toString());
    }

    private void modifyParameterSets() {

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
    public void testParameterSetRoundTripNoPersisting() throws Exception {
        // Create a param library.
        parametersOperationsTest.setUpParameterSets();

        // Export the library.
        exportParameterSets(null);

        // Make some changes to the library.
        modifyParameterSets();

        // Import the library.
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(libraryFile), true);

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
    public void testParameterSetImportFromFile() throws Exception {
        List<ParameterSetDescriptor> paramsDescriptors = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);

        assertEquals(3, paramsDescriptors.size());
        for (ParameterSetDescriptor descriptor : paramsDescriptors) {
            assertEquals(ParameterSetDescriptor.State.CREATE, descriptor.getState());
            assertNotNull(descriptor.getFileProps());
            assertNull(descriptor.getLibraryProps());
        }

        // Check the contents of the descriptors
        Map<String, ParameterSetDescriptor> parameterSetDescriptorByName = parameterSetDescriptorByName(
            paramsDescriptors);

        // sample classless parameter set
        ParameterSetDescriptor descriptor = parameterSetDescriptorByName
            .get("Sample classless parameter set");
        Set<Parameter> parameters = descriptor.getImportedProperties();
        assertEquals(3, parameters.size());
        Map<String, Parameter> parameterByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parameterByName, "z1", "100", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parameterByName, "z2", "28.56,57.12",
            ZiggyDataType.ZIGGY_FLOAT, false));
        assertTrue(checkParameterValues(parameterByName, "z3", "some text",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set
        descriptor = parameterSetDescriptorByName.get("ISOFIT algorithm parameters");
        parameters = descriptor.getImportedProperties();
        assertEquals(3, parameters.size());
        parameterByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parameterByName, "n_cores", "4", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parameterByName, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parameterByName, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // Multiple subtask configuration.
        descriptor = parameterSetDescriptorByName.get("Multiple subtask configuration");
        parameters = descriptor.getImportedProperties();
        assertEquals(5, parameters.size());
        parameterByName = parameterByName(parameters);
        assertTrue(checkParameterValues(parameterByName, "taskDirectoryRegex", "set-([0-9]{1})",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkParameterValues(parameterByName, "singleSubtask", "false",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parameterByName, "maxFailedSubtaskCount", "0",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parameterByName, "reprocess", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parameterByName, "reprocessingTasksExclude", "0",
            ZiggyDataType.ZIGGY_INT, false));
    }

    private Map<String, ParameterSetDescriptor> parameterSetDescriptorByName(
        Collection<ParameterSetDescriptor> descriptors) {
        Map<String, ParameterSetDescriptor> parameterSetByName = new HashMap<>();
        for (ParameterSetDescriptor descriptor : descriptors) {
            parameterSetByName.put(descriptor.getName(), descriptor);
        }
        return parameterSetByName;
    }

    private Map<String, Parameter> parameterByName(Set<Parameter> parameters) {
        Map<String, Parameter> parameterByName = new HashMap<>();
        for (Parameter property : parameters) {
            parameterByName.put(property.getName(), property);
        }
        return parameterByName;
    }

    private boolean checkParameterValues(Map<String, Parameter> parameterByName, String name,
        String value, ZiggyDataType type, boolean scalar) {
        boolean goodParameter = parameterByName.containsKey(name);
        if (goodParameter) {
            Parameter parameter = parameterByName.get(name);
            goodParameter = goodParameter && parameter.getString().equals(value);
            goodParameter = goodParameter && parameter.getDataType().equals(type);
            goodParameter = goodParameter && scalar == parameter.isScalar();
        }

        return goodParameter;
    }

    // Test an override that tries to add a parameter in a ParameterSet instance
    @Test(expected = IllegalArgumentException.class)
    public void testParameterSetOverrideWithNewParamException() throws Exception {

        // Import the initial parameter library and persist to the database
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Import an override parameter set that adds a parameter.
        actualResults = parameterSetImportConditioner.parameterSetDescriptors(
            unmarshalParameterSets(TEST_DATA.resolve("pl-override-mismatch.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);
    }

    // Test an override that changes the type of a Parameters instance
    @Test(expected = NumberFormatException.class)
    public void testParameterSetOverrideWithBadTypeException() throws Exception {

        // Import the initial parameter library and persist to the database
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Import an override parameter set that changes the type a parameter.
        actualResults = parameterSetImportConditioner.parameterSetDescriptors(
            unmarshalParameterSets(TEST_DATA.resolve("pl-override-bad-type.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);
    }

    // Test an override that has a parameter set that's not already in the database
    @Test(expected = PipelineException.class)
    public void testParameterSetOverrideWithNewParamSetException() throws Exception {

        // Import the initial parameter library and persist to the database
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Import an override parameter set that doesn't exist in the library.
        actualResults = parameterSetImportConditioner.parameterSetDescriptors(
            unmarshalParameterSets(TEST_DATA.resolve("pl-override-new-param-set.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);
    }

    @Test(expected = PipelineException.class)
    public void testParameterSetOverrideWithNewParamSetNoUpdateFlag() {

        // Import the initial parameter library and persist to the database
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Import an override parameter set that doesn't exist in the library.
        actualResults = parameterSetImportConditioner.parameterSetDescriptors(
            unmarshalParameterSets(TEST_DATA.resolve("pl-override-new-param-set.xml")), false);
        pipelineImportOperations.persistParameterSets(actualResults);
    }

    @Test
    public void testImportOverrideFromFile() {

        // Import the initial parameter library and persist to the database
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Now import the overrides and persist them to the database
        actualResults = parameterSetImportConditioner.parameterSetDescriptors(
            unmarshalParameterSets(TEST_DATA.resolve("pl-overrides.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Check the descriptor states
        Map<String, ParameterSetDescriptor> nameToDescriptor = parameterSetDescriptorByName(
            actualResults);
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("Sample classless parameter set").getState());
        assertNotNull(nameToDescriptor.get("Sample classless parameter set").getLibraryProps());
        assertNotNull(nameToDescriptor.get("Sample classless parameter set").getFileProps());
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("ISOFIT algorithm parameters").getState());
        assertNotNull(nameToDescriptor.get("ISOFIT algorithm parameters").getLibraryProps());
        assertNotNull(nameToDescriptor.get("ISOFIT algorithm parameters").getFileProps());
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("Multiple subtask configuration").getState());
        assertNotNull(nameToDescriptor.get("Multiple subtask configuration").getLibraryProps());
        assertNotNull(nameToDescriptor.get("Multiple subtask configuration").getFileProps());

        // Retrieve the parameter sets from the database and check their values
        List<ParameterSet> parameterSets = parametersOperations.parameterSets();
        assertEquals(3, parameterSets.size());
        Map<String, ParameterSet> parameterSetByName = ParameterSet
            .parameterSetByName(parameterSets);

        // The sample classless parameter set no changed parameters
        Set<Parameter> parameters = parameterSetByName.get("Sample classless parameter set")
            .getParameters();
        assertEquals(3, parameters.size());
        Map<String, Parameter> parametersByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parametersByName, "z1", "100", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "z2", "28.56,57.12",
            ZiggyDataType.ZIGGY_FLOAT, false));
        assertTrue(checkParameterValues(parametersByName, "z3", "some text",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set has one changed parameter
        parameters = parameterSetByName.get("ISOFIT algorithm parameters").getParameters();
        assertEquals(3, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parametersByName, "n_cores", "8", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parametersByName, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // The TaskConfigurationParameters set has new values for its reprocessing tasks exclude
        // parameter.
        parameters = parameterSetByName.get("Multiple subtask configuration").getParameters();
        assertEquals(5, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(checkParameterValues(parametersByName, "taskDirectoryRegex", "set-([0-9]{1})",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkParameterValues(parametersByName, "reprocessingTasksExclude", "1,2,3",
            ZiggyDataType.ZIGGY_INT, false));
        assertTrue(checkParameterValues(parametersByName, "singleSubtask", "false",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parametersByName, "maxFailedSubtaskCount", "0",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "reprocess", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
    }

    public void testNoUpdateParameterSetOverrideImportFromFile() {

        // Import the initial parameter library and persist to the database
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Now import the overrides and persist them to the database
        actualResults = parameterSetImportConditioner.parameterSetDescriptors(
            unmarshalParameterSets(TEST_DATA.resolve("pl-overrides.xml")), false);
        pipelineImportOperations.persistParameterSets(actualResults);

        // All the descriptors are SAME.
        Map<String, ParameterSetDescriptor> nameToDescriptor = parameterSetDescriptorByName(
            actualResults);
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("Sample classless parameter set").getState());
        assertNotNull(nameToDescriptor.get("Sample classless parameter set").getLibraryProps());
        assertNotNull(nameToDescriptor.get("Sample classless parameter set").getFileProps());
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("ISOFIT algorithm parameters").getState());
        assertNotNull(nameToDescriptor.get("ISOFIT algorithm parameters").getLibraryProps());
        assertNotNull(nameToDescriptor.get("ISOFIT algorithm parameters").getFileProps());
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("Multiple subtask configuration").getState());
        assertEquals(3, nameToDescriptor.size());
        assertNotNull(nameToDescriptor.get("Multiple subtask configuration").getLibraryProps());
        assertNotNull(nameToDescriptor.get("Multiple subtask configuration").getFileProps());

        // Retrieve the parameter sets from the database and check their values
        List<ParameterSet> parameterSets = parametersOperations.parameterSets();
        assertEquals(3, parameterSets.size());
        Map<String, ParameterSet> nameToParameterSet = ParameterSet
            .parameterSetByName(parameterSets);

        // The sample classless parameter set has no changed parameters
        Set<Parameter> parameters = nameToParameterSet.get("Sample classless parameter set")
            .getParameters();
        assertEquals(3, parameters.size());
        Map<String, Parameter> parametersByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parametersByName, "z1", "100", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "z2", "28.56,57.12",
            ZiggyDataType.ZIGGY_FLOAT, false));
        assertTrue(checkParameterValues(parametersByName, "z3", "some text",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set has no changed parameters.
        parameters = nameToParameterSet.get("ISOFIT algorithm parameters").getParameters();
        assertEquals(3, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parametersByName, "n_cores", "4", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parametersByName, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // The multiple subtask configuration has no changed parameters.
        parameters = nameToParameterSet.get("Multiple subtask configuration").getParameters();
        assertEquals(5, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(checkParameterValues(parametersByName, "taskDirectoryRegex", "set-([0-9]{1})",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkParameterValues(parametersByName, "reprocessingTasksExclude", "0",
            ZiggyDataType.ZIGGY_INT, false));
        assertTrue(checkParameterValues(parametersByName, "singleSubtask", "false",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parametersByName, "maxFailedSubtaskCount", "0",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "reprocess", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
    }

    @Test
    public void importReplacementParameterSetsFromFile() {

        // Import the initial parameter library and persist to the database
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Import the replacement parameter library and persist to the database
        List<ParameterSet> parameterSetsFromFile = unmarshalParameterSets(
            TEST_DATA.resolve("pl-replacement-param-sets.xml"));
        actualResults = parameterSetImportConditioner.parameterSetDescriptors(parameterSetsFromFile,
            true);
        pipelineImportOperations.persistParameterSets(actualResults);

        assertEquals(4, actualResults.size());

        // Check the descriptor states
        Map<String, ParameterSetDescriptor> nameToDescriptor = parameterSetDescriptorByName(
            actualResults);
        assertEquals(ParameterSetDescriptor.State.UPDATE,
            nameToDescriptor.get("Sample classless parameter set").getState());
        assertNotNull(nameToDescriptor.get("Sample classless parameter set").getLibraryProps());
        assertNotNull(nameToDescriptor.get("Sample classless parameter set").getFileProps());
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("ISOFIT algorithm parameters").getState());
        assertNotNull(nameToDescriptor.get("ISOFIT algorithm parameters").getLibraryProps());
        assertNotNull(nameToDescriptor.get("ISOFIT algorithm parameters").getFileProps());
        assertEquals(ParameterSetDescriptor.State.CREATE,
            nameToDescriptor.get("All-new parameters").getState());
        assertNull(nameToDescriptor.get("All-new parameters").getLibraryProps());
        assertNotNull(nameToDescriptor.get("All-new parameters").getFileProps());
        assertEquals(ParameterSetDescriptor.State.LIBRARY_ONLY,
            nameToDescriptor.get("Multiple subtask configuration").getState());
        assertNotNull(nameToDescriptor.get("Multiple subtask configuration").getLibraryProps());
        assertNull(nameToDescriptor.get("Multiple subtask configuration").getFileProps());

        // Retrieve the parameter sets from the database and check their values
        List<ParameterSet> parameterSets = parametersOperations.parameterSets();
        assertEquals(4, parameterSets.size());
        Map<String, ParameterSet> parameterSetsByName = ParameterSet
            .parameterSetByName(parameterSets);

        // The sample classless parameter set now has only 1 parameter
        Set<Parameter> parameters = parameterSetsByName.get("Sample classless parameter set")
            .getParameters();
        assertEquals(1, parameters.size());
        Map<String, Parameter> parametersByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parametersByName, "z1", "100", ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set has no changes
        parameters = parameterSetsByName.get("ISOFIT algorithm parameters").getParameters();
        assertEquals(3, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parametersByName, "n_cores", "4", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parametersByName, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // All-new parameters has 1 parameter
        parameters = parameterSetsByName.get("All-new parameters").getParameters();
        assertEquals(1, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(checkParameterValues(parametersByName, "parameter", "4", ZiggyDataType.ZIGGY_INT,
            true));

        // The TaskConfigurationParameters set has no changes.
        parameters = parameterSetsByName.get("Multiple subtask configuration").getParameters();
        assertEquals(5, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(checkParameterValues(parametersByName, "taskDirectoryRegex", "set-([0-9]{1})",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkParameterValues(parametersByName, "reprocessingTasksExclude", "0",
            ZiggyDataType.ZIGGY_INT, false));
        assertTrue(checkParameterValues(parametersByName, "singleSubtask", "false",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parametersByName, "maxFailedSubtaskCount", "0",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "reprocess", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
    }

    @Test
    public void testReplacementParameterSetsNoUpdate() {

        // Import the initial parameter library and persist to the database
        List<ParameterSetDescriptor> actualResults = parameterSetImportConditioner
            .parameterSetDescriptors(unmarshalParameterSets(TEST_DATA.resolve("test.xml")), true);
        pipelineImportOperations.persistParameterSets(actualResults);

        // Import the replacement parameter library and persist to the database
        actualResults = parameterSetImportConditioner.parameterSetDescriptors(
            unmarshalParameterSets(TEST_DATA.resolve("pl-replacement-param-sets.xml")), false);
        pipelineImportOperations.persistParameterSets(actualResults);

        assertEquals(4, actualResults.size());

        // Check the descriptor states
        Map<String, ParameterSetDescriptor> nameToDescriptor = parameterSetDescriptorByName(
            actualResults);
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("Sample classless parameter set").getState());
        assertNotNull(nameToDescriptor.get("Sample classless parameter set").getLibraryProps());
        assertNotNull(nameToDescriptor.get("Sample classless parameter set").getFileProps());
        assertEquals(ParameterSetDescriptor.State.SAME,
            nameToDescriptor.get("ISOFIT algorithm parameters").getState());
        assertNotNull(nameToDescriptor.get("ISOFIT algorithm parameters").getLibraryProps());
        assertNotNull(nameToDescriptor.get("ISOFIT algorithm parameters").getFileProps());
        assertEquals(ParameterSetDescriptor.State.CREATE,
            nameToDescriptor.get("All-new parameters").getState());
        assertNull(nameToDescriptor.get("All-new parameters").getLibraryProps());
        assertNotNull(nameToDescriptor.get("All-new parameters").getFileProps());
        assertEquals(ParameterSetDescriptor.State.LIBRARY_ONLY,
            nameToDescriptor.get("Multiple subtask configuration").getState());
        assertNotNull(nameToDescriptor.get("Multiple subtask configuration").getLibraryProps());
        assertNull(nameToDescriptor.get("Multiple subtask configuration").getFileProps());

        // Retrieve the parameter sets from the database and check their values
        List<ParameterSet> parameterSets = parametersOperations.parameterSets();
        assertEquals(4, parameterSets.size());
        Map<String, ParameterSet> parameterSetsByName = ParameterSet
            .parameterSetByName(parameterSets);

        // The sample classless parameter set has no changed parameters
        Set<Parameter> parameters = parameterSetsByName.get("Sample classless parameter set")
            .getParameters();
        assertEquals(3, parameters.size());
        Map<String, Parameter> parametersByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parametersByName, "z1", "100", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "z2", "28.56,57.12",
            ZiggyDataType.ZIGGY_FLOAT, false));
        assertTrue(checkParameterValues(parametersByName, "z3", "some text",
            ZiggyDataType.ZIGGY_STRING, true));

        // ISOFIT classless parameter set has no changed parameters.
        parameters = parameterSetsByName.get("ISOFIT algorithm parameters").getParameters();
        assertEquals(3, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(
            checkParameterValues(parametersByName, "n_cores", "4", ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "presolve", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parametersByName, "empirical_line", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // The multiple subtask configuration has no changed parameters.
        parameters = parameterSetsByName.get("Multiple subtask configuration").getParameters();
        assertEquals(5, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(checkParameterValues(parametersByName, "taskDirectoryRegex", "set-([0-9]{1})",
            ZiggyDataType.ZIGGY_STRING, true));
        assertTrue(checkParameterValues(parametersByName, "reprocessingTasksExclude", "0",
            ZiggyDataType.ZIGGY_INT, false));
        assertTrue(checkParameterValues(parametersByName, "singleSubtask", "false",
            ZiggyDataType.ZIGGY_BOOLEAN, true));
        assertTrue(checkParameterValues(parametersByName, "maxFailedSubtaskCount", "0",
            ZiggyDataType.ZIGGY_INT, true));
        assertTrue(checkParameterValues(parametersByName, "reprocess", "true",
            ZiggyDataType.ZIGGY_BOOLEAN, true));

        // All-new parameters has 1 parameter
        parameters = parameterSetsByName.get("All-new parameters").getParameters();
        assertEquals(1, parameters.size());
        parametersByName = parameterByName(parameters);
        assertTrue(checkParameterValues(parametersByName, "parameter", "4", ZiggyDataType.ZIGGY_INT,
            true));
    }

    private class TestOperations extends DatabaseOperations {

        public void remove(ParameterSet parameterSet) {
            performTransaction(() -> new ParameterSetCrud().remove(parameterSet));
        }
    }
}
