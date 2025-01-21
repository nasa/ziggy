package gov.nasa.ziggy.pipeline.definition;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HOME_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreConfigurationImporter;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterImportExportOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Implements unit tests for the {@link PipelineDefinitionImporter} class. Because of the complexity
 * of that class, unit tests are very spare.
 */
public class PipelineDefinitionImporterTest {

    private Path pipelineDefsDir = ZiggyUnitTestUtils.TEST_DATA
        .resolve(PipelineDefinitionImporter.class.getSimpleName());
    private File pipelineDefinitionFile;
    private File parameterSetFile;
    private File dataTypeDefinitionFile;
    private File pipelineUpdateFile;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Rule
    public ZiggyPropertyRule pipelineHomeDirPropertyRule = new ZiggyPropertyRule(PIPELINE_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Before
    public void setUp() {
        pipelineDefinitionFile = TEST_DATA.resolve("configuration")
            .resolve("pd-hyperion.xml")
            .toFile();
        pipelineUpdateFile = TEST_DATA.resolve("configuration")
            .resolve("pd-hyperion-update.xml")
            .toFile();
        dataTypeDefinitionFile = TEST_DATA.resolve("configuration")
            .resolve("pt-hyperion.xml")
            .toFile();
        parameterSetFile = TEST_DATA.resolve("paramlib").resolve("pl-hyperion.xml").toFile();
    }

    @Test(expected = PipelineException.class)
    public void testPipelineDefinitionImportWithoutParameterSets() {
        new PipelineDefinitionImporter()
            .importPipelineConfiguration(List.of(pipelineDefinitionFile));
    }

    @Test(expected = PipelineException.class)
    public void testPipelineDefinitionImporterWithoutDataTypes() {
        new ParameterImportExportOperations().importParameterLibrary(parameterSetFile, null,
            ParamIoMode.STANDARD);
        new PipelineDefinitionImporter()
            .importPipelineConfiguration(List.of(pipelineDefinitionFile));
    }

    @Test
    public void testPipelineDefinitionImporter() {
        new ParameterImportExportOperations().importParameterLibrary(parameterSetFile, null,
            ParamIoMode.STANDARD);
        new DatastoreConfigurationImporter(List.of(dataTypeDefinitionFile.getAbsolutePath()), false)
            .importConfiguration();
        new PipelineDefinitionImporter()
            .importPipelineConfiguration(List.of(pipelineDefinitionFile));
        List<PipelineDefinition> pipelineDefinitions = new PipelineDefinitionOperations()
            .allPipelineDefinitions();
        assertEquals(1, pipelineDefinitions.size());
        PipelineDefinition pipelineDefinition = pipelineDefinitions.get(0);
        assertEquals("hyperion", pipelineDefinition.getName());
        verifyHyperionPipeline(pipelineDefinition);
        List<PipelineModuleDefinition> moduleDefinitions = new PipelineModuleDefinitionOperations()
            .allPipelineModuleDefinitions();
        assertEquals(2, moduleDefinitions.size());
        Set<String> moduleNames = moduleDefinitions.stream()
            .map(PipelineModuleDefinition::getName)
            .collect(Collectors.toSet());
        assertTrue(moduleNames.contains("level0"));
        assertTrue(moduleNames.contains("level1"));
        verifyHyperionModules(moduleDefinitions);
    }

    @Test
    public void testDoNotUpdateDefinitions() {
        PipelineDefinitionOperations pipelineOps = new PipelineDefinitionOperations();
        PipelineModuleDefinitionOperations moduleOps = new PipelineModuleDefinitionOperations();
        new ParameterImportExportOperations().importParameterLibrary(parameterSetFile, null,
            ParamIoMode.STANDARD);
        new DatastoreConfigurationImporter(List.of(dataTypeDefinitionFile.getAbsolutePath()), false)
            .importConfiguration();
        new PipelineDefinitionImporter()
            .importPipelineConfiguration(List.of(pipelineDefinitionFile));

        // Now we import the file with both new and updated definitions.
        new PipelineDefinitionImporter().importPipelineConfiguration(List.of(pipelineUpdateFile));
        List<PipelineDefinition> pipelineDefinitions = pipelineOps.allPipelineDefinitions();
        assertEquals(2, pipelineDefinitions.size());
        Map<String, PipelineDefinition> definitionByName = new HashMap<>();
        for (PipelineDefinition pipelineDefinition : pipelineDefinitions) {
            definitionByName.put(pipelineDefinition.getName(), pipelineDefinition);
        }
        PipelineDefinition definition = definitionByName.get("hyperion");
        verifyHyperionPipeline(definition);
        assertFalse(definition.isLocked());
        assertEquals(0, definition.getVersion());
        definition = definitionByName.get("genuinely-new");
        verifyGenuinelyNewPipeline(definition);

        List<PipelineModuleDefinition> moduleDefinitions = moduleOps.allPipelineModuleDefinitions();
        assertEquals(3, moduleDefinitions.size());
        Map<String, PipelineModuleDefinition> moduleByName = new HashMap<>();
        for (PipelineModuleDefinition moduleDefinition : moduleDefinitions) {
            moduleByName.put(moduleDefinition.getName(), moduleDefinition);
        }
        assertTrue(moduleByName.keySet().contains("level0"));
        assertTrue(moduleByName.keySet().contains("level1"));
        assertTrue(moduleByName.keySet().contains("level3"));
        verifyHyperionModules(List.of(moduleByName.get("level0"), moduleByName.get("level1")));
        verifyLevel3Module(moduleByName.get("level3"));
        for (PipelineModuleDefinition module : moduleByName.values()) {
            assertEquals(0, module.getVersion());
            assertFalse(module.isLocked());
        }
    }

    /**
     * Exercises update of module and pipeline definitions when all database instances are unlocked.
     */
    @Test
    public void testUpdateUnlockedDefinitions() {
        PipelineDefinitionOperations pipelineOps = new PipelineDefinitionOperations();
        PipelineModuleDefinitionOperations moduleOps = new PipelineModuleDefinitionOperations();
        new ParameterImportExportOperations().importParameterLibrary(parameterSetFile, null,
            ParamIoMode.STANDARD);
        new DatastoreConfigurationImporter(List.of(dataTypeDefinitionFile.getAbsolutePath()), false)
            .importConfiguration();
        new PipelineDefinitionImporter()
            .importPipelineConfiguration(List.of(pipelineDefinitionFile));

        // Now we import the file with both new and updated definitions.
        PipelineDefinitionImporter importer = new PipelineDefinitionImporter();
        importer.setUpdate(true);
        importer.importPipelineConfiguration(List.of(pipelineUpdateFile));

        List<PipelineDefinition> pipelineDefinitions = pipelineOps.allPipelineDefinitions();
        assertEquals(2, pipelineDefinitions.size());
        Map<String, PipelineDefinition> definitionByName = new HashMap<>();
        for (PipelineDefinition pipelineDefinition : pipelineDefinitions) {
            definitionByName.put(pipelineDefinition.getName(), pipelineDefinition);
        }
        PipelineDefinition definition = definitionByName.get("hyperion");
        verifyUpdatedHyperionPipeline(definition);
        assertFalse(definition.isLocked());
        assertEquals(0, definition.getVersion());
        definition = definitionByName.get("genuinely-new");
        verifyGenuinelyNewPipeline(definition);
        assertFalse(definition.isLocked());
        assertEquals(0, definition.getVersion());

        List<PipelineDefinitionNode> pipelineDefinitionNodes = new PipelineDefinitionOperations()
            .rootNodes(definition);
        PipelineDefinitionNode pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level0", pipelineDefinitionNode.getModuleName());
        assertEquals("genuinely-new", pipelineDefinitionNode.getPipelineName());
        assertEquals(1, pipelineDefinitionNodes.size());

        definition = definitionByName.get("hyperion");
        pipelineDefinitionNodes = new PipelineDefinitionOperations().rootNodes(definition);
        pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level1", pipelineDefinitionNode.getModuleName());
        assertEquals("hyperion", pipelineDefinitionNode.getPipelineName());
        assertEquals(1, pipelineDefinitionNodes.size());

        List<PipelineModuleDefinition> moduleDefinitions = moduleOps.allPipelineModuleDefinitions();
        assertEquals(3, moduleDefinitions.size());
        Map<String, PipelineModuleDefinition> moduleByName = new HashMap<>();
        for (PipelineModuleDefinition moduleDefinition : moduleDefinitions) {
            moduleByName.put(moduleDefinition.getName(), moduleDefinition);
        }
        assertTrue(moduleByName.keySet().contains("level0"));
        assertTrue(moduleByName.keySet().contains("level1"));
        assertTrue(moduleByName.keySet().contains("level3"));
        verifyHyperionModules(List.of(moduleByName.get("level1")));
        verifyLevel3Module(moduleByName.get("level3"));
        verifyUpdatedLevel0Module(moduleByName.get("level0"));
        for (PipelineModuleDefinition module : moduleByName.values()) {
            assertEquals(0, module.getVersion());
            assertFalse(module.isLocked());
        }
    }

    /**
     * Exercises update of module and pipeline definitions when all database instances are unlocked.
     */
    @Test
    public void testUpdateLockedDefinitions() {
        PipelineDefinitionOperations pipelineOps = new PipelineDefinitionOperations();
        PipelineModuleDefinitionOperations moduleOps = new PipelineModuleDefinitionOperations();
        new ParameterImportExportOperations().importParameterLibrary(parameterSetFile, null,
            ParamIoMode.STANDARD);
        new DatastoreConfigurationImporter(List.of(dataTypeDefinitionFile.getAbsolutePath()), false)
            .importConfiguration();
        new PipelineDefinitionImporter()
            .importPipelineConfiguration(List.of(pipelineDefinitionFile));

        // Lock everything.
        List<PipelineDefinition> pipelineDefinitions = pipelineOps.allPipelineDefinitions();
        pipelineOps.lock(pipelineDefinitions.get(0));
        List<PipelineModuleDefinition> moduleDefinitions = moduleOps.allPipelineModuleDefinitions();
        for (PipelineModuleDefinition moduleDefinition : moduleDefinitions) {
            moduleOps.lock(moduleDefinition);
        }

        // Now we import the file with both new and updated definitions.
        PipelineDefinitionImporter importer = new PipelineDefinitionImporter();
        importer.setUpdate(true);
        importer.importPipelineConfiguration(List.of(pipelineUpdateFile));

        pipelineDefinitions = pipelineOps.allPipelineDefinitions();
        assertEquals(2, pipelineDefinitions.size());
        Map<String, PipelineDefinition> definitionByName = new HashMap<>();
        for (PipelineDefinition pipelineDefinition : pipelineDefinitions) {
            definitionByName.put(pipelineDefinition.getName(), pipelineDefinition);
        }
        PipelineDefinition definition = definitionByName.get("hyperion");
        verifyUpdatedHyperionPipeline(definition);
        assertFalse(definition.isLocked());
        assertEquals(1, definition.getVersion());
        definition = definitionByName.get("genuinely-new");
        verifyGenuinelyNewPipeline(definition);
        assertFalse(definition.isLocked());
        assertEquals(0, definition.getVersion());

        List<PipelineDefinitionNode> pipelineDefinitionNodes = new PipelineDefinitionOperations()
            .rootNodes(definition);
        PipelineDefinitionNode pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level0", pipelineDefinitionNode.getModuleName());
        assertEquals("genuinely-new", pipelineDefinitionNode.getPipelineName());
        assertEquals(1, pipelineDefinitionNodes.size());

        definition = definitionByName.get("hyperion");
        pipelineDefinitionNodes = new PipelineDefinitionOperations().rootNodes(definition);
        pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level1", pipelineDefinitionNode.getModuleName());
        assertEquals("hyperion", pipelineDefinitionNode.getPipelineName());
        assertEquals(1, pipelineDefinitionNodes.size());

        moduleDefinitions = moduleOps.pipelineModuleDefinitions();
        assertEquals(3, moduleDefinitions.size());
        Map<String, PipelineModuleDefinition> moduleByName = new HashMap<>();
        for (PipelineModuleDefinition moduleDefinition : moduleDefinitions) {
            moduleByName.put(moduleDefinition.getName(), moduleDefinition);
        }
        assertTrue(moduleByName.keySet().contains("level0"));
        assertTrue(moduleByName.keySet().contains("level1"));
        assertTrue(moduleByName.keySet().contains("level3"));
        verifyHyperionModules(List.of(moduleByName.get("level1")));
        verifyLevel3Module(moduleByName.get("level3"));
        verifyUpdatedLevel0Module(moduleByName.get("level0"));
        assertFalse(moduleByName.get("level0").isLocked());
        assertEquals(1, moduleByName.get("level0").getVersion());
        assertTrue(moduleByName.get("level1").isLocked());
        assertEquals(0, moduleByName.get("level1").getVersion());
        assertFalse(moduleByName.get("level3").isLocked());
        assertEquals(0, moduleByName.get("level3").getVersion());
    }

    @Test
    public void testMultipleDefaultParamSets() throws Exception {

        // Use the PipelineSupervisor constructor to set the worker count.
        new PipelineSupervisor(1, 1000);

        // Read in the parameter library
        new ParameterImportExportOperations().importParameterLibrary(
            new File(pipelineDefsDir.toFile(), "pl-four-default-param-sets.xml"), null,
            ParamIoMode.STANDARD);

        // Read in the pipeline definition that has 4 instances of Parameters
        // attached to it.
        new PipelineDefinitionImporter().importPipelineConfiguration(
            Sets.newHashSet(new File(pipelineDefsDir.toFile(), "pd-four-default-param-sets.xml")));

        // Retrieve the pipeline definition
        PipelineDefinition pipelineDef = new PipelineDefinitionOperations()
            .pipelineDefinition("hyperion");

        // Check the contents of the parameter set map.
        Set<String> parameterSetNames = new PipelineDefinitionOperations()
            .parameterSetNames(pipelineDef);
        pipelineDef.getParameterSetNames();
        assertTrue(parameterSetNames.contains("Sample classless parameter set"));
        assertTrue(parameterSetNames.contains("ISOFIT module parameters"));
        assertEquals(2, parameterSetNames.size());

        List<PipelineDefinitionNode> rootNodes = new PipelineDefinitionOperations()
            .rootNodes(pipelineDef);
        parameterSetNames = new PipelineDefinitionNodeOperations()
            .parameterSetNames(rootNodes.get(0));
        assertTrue(parameterSetNames.contains("another darn parameter set!"));
        assertTrue(parameterSetNames.contains("yet another one..."));
        assertEquals(2, parameterSetNames.size());

        // Create a pipeline instance for this pipeline
        PipelineInstance pipelineInstance = new PipelineExecutor().launch(pipelineDef,
            "instance-name", null, null, null);

        Map<String, ParameterSet> parameterSetsByName = ParameterSet
            .parameterSetByName(new PipelineInstanceOperations().parameterSets(pipelineInstance));
        assertEquals(2, parameterSetsByName.size());

        // Check the values in the parameter sets
        checkParameterSetValues(parameterSetsByName);
    }

    private void checkParameterSetValues(Map<String, ParameterSet> parameterSetsByName) {
        ParameterSet parameterSet = parameterSetsByName.get("Sample classless parameter set");
        assertNotNull(parameterSet);
        Map<String, Parameter> parametersByName = parameterSet.parameterByName();

        Parameter parameter = parametersByName.get("z1");
        assertNotNull(parameter);
        assertEquals("100", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_INT, parameter.getDataType());
        assertTrue(parameter.isScalar());

        parameter = parametersByName.get("z2");
        assertNotNull(parameter);
        assertEquals("28.56,57.12", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_FLOAT, parameter.getDataType());
        assertFalse(parameter.isScalar());

        parameter = parametersByName.get("z3");
        assertNotNull(parameter);
        assertEquals("some text", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_STRING, parameter.getDataType());
        assertTrue(parameter.isScalar());

        assertEquals(3, parametersByName.size());

        parameterSet = parameterSetsByName.get("ISOFIT module parameters");
        assertNotNull(parameterSet);
        parametersByName = parameterSet.parameterByName();

        parameter = parametersByName.get("n_cores");
        assertNotNull(parameter);
        assertEquals("4", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_INT, parameter.getDataType());
        assertTrue(parameter.isScalar());

        parameter = parametersByName.get("use_hyperthreading");
        assertNotNull(parameter);
        assertEquals("false", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_BOOLEAN, parameter.getDataType());
        assertTrue(parameter.isScalar());

        parameter = parametersByName.get("presolve");
        assertNotNull(parameter);
        assertEquals("true", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_BOOLEAN, parameter.getDataType());
        assertTrue(parameter.isScalar());

        parameter = parametersByName.get("empirical_line");
        assertNotNull(parameter);
        assertEquals("true", parameter.getString());
        assertEquals(ZiggyDataType.ZIGGY_BOOLEAN, parameter.getDataType());
        assertTrue(parameter.isScalar());

        assertEquals(4, parametersByName.size());
        assertEquals(2, parameterSetsByName.size());
    }

    @Test(expected = PipelineException.class)
    public void testBadPipelineParameterSetName() {
        // Read in the parameter library
        new ParameterImportExportOperations().importParameterLibrary(
            new File(pipelineDefsDir.toFile(), "pl-four-default-param-sets.xml"), null,
            ParamIoMode.STANDARD);

        // Read in the pipeline definition that has 4 instances of Parameters
        // attached to it, with one of the pipeline parameter sets not in the database.
        new PipelineDefinitionImporter().importPipelineConfiguration(
            Sets.newHashSet(new File(pipelineDefsDir.toFile(), "pd-bad-pipeline-param-sets.xml")));
    }

    @Test(expected = PipelineException.class)
    public void testBadModuleParameterSetName() {
        // Read in the parameter library
        new ParameterImportExportOperations().importParameterLibrary(
            new File(pipelineDefsDir.toFile(), "pl-four-default-param-sets.xml"), null,
            ParamIoMode.STANDARD);

        // Read in the pipeline definition that has 4 instances of Parameters
        // attached to it, with one of the module parameter sets not in the database.
        new PipelineDefinitionImporter().importPipelineConfiguration(
            Sets.newHashSet(new File(pipelineDefsDir.toFile(), "pd-bad-module-param-sets.xml")));
    }

    /**
     * Tests that whitespace is allowed around the list element delimiter.
     */
    @Test
    public void testSplitList() {
        verifyParsing("1", new String[] { "1" });
        verifyParsing("1,2", new String[] { "1", "2" });
        verifyParsing("1 ,2", new String[] { "1", "2" });
        verifyParsing("1, 2", new String[] { "1", "2" });
        verifyParsing("1  ,  2", new String[] { "1", "2" });
    }

    private void verifyParsing(String value, String[] elements) {
        List<String> expected = ImmutableList.copyOf(elements);
        List<String> actual = ZiggyStringUtils.splitStringAtCommas(value);
        assertEquals(expected, actual);
    }

    private void verifyHyperionPipeline(PipelineDefinition pipelineDefinition) {
        PipelineDefinitionNodeOperations nodeOperations = new PipelineDefinitionNodeOperations();
        List<PipelineDefinitionNode> pipelineDefinitionNodes = pipelineDefinition.getRootNodes();
        assertEquals(1, pipelineDefinitionNodes.size());
        PipelineDefinitionNode pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level0", pipelineDefinitionNode.getModuleName());
        Set<DataFileType> inputDataFileTypes = nodeOperations
            .inputDataFileTypes(pipelineDefinitionNode);
        assertEquals(1, inputDataFileTypes.size());
        DataFileType inputDataFileType = inputDataFileTypes.iterator().next();
        assertEquals("Hyperion L0", inputDataFileType.getName());
        Set<DataFileType> outputDataFileTypes = nodeOperations
            .outputDataFileTypes(pipelineDefinitionNode);
        assertEquals(1, outputDataFileTypes.size());
        DataFileType outputDataFileType = outputDataFileTypes.iterator().next();
        assertEquals("Hyperion L1R", outputDataFileType.getName());
        Set<String> parameterSetNames = nodeOperations.parameterSetNames(pipelineDefinitionNode);
        assertTrue(parameterSetNames.contains("New Remote Hyperion L0"));
        assertTrue(parameterSetNames.contains("Hyperion L0 Task Configuration"));
        assertTrue(parameterSetNames.contains("Sample classless parameter set"));
        assertEquals(3, parameterSetNames.size());
        Set<ModelType> modelTypes = nodeOperations.modelTypes(pipelineDefinitionNode);
        Set<String> modelTypeNames = modelTypes.stream()
            .map(ModelType::getType)
            .collect(Collectors.toSet());
        assertTrue(modelTypeNames.contains("bandwidth"));
        assertTrue(modelTypeNames.contains("template"));
        assertTrue(modelTypeNames.contains("gain"));
        assertTrue(modelTypeNames.contains("ratio"));
        assertTrue(modelTypeNames.contains("metadata-updates"));
        assertTrue(modelTypeNames.contains("spectra"));
        assertTrue(modelTypeNames.contains("L0 attributes"));
        assertEquals(7, modelTypeNames.size());

        pipelineDefinitionNodes = pipelineDefinitionNode.getNextNodes();
        assertEquals(1, pipelineDefinitionNodes.size());
        pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level1", pipelineDefinitionNode.getModuleName());
        inputDataFileTypes = nodeOperations.inputDataFileTypes(pipelineDefinitionNode);
        assertEquals(1, inputDataFileTypes.size());
        inputDataFileType = inputDataFileTypes.iterator().next();
        assertEquals("Hyperion L1R", inputDataFileType.getName());
        outputDataFileTypes = nodeOperations.outputDataFileTypes(pipelineDefinitionNode);
        assertEquals(1, outputDataFileTypes.size());
        outputDataFileType = outputDataFileTypes.iterator().next();
        assertEquals("Hyperion L2", outputDataFileType.getName());
        parameterSetNames = nodeOperations.parameterSetNames(pipelineDefinitionNode);
        assertTrue(parameterSetNames.contains("Hyperion L0 Task Configuration"));
        assertTrue(parameterSetNames.contains("ISOFIT module parameters"));
        assertTrue(parameterSetNames.contains("Remote Hyperion L1"));
        assertEquals(3, parameterSetNames.size());
        modelTypes = nodeOperations.modelTypes(pipelineDefinitionNode);
        modelTypeNames = modelTypes.stream().map(ModelType::getType).collect(Collectors.toSet());
        assertTrue(modelTypeNames.contains("sRTM neural network"));
        assertTrue(modelTypeNames.contains("surface"));
        assertTrue(modelTypeNames.contains("dem"));
        assertEquals(3, modelTypeNames.size());

        assertTrue(CollectionUtils.isEmpty(pipelineDefinitionNode.getNextNodes()));
    }

    private void verifyHyperionModules(Collection<PipelineModuleDefinition> moduleDefinitions) {
        PipelineModuleDefinitionOperations moduleOperations = new PipelineModuleDefinitionOperations();
        for (PipelineModuleDefinition module : moduleDefinitions) {
            PipelineModuleExecutionResources resources = moduleOperations
                .pipelineModuleExecutionResources(module);
            assertEquals(2000000, resources.getExeTimeoutSeconds());
            assertEquals(0, resources.getMinMemoryMegabytes());
        }
    }

    private void verifyGenuinelyNewPipeline(PipelineDefinition pipelineDefinition) {
        PipelineDefinitionNodeOperations nodeOperations = new PipelineDefinitionNodeOperations();
        List<PipelineDefinitionNode> pipelineDefinitionNodes = pipelineDefinition.getRootNodes();
        assertEquals(1, pipelineDefinitionNodes.size());
        PipelineDefinitionNode pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level0", pipelineDefinitionNode.getModuleName());
        Set<DataFileType> inputDataFileTypes = nodeOperations
            .inputDataFileTypes(pipelineDefinitionNode);
        assertEquals(1, inputDataFileTypes.size());
        DataFileType inputDataFileType = inputDataFileTypes.iterator().next();
        assertEquals("Hyperion L2", inputDataFileType.getName());
        assertTrue(
            CollectionUtils.isEmpty(nodeOperations.outputDataFileTypes(pipelineDefinitionNode)));
        assertTrue(CollectionUtils.isEmpty(nodeOperations.modelTypes(pipelineDefinitionNode)));
        Set<String> parameterSetNames = nodeOperations.parameterSetNames(pipelineDefinitionNode);
        assertEquals(1, parameterSetNames.size());
        assertEquals("New Remote Hyperion L0", parameterSetNames.iterator().next());
        assertTrue(CollectionUtils.isEmpty(pipelineDefinitionNode.getNextNodes()));
    }

    private void verifyLevel3Module(PipelineModuleDefinition module) {
        PipelineModuleExecutionResources resources = new PipelineModuleDefinitionOperations()
            .pipelineModuleExecutionResources(module);
        assertEquals(1000000, resources.getExeTimeoutSeconds());
        assertEquals(10, resources.getMinMemoryMegabytes());
    }

    private void verifyUpdatedHyperionPipeline(PipelineDefinition pipelineDefinition) {
        PipelineDefinitionNodeOperations nodeOperations = new PipelineDefinitionNodeOperations();
        List<PipelineDefinitionNode> pipelineDefinitionNodes = pipelineDefinition.getRootNodes();
        assertEquals(1, pipelineDefinitionNodes.size());
        PipelineDefinitionNode pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level1", pipelineDefinitionNode.getModuleName());
        assertTrue(
            CollectionUtils.isEmpty(nodeOperations.inputDataFileTypes(pipelineDefinitionNode)));
        assertTrue(
            CollectionUtils.isEmpty(nodeOperations.outputDataFileTypes(pipelineDefinitionNode)));
        Set<ModelType> modelTypes = nodeOperations.modelTypes(pipelineDefinitionNode);
        assertEquals(1, modelTypes.size());
        assertEquals("bandwidth", modelTypes.iterator().next().getType());
        Set<String> parameterSetNames = nodeOperations.parameterSetNames(pipelineDefinitionNode);
        assertEquals(1, parameterSetNames.size());
        assertEquals("Hyperion L0 Task Configuration", parameterSetNames.iterator().next());

        pipelineDefinitionNodes = pipelineDefinitionNode.getNextNodes();
        assertEquals(1, pipelineDefinitionNodes.size());
        pipelineDefinitionNode = pipelineDefinitionNodes.get(0);
        assertEquals("level3", pipelineDefinitionNode.getModuleName());
        assertTrue(
            CollectionUtils.isEmpty(nodeOperations.inputDataFileTypes(pipelineDefinitionNode)));
        assertTrue(
            CollectionUtils.isEmpty(nodeOperations.outputDataFileTypes(pipelineDefinitionNode)));
        assertTrue(CollectionUtils.isEmpty(nodeOperations.modelTypes(pipelineDefinitionNode)));
        assertTrue(
            CollectionUtils.isEmpty(nodeOperations.parameterSetNames(pipelineDefinitionNode)));

        assertTrue(CollectionUtils.isEmpty(pipelineDefinitionNode.getNextNodes()));
    }

    private void verifyUpdatedLevel0Module(PipelineModuleDefinition module) {
        PipelineModuleExecutionResources resources = new PipelineModuleDefinitionOperations()
            .pipelineModuleExecutionResources(module);
        assertEquals(200000, resources.getExeTimeoutSeconds());
        assertEquals(1, resources.getMinMemoryMegabytes());
    }
}
