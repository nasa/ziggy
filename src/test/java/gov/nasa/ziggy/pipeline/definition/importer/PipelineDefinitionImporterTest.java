package gov.nasa.ziggy.pipeline.definition.importer;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HOME_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreNode;
import gov.nasa.ziggy.data.datastore.DatastoreOperations;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.remote.Architecture;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueue;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironmentOperations;
import gov.nasa.ziggy.pipeline.step.remote.batch.SupportedBatchSystem;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventOperations;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.ZiggyStringUtils;
import gov.nasa.ziggy.worker.WorkerResources;
import gov.nasa.ziggy.worker.WorkerResourcesOperations;

/**
 * Implements unit tests for the {@link PipelineDefinitionImporter} class. Because of the complexity
 * of that class, unit tests are very spare.
 */
public class PipelineDefinitionImporterTest {

    private Path pipelineDefinitionFile;
    private Path parameterSetFile;
    private Path dataTypeDefinitionFile;
    private Path pipelineUpdateFile;
    private Path pipelineDefinitionOnlyFile;
    private Path dataReceiptTestFile;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Rule
    public ZiggyPropertyRule pipelineHomeDirPropertyRule = new ZiggyPropertyRule(PIPELINE_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();
    public ZiggyPropertyRule datstorePropertyRule = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR, ziggyDirectoryRule, "datastore");

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(ziggyDirectoryRule)
        .around(datstorePropertyRule);

    @Before
    public void setUp() {
        pipelineDefinitionFile = TEST_DATA.resolve("pd-hyperion.xml");
        pipelineUpdateFile = TEST_DATA.resolve("pd-hyperion-update.xml");
        dataTypeDefinitionFile = TEST_DATA.resolve("pt-hyperion.xml");
        parameterSetFile = TEST_DATA.resolve("pl-hyperion.xml");
        pipelineDefinitionOnlyFile = TEST_DATA.resolve("pd-hyperion-pipeline-only.xml");
        WorkerResources workerResources = new WorkerResources(1, 1);
        workerResources.setDefaultInstance(true);
        new WorkerResourcesOperations().merge(workerResources);
        dataReceiptTestFile = TEST_DATA.resolve("data-receipt-test.xml");
    }

    @Test(expected = PipelineException.class)
    public void testPipelineDefinitionImporterWithoutParameterSets() {
        new PipelineDefinitionImporter(List.of(pipelineDefinitionOnlyFile, dataTypeDefinitionFile))
            .importPipelineDefinitions();
    }

    @Test(expected = PipelineException.class)
    public void testPipelineDefinitionImporterWithoutDataTypes() {
        new PipelineDefinitionImporter(List.of(pipelineDefinitionOnlyFile, parameterSetFile))
            .importPipelineDefinitions();
    }

    @Test
    public void testPipelineDefinitionImporter() {
        new PipelineDefinitionImporter(List.of(pipelineDefinitionFile)).importPipelineDefinitions();

        List<Pipeline> pipelines = new PipelineOperations().allPipelines();
        assertEquals(1, pipelines.size());
        Pipeline pipeline = pipelines.get(0);
        assertEquals("hyperion", pipeline.getName());
        verifyHyperionPipeline(pipeline);

        List<PipelineStep> pipelineSteps = new PipelineStepOperations().allPipelineSteps();
        assertEquals(2, pipelineSteps.size());
        Set<String> pipelineStepNames = pipelineSteps.stream()
            .map(PipelineStep::getName)
            .collect(Collectors.toSet());
        assertTrue(pipelineStepNames.contains("level0"));
        assertTrue(pipelineStepNames.contains("level1"));

        verifyParameterSets();

        verifyDatastoreConfiguration();

        verifyDataFileTypes();

        verifyModelTypes();

        verifyRemoteEnvironments();
    }

    @Test
    public void testDoNotUpdateDefinitions() {
        PipelineOperations pipelineOperations = new PipelineOperations();
        PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
        new PipelineDefinitionImporter(List.of(pipelineDefinitionFile)).importPipelineDefinitions();

        // Now we import the file with both new and updated definitions.
        new PipelineDefinitionImporter(List.of(pipelineUpdateFile)).importPipelineDefinitions();
        List<Pipeline> pipelines = pipelineOperations.allPipelines();
        assertEquals(2, pipelines.size());
        Map<String, Pipeline> pipelineByName = new HashMap<>();
        for (Pipeline pipeline : pipelines) {
            pipelineByName.put(pipeline.getName(), pipeline);
        }
        Pipeline pipeline = pipelineByName.get("hyperion");
        verifyHyperionPipeline(pipeline);
        assertFalse(pipeline.isLocked());
        assertEquals(0, pipeline.getVersion());
        pipeline = pipelineByName.get("genuinely-new");
        verifyGenuinelyNewPipeline(pipeline);

        List<PipelineStep> pipelineSteps = pipelineStepOperations.allPipelineSteps();
        assertEquals(3, pipelineSteps.size());
        Map<String, PipelineStep> pipelineStepByName = new HashMap<>();
        for (PipelineStep pipelineStep : pipelineSteps) {
            pipelineStepByName.put(pipelineStep.getName(), pipelineStep);
        }
        assertTrue(pipelineStepByName.containsKey("level0"));
        assertTrue(pipelineStepByName.containsKey("level1"));
        assertTrue(pipelineStepByName.containsKey("level3"));
        for (PipelineStep pipelineStep : pipelineStepByName.values()) {
            assertEquals(0, pipelineStep.getVersion());
            assertFalse(pipelineStep.isLocked());
        }

        Map<String, RemoteEnvironment> remoteEnvironmentByName = new RemoteEnvironmentOperations()
            .remoteEnvironmentByName();
        assertTrue(remoteEnvironmentByName.containsKey("nas"));
        assertTrue(remoteEnvironmentByName.containsKey("bauhaus"));
        assertTrue(remoteEnvironmentByName.containsKey("duranduran"));
        assertEquals(3, remoteEnvironmentByName.size());

        // The nas environment should be unchanged despite the presence of an update
        // in the file.
        RemoteEnvironment environment = remoteEnvironmentByName.get("nas");
        assertEquals("SBU", environment.getCostUnit());
        assertEquals(SupportedBatchSystem.PBS, environment.getBatchSystem());
        Map<String, Architecture> architectureByName = architectureByName(environment);
        assertTrue(architectureByName.containsKey("has"));
        assertTrue(architectureByName.containsKey("bro"));
        assertEquals(2, architectureByName.size());
        assertEquals(0, architectureByName.get("has").getBandwidthGbps(), 1e-6);
        assertEquals(0, architectureByName.get("bro").getBandwidthGbps(), 1e-6);
        Architecture architecture = architectureByName.get("bro");
        assertEquals(28, architecture.getCores());
        assertEquals(128, architecture.getRamGigabytes());
        assertEquals(1.0, architecture.getCost(), 1e-3);
        Map<String, BatchQueue> queueByName = queueByName(environment);
        assertTrue(queueByName.containsKey("debug"));
        assertTrue(queueByName.containsKey("reserved"));
        assertTrue(queueByName.containsKey("normal"));
        assertEquals(3, queueByName.size());
        BatchQueue queue = queueByName.get("debug");
        assertFalse(queue.isAutoSelectable());
        assertFalse(queue.isReserved());
        assertEquals(2, queue.getMaxWallTimeHours(), 1e-3);
        assertEquals(2, queue.getMaxNodes());
        queue = queueByName.get("reserved");
        assertFalse(queue.isAutoSelectable());
        assertTrue(queue.isReserved());
        assertEquals(Float.MAX_VALUE, queue.getMaxWallTimeHours(), 1000);
        assertEquals(Integer.MAX_VALUE, queue.getMaxNodes());
        queue = queueByName.get("normal");
        assertTrue(queue.isAutoSelectable());
        assertFalse(queue.isReserved());
        assertEquals(8, queue.getMaxWallTimeHours(), 1e-3);
        assertEquals(Integer.MAX_VALUE, queue.getMaxNodes());
    }

    /**
     * Exercises update of pipeline definitions when all database instances are unlocked.
     */
    @Test
    public void testUpdateUnlockedPipelines() {
        PipelineOperations pipelineOperations = new PipelineOperations();
        PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
        new PipelineDefinitionImporter(List.of(pipelineDefinitionFile)).importPipelineDefinitions();

        // Now we import the file with both new and updated definitions.
        PipelineDefinitionImporter importer = new PipelineDefinitionImporter(
            List.of(pipelineUpdateFile));
        importer.setUpdate(true);
        importer.importPipelineDefinitions();

        List<Pipeline> pipelines = pipelineOperations.allPipelines();
        assertEquals(2, pipelines.size());
        Map<String, Pipeline> pipelineByName = new HashMap<>();
        for (Pipeline pipeline : pipelines) {
            pipelineByName.put(pipeline.getName(), pipeline);
        }
        Pipeline pipeline = pipelineByName.get("hyperion");
        verifyUpdatedHyperionPipeline(pipeline);
        assertFalse(pipeline.isLocked());
        assertEquals(0, pipeline.getVersion());
        pipeline = pipelineByName.get("genuinely-new");
        verifyGenuinelyNewPipeline(pipeline);
        assertFalse(pipeline.isLocked());
        assertEquals(0, pipeline.getVersion());

        List<PipelineNode> pipelineNodes = new PipelineOperations().rootNodes(pipeline);
        PipelineNode pipelineNode = pipelineNodes.get(0);
        assertEquals("level0", pipelineNode.getPipelineStepName());
        assertEquals("genuinely-new", pipelineNode.getPipelineName());
        assertEquals(1, pipelineNodes.size());

        pipeline = pipelineByName.get("hyperion");
        pipelineNodes = new PipelineOperations().rootNodes(pipeline);
        pipelineNode = pipelineNodes.get(0);
        assertEquals("level1", pipelineNode.getPipelineStepName());
        assertEquals("hyperion", pipelineNode.getPipelineName());
        assertEquals(1, pipelineNodes.size());

        List<PipelineStep> pipelineSteps = pipelineStepOperations.allPipelineSteps();
        assertEquals(3, pipelineSteps.size());
        Map<String, PipelineStep> pipelineStepByName = new HashMap<>();
        for (PipelineStep pipelineStep : pipelineSteps) {
            pipelineStepByName.put(pipelineStep.getName(), pipelineStep);
        }
        assertTrue(pipelineStepByName.containsKey("level0"));
        assertTrue(pipelineStepByName.containsKey("level1"));
        assertTrue(pipelineStepByName.containsKey("level3"));
        for (PipelineStep pipelineStep : pipelineStepByName.values()) {
            assertEquals(0, pipelineStep.getVersion());
            assertFalse(pipelineStep.isLocked());
        }

        Map<String, RemoteEnvironment> remoteEnvironmentByName = new RemoteEnvironmentOperations()
            .remoteEnvironmentByName();
        assertTrue(remoteEnvironmentByName.containsKey("nas"));
        assertTrue(remoteEnvironmentByName.containsKey("bauhaus"));
        assertTrue(remoteEnvironmentByName.containsKey("duranduran"));
        assertEquals(3, remoteEnvironmentByName.size());

        RemoteEnvironment environment = remoteEnvironmentByName.get("nas");
        assertEquals("SBU2", environment.getCostUnit());
        assertEquals(SupportedBatchSystem.PBS, environment.getBatchSystem());
        Map<String, Architecture> architectureByName = architectureByName(environment);
        assertTrue(architectureByName.containsKey("rom_ait"));
        assertTrue(architectureByName.containsKey("bro"));
        assertEquals(2, architectureByName.size());
        assertEquals(0, architectureByName.get("rom_ait").getBandwidthGbps(), 1e-6);
        assertEquals(0, architectureByName.get("bro").getBandwidthGbps(), 1e-6);
        Architecture architecture = architectureByName.get("bro");
        assertEquals(32, architecture.getCores());
        assertEquals(128, architecture.getRamGigabytes());
        assertEquals(1.0, architecture.getCost(), 1e-3);
        architecture = architectureByName.get("rom_ait");
        assertEquals(64, architecture.getCores());
        assertEquals(256, architecture.getRamGigabytes());
        assertEquals(4.5, architecture.getCost(), 1e-3);
        Map<String, BatchQueue> queueByName = queueByName(environment);
        assertTrue(queueByName.containsKey("debug"));
        assertTrue(queueByName.containsKey("normal"));
        assertEquals(2, queueByName.size());
        BatchQueue queue = queueByName.get("debug");
        assertFalse(queue.isAutoSelectable());
        assertFalse(queue.isReserved());
        assertEquals(2, queue.getMaxWallTimeHours(), 1e-3);
        assertEquals(4, queue.getMaxNodes());
        queue = queueByName.get("normal");
        assertTrue(queue.isAutoSelectable());
        assertFalse(queue.isReserved());
        assertEquals(8, queue.getMaxWallTimeHours(), 1e-3);
        assertEquals(Integer.MAX_VALUE, queue.getMaxNodes());
    }

    /**
     * Exercises update of pipeline definitions when all database instances are unlocked.
     */
    @Test
    public void testUpdateLockedDefinitions() {
        PipelineOperations pipelineOperations = new PipelineOperations();
        PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
        new PipelineDefinitionImporter(List.of(pipelineDefinitionFile, parameterSetFile))
            .importPipelineDefinitions();

        // Lock everything.
        List<Pipeline> pipelines = pipelineOperations.allPipelines();
        pipelineOperations.lock(pipelines.get(0));
        List<PipelineStep> pipelineSteps = pipelineStepOperations.allPipelineSteps();
        for (PipelineStep pipelineStep : pipelineSteps) {
            pipelineStepOperations.lock(pipelineStep);
        }

        // Now we import the file with both new and updated definitions.
        PipelineDefinitionImporter importer = new PipelineDefinitionImporter(
            List.of(pipelineUpdateFile));
        importer.setUpdate(true);
        importer.importPipelineDefinitions();

        pipelines = pipelineOperations.allPipelines();
        assertEquals(2, pipelines.size());
        Map<String, Pipeline> pipelineByName = new HashMap<>();
        for (Pipeline pipeline : pipelines) {
            pipelineByName.put(pipeline.getName(), pipeline);
        }
        Pipeline pipeline = pipelineByName.get("hyperion");
        verifyUpdatedHyperionPipeline(pipeline);
        assertFalse(pipeline.isLocked());
        assertEquals(1, pipeline.getVersion());
        pipeline = pipelineByName.get("genuinely-new");
        verifyGenuinelyNewPipeline(pipeline);
        assertFalse(pipeline.isLocked());
        assertEquals(0, pipeline.getVersion());

        List<PipelineNode> pipelineNodes = new PipelineOperations().rootNodes(pipeline);
        PipelineNode pipelineNode = pipelineNodes.get(0);
        assertEquals("level0", pipelineNode.getPipelineStepName());
        assertEquals("genuinely-new", pipelineNode.getPipelineName());
        assertEquals(1, pipelineNodes.size());

        pipeline = pipelineByName.get("hyperion");
        pipelineNodes = new PipelineOperations().rootNodes(pipeline);
        pipelineNode = pipelineNodes.get(0);
        assertEquals("level1", pipelineNode.getPipelineStepName());
        assertEquals("hyperion", pipelineNode.getPipelineName());
        assertEquals(1, pipelineNodes.size());

        pipelineSteps = pipelineStepOperations.pipelineSteps();
        assertEquals(3, pipelineSteps.size());
        Map<String, PipelineStep> pipelineStepByName = new HashMap<>();
        for (PipelineStep pipelineStep : pipelineSteps) {
            pipelineStepByName.put(pipelineStep.getName(), pipelineStep);
        }
        assertTrue(pipelineStepByName.containsKey("level0"));
        assertTrue(pipelineStepByName.containsKey("level1"));
        assertTrue(pipelineStepByName.containsKey("level3"));
        assertFalse(pipelineStepByName.get("level0").isLocked());
        assertEquals(1, pipelineStepByName.get("level0").getVersion());
        assertTrue(pipelineStepByName.get("level1").isLocked());
        assertEquals(0, pipelineStepByName.get("level1").getVersion());
        assertFalse(pipelineStepByName.get("level3").isLocked());
        assertEquals(0, pipelineStepByName.get("level3").getVersion());
    }

    @Test
    public void testMultipleDefaultParamSets() throws Exception {

        // Use the PipelineSupervisor constructor to set the worker count.
        new PipelineSupervisor(1, 1000);

        // Read in the pipeline definition that has 4 instances of Parameters
        // attached to it.
        new PipelineDefinitionImporter(
            List.of(ZiggyUnitTestUtils.TEST_DATA.resolve("pd-four-default-param-sets.xml"),
                ZiggyUnitTestUtils.TEST_DATA.resolve("pl-four-default-param-sets.xml")))
                    .importPipelineDefinitions();

        // Retrieve the pipeline definition
        Pipeline pipeline = new PipelineOperations().pipeline("hyperion");

        // Check the contents of the parameter set map.
        Set<String> parameterSetNames = new PipelineOperations().parameterSetNames(pipeline);
        pipeline.getParameterSetNames();
        assertTrue(parameterSetNames.contains("Sample classless parameter set"));
        assertTrue(parameterSetNames.contains("ISOFIT algorithm parameters"));
        assertEquals(2, parameterSetNames.size());

        List<PipelineNode> rootNodes = new PipelineOperations().rootNodes(pipeline);
        parameterSetNames = new PipelineNodeOperations().parameterSetNames(rootNodes.get(0));
        assertTrue(parameterSetNames.contains("another darn parameter set!"));
        assertTrue(parameterSetNames.contains("yet another one..."));
        assertEquals(2, parameterSetNames.size());

        // Create a pipeline instance for this pipeline
        PipelineInstance pipelineInstance = new PipelineExecutor().launch(pipeline, "instance-name",
            null, null, null);

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

        parameterSet = parameterSetsByName.get("ISOFIT algorithm parameters");
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
        // Read in the pipeline definition that has 4 instances of Parameters
        // attached to it, with one of the pipeline parameter sets not in the database.
        new PipelineDefinitionImporter(
            List.of(ZiggyUnitTestUtils.TEST_DATA.resolve("pd-bad-pipeline-param-sets.xml"),
                ZiggyUnitTestUtils.TEST_DATA.resolve("pl-four-default-param-sets.xml")))
                    .importPipelineDefinitions();
    }

    @Test(expected = PipelineException.class)
    public void testBadNodeParameterSetName() {
        // Read in the pipeline definition that has 4 instances of Parameters
        // attached to it, with one of the node parameter sets not in the database.
        new PipelineDefinitionImporter(
            List.of(ZiggyUnitTestUtils.TEST_DATA.resolve("pd-bad-node-param-sets.xml"),
                ZiggyUnitTestUtils.TEST_DATA.resolve("pl-four-default-param-sets.xml")))
                    .importPipelineDefinitions();
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

    @Test
    public void testImportDataReceiptNode() {
        PipelineOperations pipelineOperations = new PipelineOperations();
        PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
        pipelineStepOperations.createDataReceiptPipelineStep();
        new PipelineDefinitionImporter(List.of(dataReceiptTestFile)).importPipelineDefinitions();

        List<Pipeline> pipelines = pipelineOperations.allPipelines();
        assertEquals(1, pipelines.size());
        Pipeline pipeline = pipelines.get(0);
        assertEquals(1, pipeline.getRootNodes().size());
        PipelineNode pipelineNode = pipeline.getRootNodes().get(0);
        assertEquals("data-receipt", pipelineNode.getPipelineStepName());
        assertTrue(pipelineNode.getSingleSubtask());
        assertTrue(pipelineNode.getNextNodes().isEmpty());
    }

    private void verifyParsing(String value, String[] elements) {
        List<String> expected = ImmutableList.copyOf(elements);
        List<String> actual = ZiggyStringUtils.splitStringAtCommas(value);
        assertEquals(expected, actual);
    }

    private void verifyHyperionPipeline(Pipeline pipelineDefinition) {
        PipelineNodeOperations nodeOperations = new PipelineNodeOperations();
        List<PipelineNode> pipelineNodes = pipelineDefinition.getRootNodes();
        assertEquals(1, pipelineNodes.size());
        PipelineNode pipelineNode = pipelineNodes.get(0);
        assertEquals("level0", pipelineNode.getPipelineStepName());
        assertFalse(pipelineNode.getSingleSubtask());
        Set<DataFileType> inputDataFileTypes = nodeOperations.inputDataFileTypes(pipelineNode);
        assertEquals(1, inputDataFileTypes.size());
        DataFileType inputDataFileType = inputDataFileTypes.iterator().next();
        assertEquals("Hyperion L0", inputDataFileType.getName());
        Set<DataFileType> outputDataFileTypes = nodeOperations.outputDataFileTypes(pipelineNode);
        assertEquals(1, outputDataFileTypes.size());
        DataFileType outputDataFileType = outputDataFileTypes.iterator().next();
        assertEquals("Hyperion L1R", outputDataFileType.getName());
        Set<String> parameterSetNames = nodeOperations.parameterSetNames(pipelineNode);
        assertTrue(parameterSetNames.contains("New Remote Hyperion L0"));
        assertTrue(parameterSetNames.contains("Hyperion L0 Task Configuration"));
        assertTrue(parameterSetNames.contains("Sample classless parameter set"));
        assertEquals(3, parameterSetNames.size());
        Set<ModelType> modelTypes = nodeOperations.modelTypes(pipelineNode);
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

        pipelineNodes = pipelineNode.getNextNodes();
        assertEquals(1, pipelineNodes.size());
        pipelineNode = pipelineNodes.get(0);
        assertEquals("level1", pipelineNode.getPipelineStepName());
        assertTrue(pipelineNode.getSingleSubtask());
        inputDataFileTypes = nodeOperations.inputDataFileTypes(pipelineNode);
        assertEquals(1, inputDataFileTypes.size());
        inputDataFileType = inputDataFileTypes.iterator().next();
        assertEquals("Hyperion L1R", inputDataFileType.getName());
        outputDataFileTypes = nodeOperations.outputDataFileTypes(pipelineNode);
        assertEquals(1, outputDataFileTypes.size());
        outputDataFileType = outputDataFileTypes.iterator().next();
        assertEquals("Hyperion L2", outputDataFileType.getName());
        parameterSetNames = nodeOperations.parameterSetNames(pipelineNode);
        assertTrue(parameterSetNames.contains("Hyperion L0 Task Configuration"));
        assertTrue(parameterSetNames.contains("ISOFIT algorithm parameters"));
        assertTrue(parameterSetNames.contains("Remote Hyperion L1"));
        assertEquals(3, parameterSetNames.size());
        modelTypes = nodeOperations.modelTypes(pipelineNode);
        modelTypeNames = modelTypes.stream().map(ModelType::getType).collect(Collectors.toSet());
        assertTrue(modelTypeNames.contains("sRTM neural network"));
        assertTrue(modelTypeNames.contains("surface"));
        assertTrue(modelTypeNames.contains("dem"));
        assertEquals(3, modelTypeNames.size());

        assertTrue(CollectionUtils.isEmpty(pipelineNode.getNextNodes()));

        List<ZiggyEventHandler> ziggyEventHandlers = new ZiggyEventOperations().eventHandlers();
        assertEquals(2, ziggyEventHandlers.size());
        List<String> eventHandlerNames = ziggyEventHandlers.stream()
            .map(ZiggyEventHandler::getName)
            .toList();
        assertTrue(eventHandlerNames.contains("data-receipt"));
        assertTrue(eventHandlerNames.contains("another-event-handler"));
    }

    private void verifyGenuinelyNewPipeline(Pipeline pipeline) {
        PipelineNodeOperations nodeOperations = new PipelineNodeOperations();
        List<PipelineNode> pipelineNodes = pipeline.getRootNodes();
        assertEquals(1, pipelineNodes.size());
        PipelineNode pipelineNode = pipelineNodes.get(0);
        assertEquals("level0", pipelineNode.getPipelineStepName());
        Set<DataFileType> inputDataFileTypes = nodeOperations.inputDataFileTypes(pipelineNode);
        assertEquals(1, inputDataFileTypes.size());
        DataFileType inputDataFileType = inputDataFileTypes.iterator().next();
        assertEquals("Hyperion L2", inputDataFileType.getName());
        assertTrue(CollectionUtils.isEmpty(nodeOperations.outputDataFileTypes(pipelineNode)));
        assertTrue(CollectionUtils.isEmpty(nodeOperations.modelTypes(pipelineNode)));
        Set<String> parameterSetNames = nodeOperations.parameterSetNames(pipelineNode);
        assertEquals(1, parameterSetNames.size());
        assertEquals("New Remote Hyperion L0", parameterSetNames.iterator().next());
        assertTrue(CollectionUtils.isEmpty(pipelineNode.getNextNodes()));
    }

    private void verifyUpdatedHyperionPipeline(Pipeline pipeline) {
        PipelineNodeOperations nodeOperations = new PipelineNodeOperations();
        List<PipelineNode> pipelineNodes = pipeline.getRootNodes();
        assertEquals(1, pipelineNodes.size());
        PipelineNode pipelineNode = pipelineNodes.get(0);
        assertEquals("level1", pipelineNode.getPipelineStepName());
        assertTrue(CollectionUtils.isEmpty(nodeOperations.inputDataFileTypes(pipelineNode)));
        assertTrue(CollectionUtils.isEmpty(nodeOperations.outputDataFileTypes(pipelineNode)));
        assertTrue(pipelineNode.getSingleSubtask());
        Set<ModelType> modelTypes = nodeOperations.modelTypes(pipelineNode);
        assertEquals(1, modelTypes.size());
        assertEquals("bandwidth", modelTypes.iterator().next().getType());
        Set<String> parameterSetNames = nodeOperations.parameterSetNames(pipelineNode);
        assertEquals(1, parameterSetNames.size());
        assertEquals("Hyperion L0 Task Configuration", parameterSetNames.iterator().next());

        pipelineNodes = pipelineNode.getNextNodes();
        assertEquals(1, pipelineNodes.size());
        pipelineNode = pipelineNodes.get(0);
        assertEquals("level3", pipelineNode.getPipelineStepName());
        assertTrue(CollectionUtils.isEmpty(nodeOperations.inputDataFileTypes(pipelineNode)));
        assertTrue(CollectionUtils.isEmpty(nodeOperations.outputDataFileTypes(pipelineNode)));
        assertTrue(CollectionUtils.isEmpty(nodeOperations.modelTypes(pipelineNode)));
        assertTrue(CollectionUtils.isEmpty(nodeOperations.parameterSetNames(pipelineNode)));

        assertTrue(CollectionUtils.isEmpty(pipelineNode.getNextNodes()));
    }

    private void verifyParameterSets() {
        List<String> parameterSetNames = new ParametersOperations().parameterSetNames();
        assertTrue(parameterSetNames.contains("New Remote Hyperion L0"));
        assertTrue(parameterSetNames.contains("Remote Hyperion L1"));
        assertTrue(parameterSetNames.contains("Hyperion L0 Task Configuration"));
        assertTrue(parameterSetNames.contains("Sample classless parameter set"));
        assertTrue(parameterSetNames.contains("ISOFIT algorithm parameters"));
        assertEquals(5, parameterSetNames.size());
    }

    private void verifyDatastoreConfiguration() {
        DatastoreOperations datastoreOperations = new DatastoreOperations();
        Map<String, DatastoreRegexp> datastoreRegexps = datastoreOperations
            .datastoreRegexpsByName();
        assertTrue(datastoreRegexps.containsKey("year"));
        assertTrue(datastoreRegexps.containsKey("chunk"));
        assertEquals(2, datastoreRegexps.size());

        Map<String, DatastoreNode> datastoreNodesByFullPath = datastoreOperations
            .datastoreNodesByFullPath();
        assertTrue(datastoreNodesByFullPath.containsKey("hyperion"));
        assertTrue(datastoreNodesByFullPath.containsKey("hyperion/year"));
        assertTrue(datastoreNodesByFullPath.containsKey("hyperion/year/L0"));
        assertTrue(datastoreNodesByFullPath.containsKey("hyperion/year/L0/chunk"));
        assertTrue(datastoreNodesByFullPath.containsKey("hyperion/year/L1R"));
        assertTrue(datastoreNodesByFullPath.containsKey("hyperion/year/L2"));
        assertEquals(6, datastoreNodesByFullPath.size());
    }

    private void verifyDataFileTypes() {
        DatastoreOperations datastoreOperations = new DatastoreOperations();
        List<String> dataFileTypeNames = datastoreOperations.dataFileTypeNames();
        assertTrue(dataFileTypeNames.contains("Hyperion L0"));
        assertTrue(dataFileTypeNames.contains("Hyperion L1R"));
        assertTrue(dataFileTypeNames.contains("Hyperion L2"));
        assertEquals(3, dataFileTypeNames.size());
    }

    private void verifyModelTypes() {
        DatastoreOperations datastoreOperations = new DatastoreOperations();
        List<String> modelTypes = datastoreOperations.modelTypes();
        assertTrue(modelTypes.contains("metadata-updates"));
        assertTrue(modelTypes.contains("bandwidth"));
        assertTrue(modelTypes.contains("template"));
        assertTrue(modelTypes.contains("gain"));
        assertTrue(modelTypes.contains("ratio"));
        assertTrue(modelTypes.contains("spectra"));
        assertTrue(modelTypes.contains("L0 attributes"));
        assertTrue(modelTypes.contains("sRTM neural network"));
        assertTrue(modelTypes.contains("surface"));
        assertTrue(modelTypes.contains("dem"));
        assertEquals(10, modelTypes.size());
    }

    private void verifyRemoteEnvironments() {
        RemoteEnvironmentOperations remoteEnvironmentOperations = new RemoteEnvironmentOperations();
        Map<String, RemoteEnvironment> remoteEnvironmentByName = remoteEnvironmentOperations
            .remoteEnvironmentByName();
        assertTrue(remoteEnvironmentByName.containsKey("nas"));
        assertTrue(remoteEnvironmentByName.containsKey("bauhaus"));
        assertEquals(2, remoteEnvironmentByName.size());

        RemoteEnvironment environment = remoteEnvironmentByName.get("nas");
        assertEquals("SBU", environment.getCostUnit());
        assertEquals(SupportedBatchSystem.PBS, environment.getBatchSystem());
        Map<String, Architecture> architectureByName = architectureByName(environment);
        assertTrue(architectureByName.containsKey("has"));
        assertTrue(architectureByName.containsKey("bro"));
        assertEquals(2, architectureByName.size());
        assertEquals(0, architectureByName.get("has").getBandwidthGbps(), 1e-6);
        assertEquals(0, architectureByName.get("bro").getBandwidthGbps(), 1e-6);
        Architecture architecture = architectureByName.get("bro");
        assertEquals(28, architecture.getCores());
        assertEquals(128, architecture.getRamGigabytes());
        assertEquals(1.0, architecture.getCost(), 1e-3);
        Map<String, BatchQueue> queueByName = queueByName(environment);
        assertTrue(queueByName.containsKey("debug"));
        assertTrue(queueByName.containsKey("reserved"));
        assertTrue(queueByName.containsKey("normal"));
        assertEquals(3, queueByName.size());
        BatchQueue queue = queueByName.get("debug");
        assertFalse(queue.isAutoSelectable());
        assertFalse(queue.isReserved());
        assertEquals(2, queue.getMaxWallTimeHours(), 1e-3);
        assertEquals(2, queue.getMaxNodes());
        queue = queueByName.get("reserved");
        assertFalse(queue.isAutoSelectable());
        assertTrue(queue.isReserved());
        assertEquals(Float.MAX_VALUE, queue.getMaxWallTimeHours(), 1000);
        assertEquals(Integer.MAX_VALUE, queue.getMaxNodes());
        queue = queueByName.get("normal");
        assertTrue(queue.isAutoSelectable());
        assertFalse(queue.isReserved());
        assertEquals(8, queue.getMaxWallTimeHours(), 1e-3);
        assertEquals(Integer.MAX_VALUE, queue.getMaxNodes());

        environment = remoteEnvironmentByName.get("bauhaus");
        assertEquals("$", environment.getCostUnit());
        assertEquals(SupportedBatchSystem.PBS, environment.getBatchSystem());
        architectureByName = architectureByName(environment);
        assertTrue(architectureByName.containsKey("ilc"));
        assertTrue(architectureByName.containsKey("bro"));
        assertEquals(2, architectureByName.size());
        architecture = architectureByName.get("ilc");
        assertEquals(10, architecture.getBandwidthGbps(), 1e-6);
        assertEquals("/path/to/file", architecture.getNodeCollectionNamesFile());
        architecture = architectureByName.get("bro");
        assertEquals(16, architecture.getCores());
        assertEquals(32, architecture.getRamGigabytes());
        assertEquals(0.5, architecture.getCost(), 1e-3);
        queueByName = queueByName(environment);
        assertTrue(queueByName.containsKey("normal"));
        assertEquals(1, queueByName.size());
        queue = queueByName.get("normal");
        assertTrue(queue.isAutoSelectable());
        assertFalse(queue.isReserved());
        assertEquals(24, queue.getMaxWallTimeHours(), 1e-3);
        assertEquals(Integer.MAX_VALUE, queue.getMaxNodes());
    }

    private Map<String, Architecture> architectureByName(RemoteEnvironment remoteEnvironment) {
        List<Architecture> architectures = remoteEnvironment.getArchitectures();
        Map<String, Architecture> architectureByName = new HashMap<>();
        for (Architecture architecture : architectures) {
            architectureByName.put(architecture.getName(), architecture);
        }
        return architectureByName;
    }

    private Map<String, BatchQueue> queueByName(RemoteEnvironment remoteEnvironment) {
        List<BatchQueue> queues = remoteEnvironment.getQueues();
        Map<String, BatchQueue> queueByName = new HashMap<>();
        for (BatchQueue queue : queues) {
            queueByName.put(queue.getName(), queue);
        }
        return queueByName;
    }
}
