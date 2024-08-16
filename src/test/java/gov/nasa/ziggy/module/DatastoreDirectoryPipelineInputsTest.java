package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager.SubtaskDefinition;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.data.datastore.DatastoreTestUtils;
import gov.nasa.ziggy.data.datastore.DatastoreWalker;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Unit test class for {@link DatastoreDirectoryPipelineInputs}.
 *
 * @author PT
 */
public class DatastoreDirectoryPipelineInputsTest {

    private static final int EXPECTED_SUBTASK_COUNT = 7;
    private PipelineTask pipelineTask;
    private PipelineInstance pipelineInstance;
    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineDefinitionNode pipelineDefinitionNode;
    private PipelineInputsForTest pipelineInputs;
    private Path taskDirectory;
    private Map<String, DatastoreRegexp> regexpsByName;
    private DatastoreWalker datastoreWalker;
    private DatastoreFileManager datastoreFileManager;
    private Map<String, String> regexpValueByName = new HashMap<>();
    private ModelMetadata modelMetadata;
    private Set<SubtaskDefinition> subtaskDefinitions;
    private Map<String, SubtaskDefinition> subtaskDefinitionsByBaseName = new HashMap<>();
    private TaskConfiguration taskConfiguration;
    private DataFileType calibratedCollateralPixelDataFileType;
    private PipelineTaskOperations pipelineTaskOperations = Mockito
        .mock(PipelineTaskOperations.class);
    private PipelineInstanceOperations pipelineInstanceOperations = Mockito
        .mock(PipelineInstanceOperations.class);
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = Mockito
        .mock(PipelineInstanceNodeOperations.class);
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = Mockito
        .mock(PipelineDefinitionNodeOperations.class);

    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootProperty = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR, ziggyDirectoryRule, "datastore");

    public ZiggyPropertyRule taskDirRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        ziggyDirectoryRule, "pipeline-results");

    @Rule
    public final RuleChain testRuleChain = RuleChain.outerRule(ziggyDirectoryRule)
        .around(datastoreRootProperty)
        .around(taskDirRule);

    @Before
    public void setup() throws IOException {

        pipelineTask = Mockito.mock(PipelineTask.class);
        taskDirectory = DirectoryProperties.taskDataDir();

        regexpsByName = DatastoreTestUtils.regexpsByName();
        datastoreWalker = new DatastoreWalker(regexpsByName,
            DatastoreTestUtils.datastoreNodesByFullPath());

        // Create datastore directories.
        DatastoreTestUtils.createDatastoreDirectories();

        // Get and update data file types.
        Map<String, DataFileType> dataFileTypes = DatastoreTestUtils.dataFileTypesByName();
        DataFileType uncalibratedSciencePixelDataFileType = dataFileTypes
            .get("uncalibrated science pixel values");
        DataFileType uncalibratedCollateralPixelDataFileType = dataFileTypes
            .get("uncalibrated collateral pixel values");
        DataFileType allFilesAllSubtasksDataFileType = dataFileTypes
            .get("calibrated science pixel values");
        calibratedCollateralPixelDataFileType = dataFileTypes
            .get("calibrated collateral pixel values");

        // Construct the Map from regexp name to value.
        regexpValueByName.put("sector", "sector-0002");
        regexpValueByName.put("cadenceType", "target");
        regexpValueByName.put("channel", "1:1:A");
        for (Map.Entry<String, String> regexpEntry : regexpValueByName.entrySet()) {
            regexpsByName.get(regexpEntry.getKey()).setInclude(regexpEntry.getValue());
        }

        // Create datastore files.
        constructDatastoreFiles(uncalibratedSciencePixelDataFileType, EXPECTED_SUBTASK_COUNT + 1,
            "uncalibrated-pixels-", ".science.nc");
        constructDatastoreFiles(uncalibratedCollateralPixelDataFileType, EXPECTED_SUBTASK_COUNT,
            "uncalibrated-pixels-", ".collateral.nc");
        constructDatastoreFiles(allFilesAllSubtasksDataFileType, 2, "everyone-needs-me-", ".nc");

        // Construct a model type and model metadata.
        ModelType modelType = new ModelType();
        modelType.setType("test");
        modelMetadata = new ModelMetadata();
        modelMetadata.setModelType(modelType);
        modelMetadata.setOriginalFileName("foo");
        modelMetadata.setDatastoreFileName("bar");
        Files.createDirectories(modelMetadata.datastoreModelPath().getParent());
        Files.createFile(modelMetadata.datastoreModelPath());

        // Create the PipelineTask.
        pipelineTask = Mockito.mock(PipelineTask.class);
        pipelineInstance = Mockito.mock(PipelineInstance.class);
        pipelineInstanceNode = Mockito.mock(PipelineInstanceNode.class);
        pipelineDefinitionNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.when(pipelineTask.getModuleName()).thenReturn("testmod");
        Mockito
            .when(
                pipelineTaskOperations.inputDataFileTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(uncalibratedSciencePixelDataFileType,
                uncalibratedCollateralPixelDataFileType, allFilesAllSubtasksDataFileType));
        Mockito
            .when(pipelineTaskOperations
                .outputDataFileTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(calibratedCollateralPixelDataFileType));
        Mockito.when(pipelineTaskOperations.modelTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(modelType));
        Mockito
            .when(pipelineTaskOperations.pipelineInstance(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineInstance);
        Mockito
            .when(pipelineTaskOperations
                .pipelineInstanceNode(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineInstanceNode);
        Mockito.when(pipelineInstanceNode.getPipelineDefinitionNode())
            .thenReturn(pipelineDefinitionNode);
        Mockito
            .when(pipelineTaskOperations
                .pipelineDefinitionNode(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineDefinitionNode);
        Mockito.when(pipelineDefinitionNode.getSingleSubtask()).thenReturn(false);

        // Create the parameter sets.
        Mockito.when(pipelineInstanceOperations.parameterSets(pipelineInstance))
            .thenReturn(pipelineParameterSets());
        Mockito
            .when(pipelineInstanceNodeOperations
                .parameterSets(ArgumentMatchers.any(PipelineInstanceNode.class)))
            .thenReturn(moduleParameterSets());

        // Construct the UOW.
        DatastoreDirectoryUnitOfWorkGenerator uowGenerator = Mockito
            .spy(DatastoreDirectoryUnitOfWorkGenerator.class);
        Mockito.doReturn(datastoreWalker).when(uowGenerator).datastoreWalker();
        Mockito
            .when(pipelineDefinitionNodeOperations
                .inputDataFileTypes(ArgumentMatchers.any(PipelineDefinitionNode.class)))
            .thenReturn(Set.of(uncalibratedSciencePixelDataFileType,
                uncalibratedCollateralPixelDataFileType, allFilesAllSubtasksDataFileType));
        Mockito.doReturn(pipelineDefinitionNodeOperations)
            .when(uowGenerator)
            .pipelineDefinitionNodeOperations();
        List<UnitOfWork> uows = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(uows.get(0));

        // Construct mocked DatastoreFileManager.
        datastoreFileManager = Mockito.mock(DatastoreFileManager.class);
        Mockito.when(datastoreFileManager.taskDirectory()).thenReturn(taskDirectory);
        subtaskDefinitions = new HashSet<>();
        populateFilesForSubtasks(EXPECTED_SUBTASK_COUNT);
        Mockito.when(datastoreFileManager.subtaskDefinitions()).thenReturn(subtaskDefinitions);
        Map<Path, String> modelFilesForTask = new HashMap<>();
        modelFilesForTask.put(modelMetadata.datastoreModelPath(), "foo");
        Mockito.when(datastoreFileManager.modelTaskFilesByDatastorePath())
            .thenReturn(modelFilesForTask);
        Mockito
            .when(datastoreFileManager.copyDatastoreFilesToTaskDirectory(ArgumentMatchers.anySet(),
                ArgumentMatchers.anyMap()))
            .thenReturn(pathsBySubtaskDirectory(EXPECTED_SUBTASK_COUNT));

        // Construct the pipeline inputs. We can't use the standard method of a Mockito spy
        // applied to a PipelineInputs instance because Mockito's spy and the HDF5 API don't
        // work together. Hence we need to have a subclass of DatastoreDirectoryPipelineInputs
        // that takes all the necessary arguments and makes correct use of them.
        pipelineInputs = new PipelineInputsForTest(datastoreFileManager,
            Mockito.mock(AlertService.class), pipelineTask, pipelineTaskOperations,
            pipelineInstanceOperations, pipelineInstanceNodeOperations);

        taskConfiguration = new TaskConfiguration();
    }

    /** Constructs a collection of zero-length files in the datastore. */
    private void constructDatastoreFiles(DataFileType dataFileType, int fileCount,
        String filenamePrefix, String filenameSuffix) throws IOException {
        Path datastorePath = datastoreWalker.pathFromLocationAndRegexpValues(regexpValueByName,
            dataFileType.getLocation());
        for (int fileCounter = 0; fileCounter < fileCount; fileCounter++) {
            String filename = filenamePrefix + fileCounter + filenameSuffix;
            Files.createDirectories(datastorePath);
            Files.createFile(datastorePath.resolve(filename));
        }
    }

    private void populateFilesForSubtasks(int subtaskCount) {
        for (int subtaskIndex = 0; subtaskIndex < subtaskCount; subtaskIndex++) {
            String baseName = "uncalibrated-pixels-" + subtaskIndex;
            Set<Path> subtaskFiles = new HashSet<>();
            subtaskFiles.add(DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("cal")
                .resolve("pixels")
                .resolve("target")
                .resolve("science")
                .resolve("1:1:A")
                .resolve("everyone-needs-me-0.nc"));
            subtaskFiles.add(DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("cal")
                .resolve("pixels")
                .resolve("target")
                .resolve("science")
                .resolve("1:1:A")
                .resolve("everyone-needs-me-1.nc"));
            subtaskFiles.add(DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("dr")
                .resolve("pixels")
                .resolve("target")
                .resolve("science")
                .resolve("1:1:A")
                .resolve(baseName + ".science.nc"));
            subtaskFiles.add(DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("dr")
                .resolve("pixels")
                .resolve("target")
                .resolve("collateral")
                .resolve("1:1:A")
                .resolve(baseName + ".collateral.nc"));
            SubtaskDefinition subtaskDefinition = new SubtaskDefinition(baseName, null);
            subtaskDefinition.addAll(subtaskFiles);
            subtaskDefinitions.add(subtaskDefinition);
            subtaskDefinitionsByBaseName.put(baseName, subtaskDefinition);
        }
    }

    private Map<Path, Set<Path>> pathsBySubtaskDirectory(int subtaskCount) throws IOException {
        Map<Path, Set<Path>> pathsBySubtaskDirectory = new HashMap<>();
        for (int subtaskIndex = 0; subtaskIndex < subtaskCount; subtaskIndex++) {
            Path subtaskPath = taskDirectory.resolve("st-" + subtaskIndex);
            Files.createDirectories(subtaskPath);
            String baseName = "uncalibrated-pixels-" + subtaskIndex;
            pathsBySubtaskDirectory.put(subtaskPath,
                subtaskDefinitionsByBaseName.get(baseName).getSubtaskFiles());
        }
        return pathsBySubtaskDirectory;
    }

    /** Exercises the copyDatastoreFilesToTaskDirectory() method. */
    @Test
    public void testCopyDatastoreFilesToTaskDirectory() throws IOException {

        // Note that we don't actually copy any files to the subtask directory.
        // That capability has been fully tested in the DatastoreFileManager.
        // Here we just want to see that the HDF5 file in each subtask directory
        // contains what we expect to see.
        pipelineInputs.copyDatastoreFilesToTaskDirectory(taskConfiguration, taskDirectory);
        assertEquals(EXPECTED_SUBTASK_COUNT, taskConfiguration.getSubtaskCount());
        Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();

        for (int subtaskIndex = 0; subtaskIndex < EXPECTED_SUBTASK_COUNT; subtaskIndex++) {
            assertTrue(Files
                .exists(taskDirectory.resolve("st-" + subtaskIndex).resolve("testmod-inputs.h5")));
            PipelineInputsForTest storedInputs = new PipelineInputsForTest(datastoreFileManager,
                Mockito.mock(AlertService.class), pipelineTask, pipelineTaskOperations,
                pipelineInstanceOperations, pipelineInstanceNodeOperations);
            hdf5ModuleInterface.readFile(
                taskDirectory.resolve("st-" + subtaskIndex).resolve("testmod-inputs.h5").toFile(),
                storedInputs, false);

            assertTrue(storedInputs.getModelFilenames().contains("foo"));
            assertEquals(1, storedInputs.getModelFilenames().size());

            assertTrue(storedInputs.getDataFilenames()
                .contains("uncalibrated-pixels-" + subtaskIndex + ".science.nc"));
            assertTrue(storedInputs.getDataFilenames()
                .contains("uncalibrated-pixels-" + subtaskIndex + ".collateral.nc"));
            assertTrue(storedInputs.getDataFilenames().contains("everyone-needs-me-0.nc"));
            assertTrue(storedInputs.getDataFilenames().contains("everyone-needs-me-1.nc"));
            assertEquals(4, storedInputs.getDataFilenames().size());

            Map<String, ParameterSet> parameterSetsByName = storedInputs.getModuleParameters()
                .getParameterSetsByName();

            ParameterSet paramSet1 = parameterSetsByName.get("params1");
            assertNotNull(paramSet1);
            Map<String, Parameter> parametersByName = paramSet1.parameterByName();
            Parameter parameter = parametersByName.get("dmy1");
            assertNotNull(parameter);
            assertEquals("500", parameter.getString());
            assertEquals(ZiggyDataType.ZIGGY_INT, parameter.getDataType());
            assertTrue(parameter.isScalar());
            parameter = parametersByName.get("dmy2");
            assertNotNull(parameter);
            assertEquals("2856.3", parameter.getString());
            assertEquals(ZiggyDataType.ZIGGY_DOUBLE, parameter.getDataType());
            assertTrue(parameter.isScalar());
            assertEquals(2, parameterSetsByName.size());

            ParameterSet paramSet2 = parameterSetsByName.get("params2");
            assertNotNull(paramSet2);
            parametersByName = paramSet2.parameterByName();
            parameter = parametersByName.get("dmy3");
            assertNotNull(parameter);
            assertEquals("dummy string", parameter.getString());
            assertEquals(ZiggyDataType.ZIGGY_STRING, parameter.getDataType());
            assertTrue(parameter.isScalar());
            parameter = parametersByName.get("dmy4");
            assertNotNull(parameter);
            assertEquals("true,false", parameter.getString());
            assertEquals(ZiggyDataType.ZIGGY_BOOLEAN, parameter.getDataType());
            assertFalse(parameter.isScalar());
            assertEquals(2, parameterSetsByName.size());

            assertEquals(2, parameterSetsByName.size());

            Collection<DataFileType> outputDataFileTypes = PipelineInputsOutputsUtils
                .deserializedOutputFileTypesFromTaskDirectory(taskDirectory);
            assertTrue(outputDataFileTypes.contains(calibratedCollateralPixelDataFileType));
            assertEquals(1, outputDataFileTypes.size());
        }
    }

    /** Tests the subtaskInformation() method. */

    @Test
    public void testSubtaskInformationFromPipelineDefinitionNode() {

        Mockito.when(datastoreFileManager.subtaskCount(pipelineDefinitionNode)).thenReturn(7);
        SubtaskInformation subtaskInformation = pipelineInputs
            .subtaskInformation(pipelineDefinitionNode);
        assertEquals("testmod", subtaskInformation.getModuleName());
        assertEquals("[sector-0002;target;1:1:A]", subtaskInformation.getUowBriefState());
        assertEquals(7, subtaskInformation.getSubtaskCount());

        Mockito.when(pipelineDefinitionNode.getSingleSubtask()).thenReturn(true);
        subtaskInformation = pipelineInputs.subtaskInformation(pipelineDefinitionNode);
        assertEquals("testmod", subtaskInformation.getModuleName());
        assertEquals("[sector-0002;target;1:1:A]", subtaskInformation.getUowBriefState());
        assertEquals(1, subtaskInformation.getSubtaskCount());
    }

    private Set<ParameterSet> pipelineParameterSets() {
        ParameterSet s1 = new ParameterSet("params1");
        Set<Parameter> parameters = new HashSet<>();
        Parameter parameter = new Parameter("dmy1", "500", ZiggyDataType.ZIGGY_INT);
        parameters.add(parameter);
        parameter = new Parameter("dmy2", "2856.3", ZiggyDataType.ZIGGY_DOUBLE);
        parameters.add(parameter);
        s1.setParameters(parameters);
        return new HashSet<>(Set.of(s1));
    }

    private Set<ParameterSet> moduleParameterSets() {
        ParameterSet s2 = new ParameterSet("params2");
        Set<Parameter> parameters = new HashSet<>();
        Parameter parameter = new Parameter("dmy3", "dummy string", ZiggyDataType.ZIGGY_STRING);
        parameters.add(parameter);
        parameter = new Parameter("dmy4", "true, false", ZiggyDataType.ZIGGY_BOOLEAN, false);
        parameters.add(parameter);
        s2.setParameters(parameters);
        return new HashSet<>(Set.of(s2));
    }

    /**
     * Subclass of {@link DatastoreDirectoryPipelineInputs}. This is necessary because if we create
     * an instance of DatastoreDirectoryPipelineInputs and then apply a Mockito spy to it, the HDF5
     * API fails. Hence we need a subclass that has additional functionality we can use in the
     * places where ordinarily we would use Mockito doReturn() ... when() calls on a spy.
     *
     * @author PT
     */
    public static class PipelineInputsForTest extends DatastoreDirectoryPipelineInputs {

        @ProxyIgnore
        private final AlertService mockedAlertService;

        @ProxyIgnore
        private final PipelineTaskOperations mockedOperations;
        @ProxyIgnore
        private final DatastoreFileManager mockedDatastoreFileManager;
        @ProxyIgnore
        private final PipelineInstanceOperations mockedInstanceOperations;
        @ProxyIgnore
        private final PipelineInstanceNodeOperations mockedInstanceNodeOperations;

        @ProxyIgnore
        private boolean singleSubtask;

        public PipelineInputsForTest(DatastoreFileManager datastoreFileManager,
            AlertService alertService, PipelineTask pipelineTask,
            PipelineTaskOperations mockedOperations,
            PipelineInstanceOperations mockedInstanceOperations,
            PipelineInstanceNodeOperations mockedInstanceNodeOperations) {
            mockedAlertService = alertService;
            mockedDatastoreFileManager = datastoreFileManager;
            this.mockedOperations = mockedOperations;
            this.mockedInstanceOperations = mockedInstanceOperations;
            this.mockedInstanceNodeOperations = mockedInstanceNodeOperations;
            setPipelineTask(pipelineTask);
        }

        @Override
        AlertService alertService() {
            return mockedAlertService;
        }

        @Override
        DatastoreFileManager datastoreFileManager() {
            return mockedDatastoreFileManager;
        }

        @Override
        boolean singleSubtask() {
            return singleSubtask;
        }

        public void setSingleSubtask(boolean singleSubtask) {
            this.singleSubtask = singleSubtask;
        }

        @Override
        public PipelineTaskOperations pipelineTaskOperations() {
            return mockedOperations;
        }

        @Override
        public PipelineInstanceOperations pipelineInstanceOperations() {
            return mockedInstanceOperations;
        }

        @Override
        public PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
            return mockedInstanceNodeOperations;
        }
    }
}
