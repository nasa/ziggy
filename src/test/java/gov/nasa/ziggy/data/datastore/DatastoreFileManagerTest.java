package gov.nasa.ziggy.data.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager.InputFiles;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager.SubtaskDefinition;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerOperations;
import gov.nasa.ziggy.module.AlgorithmStateFiles;
import gov.nasa.ziggy.module.SubtaskUtils;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Unit tests for {@link DatastoreFileManager}.
 *
 * @author PT
 */
public class DatastoreFileManagerTest {

    private static final int SUBTASK_DIR_COUNT = 7;
    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootProperty = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR, ziggyDirectoryRule, "datastore");

    public ZiggyPropertyRule taskDirRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        ziggyDirectoryRule, "pipeline-results");

    @Rule
    public final RuleChain testRuleChain = RuleChain.outerRule(ziggyDirectoryRule)
        .around(datastoreRootProperty)
        .around(taskDirRule);

    private DatastoreFileManager datastoreFileManager;
    private PipelineTask pipelineTask;
    private DataFileType uncalibratedSciencePixelDataFileType;
    private DataFileType uncalibratedCollateralPixelDataFileType;
    private DataFileType allFilesAllSubtasksDataFileType;
    private DataFileType calibratedCollateralPixelDataFileType;
    private Map<String, DatastoreRegexp> regexpsByName;
    private DatastoreWalker datastoreWalker;
    private Path taskDirectory;
    private PipelineDefinitionNode pipelineDefinitionNode;
    private PipelineInstanceNode pipelineInstanceNode;
    private ModelRegistry modelRegistry;
    private ModelMetadata modelMetadata;
    private Map<String, String> regexpValueByName = new HashMap<>();
    private PipelineTaskOperations pipelineTaskOperations = mock(PipelineTaskOperations.class);
    private PipelineDefinitionOperations pipelineDefinitionOperations = mock(
        PipelineDefinitionOperations.class);
    private DatastoreProducerConsumerOperations datastoreProducerConsumerOperations = mock(
        DatastoreProducerConsumerOperations.class);
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = mock(
        PipelineDefinitionNodeOperations.class);

    @Before
    public void setUp() throws IOException {
        taskDirectory = DirectoryProperties.taskDataDir().toAbsolutePath();
        pipelineTask = mock(PipelineTask.class);
        datastoreFileManager = Mockito.spy(new DatastoreFileManager(pipelineTask, taskDirectory));
        doReturn(mock(AlertService.class)).when(datastoreFileManager).alertService();

        // Create datastore directories.
        DatastoreTestUtils.createDatastoreDirectories();

        // Get defined DataFileTypes and add file name regular expressions.
        // We use the "calibrated science pixels" to store files that we use as
        // all-files-all-subtasks files for input, in the interest of not rewriting
        // the entire DataFileUtils infrastructure.
        Map<String, DataFileType> dataFileTypes = DatastoreTestUtils.dataFileTypesByName();
        uncalibratedSciencePixelDataFileType = dataFileTypes
            .get("uncalibrated science pixel values");
        uncalibratedCollateralPixelDataFileType = dataFileTypes
            .get("uncalibrated collateral pixel values");
        allFilesAllSubtasksDataFileType = Mockito
            .spy(dataFileTypes.get("calibrated science pixel values"));
        when(allFilesAllSubtasksDataFileType.isIncludeAllFilesInAllSubtasks()).thenReturn(true);
        calibratedCollateralPixelDataFileType = dataFileTypes
            .get("calibrated collateral pixel values");

        // Construct datastore files.
        regexpsByName = DatastoreTestUtils.regexpsByName();
        datastoreWalker = new DatastoreWalker(regexpsByName,
            DatastoreTestUtils.datastoreNodesByFullPath());
        doReturn(datastoreWalker).when(datastoreFileManager).datastoreWalker();
        doReturn(pipelineTaskOperations).when(datastoreFileManager).pipelineTaskOperations();
        doReturn(pipelineDefinitionOperations).when(datastoreFileManager)
            .pipelineDefinitionOperations();
        when(pipelineDefinitionOperations.processingMode(ArgumentMatchers.anyString()))
            .thenReturn(ProcessingMode.PROCESS_ALL);
        doReturn(datastoreProducerConsumerOperations).when(datastoreFileManager)
            .datastoreProducerConsumerOperations();
        doReturn(pipelineDefinitionNodeOperations).when(datastoreFileManager)
            .pipelineDefinitionNodeOperations();

        // Construct the Map from regexp name to value. Note that we need to include the pixel type
        // in the way that DatastoreWalker would include it.
        regexpValueByName.put("sector", "sector-0002");
        regexpValueByName.put("cadenceType", "target");
        regexpValueByName.put("channel", "1:1:A");
        for (Map.Entry<String, String> regexpEntry : regexpValueByName.entrySet()) {
            regexpsByName.get(regexpEntry.getKey()).setInclude(regexpEntry.getValue());
        }
        regexpValueByName.put("pixelType", "pixelType$science");

        constructDatastoreFiles(uncalibratedSciencePixelDataFileType, SUBTASK_DIR_COUNT + 1,
            "uncalibrated-pixels-", ".science.nc");
        constructDatastoreFiles(uncalibratedCollateralPixelDataFileType, SUBTASK_DIR_COUNT,
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

        // Set up the pipeline task.
        pipelineInstanceNode = mock(PipelineInstanceNode.class);
        pipelineDefinitionNode = mock(PipelineDefinitionNode.class);
        when(pipelineDefinitionNode.getPipelineName()).thenReturn("test pipeline");
        when(pipelineTask.getModuleName()).thenReturn("test module");
        when(
            pipelineTaskOperations.pipelineDefinitionNode(ArgumentMatchers.any(PipelineTask.class)))
                .thenReturn(pipelineDefinitionNode);
        when(pipelineDefinitionNodeOperations
            .inputDataFileTypes(ArgumentMatchers.any(PipelineDefinitionNode.class)))
                .thenReturn(Set.of(uncalibratedSciencePixelDataFileType,
                    uncalibratedCollateralPixelDataFileType, allFilesAllSubtasksDataFileType));
        when(pipelineTaskOperations.modelTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(modelType));
        when(
            pipelineTaskOperations.pipelineDefinitionName(ArgumentMatchers.any(PipelineTask.class)))
                .thenReturn("test pipeline");
        when(pipelineTaskOperations.inputDataFileTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(uncalibratedSciencePixelDataFileType,
                uncalibratedCollateralPixelDataFileType, allFilesAllSubtasksDataFileType));

        when(pipelineTaskOperations.outputDataFileTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(calibratedCollateralPixelDataFileType));

        modelRegistry = mock(ModelRegistry.class);
        when(modelRegistry.getModels()).thenReturn(Map.of(modelType, modelMetadata));
        when(pipelineTaskOperations.modelRegistry(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(modelRegistry);

        // Construct the UOW.
        DatastoreDirectoryUnitOfWorkGenerator uowGenerator = Mockito
            .spy(DatastoreDirectoryUnitOfWorkGenerator.class);
        doReturn(datastoreWalker).when(uowGenerator).datastoreWalker();
        doReturn(pipelineDefinitionNodeOperations).when(uowGenerator)
            .pipelineDefinitionNodeOperations();
        doReturn(pipelineDefinitionNode).when(pipelineInstanceNode).getPipelineDefinitionNode();
        List<UnitOfWork> uows = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);
        doReturn(uows.get(0)).when(pipelineTask).getUnitOfWork();
    }

    /** Constructs a collection of zero-length files in the datastore. */
    private void constructDatastoreFiles(DataFileType dataFileType, int fileCount,
        String filenamePrefix, String filenameSuffix) throws IOException {
        Path datastorePath = datastoreWalker.pathFromLocationAndRegexpValues(regexpValueByName,
            DatastoreWalker.fullLocation(dataFileType));
        for (int fileCounter = 0; fileCounter < fileCount; fileCounter++) {
            String filename = filenamePrefix + fileCounter + filenameSuffix;
            Files.createDirectories(datastorePath);
            Files.createFile(datastorePath.resolve(filename));
        }
    }

    /**
     * Tests that the {@link DatastoreFileManager#subtaskDefinitions()} method works as expected.
     */
    @Test
    public void testFilesForSubtasks() {
        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager.subtaskDefinitions();
        Map<String, Set<Path>> filesForSubtasks = new HashMap<>();
        for (SubtaskDefinition subtaskDefinition : subtaskDefinitions) {
            filesForSubtasks.put(subtaskDefinition.mapEntry().getKey(),
                subtaskDefinition.mapEntry().getValue());
        }
        Set<String> subtaskBaseNames = filesForSubtasks.keySet();

        // Check that the base names are as expected -- the uncalibrated-pixels-7 entry
        // should not be present because it didn't have the right number of files.
        assertTrue(subtaskBaseNames.contains("uncalibrated-pixels-0"));
        assertTrue(subtaskBaseNames.contains("uncalibrated-pixels-1"));
        assertTrue(subtaskBaseNames.contains("uncalibrated-pixels-2"));
        assertTrue(subtaskBaseNames.contains("uncalibrated-pixels-3"));
        assertTrue(subtaskBaseNames.contains("uncalibrated-pixels-4"));
        assertTrue(subtaskBaseNames.contains("uncalibrated-pixels-5"));
        assertTrue(subtaskBaseNames.contains("uncalibrated-pixels-6"));
        assertEquals(SUBTASK_DIR_COUNT, filesForSubtasks.size());

        // Check that every entry in the Map has the expected data files from the DR science and
        // collateral pixels, plus the 2 files from the CAL science pixels.
        for (Map.Entry<String, Set<Path>> filesForSubtasksEntry : filesForSubtasks.entrySet()) {
            String baseName = filesForSubtasksEntry.getKey();
            Set<Path> subtaskFiles = filesForSubtasksEntry.getValue();
            checkForFiles(baseName, subtaskFiles);
        }
    }

    /** Tests that all expected files are found in a Set of Path instances. */
    private void checkForFiles(String baseName, Set<Path> subtaskFiles) {
        assertTrue(subtaskFiles.contains(DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("everyone-needs-me-0.nc")));
        assertTrue(subtaskFiles.contains(DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("everyone-needs-me-1.nc")));
        assertTrue(subtaskFiles.contains(DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve(baseName + ".science.nc")));
        assertTrue(subtaskFiles.contains(DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:A")
            .resolve(baseName + ".collateral.nc")));
    }

    /** Tests that filesForSubtasks acts as expected for a single-subtask use case. */
    @Test
    public void testFilesForSubtasksSingleSubtask() {
        when(pipelineDefinitionNode.getSingleSubtask()).thenReturn(true);
        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager.subtaskDefinitions();
        assertEquals(1, subtaskDefinitions.size());
        SubtaskDefinition subtaskDefinition = subtaskDefinitions.iterator().next();
        assertEquals("Single Subtask", subtaskDefinition.getRegexpValuesHash());
        Set<Path> files = subtaskDefinition.getSubtaskFiles();
        for (int baseNameCount = 0; baseNameCount < SUBTASK_DIR_COUNT; baseNameCount++) {
            String baseName = "uncalibrated-pixels-" + baseNameCount;
            checkForFiles(baseName, files);
        }
        assertTrue(files.contains(DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("uncalibrated-pixels-7.science.nc")));
        assertEquals(17, files.size());
    }

    @Test
    public void testSubtaskCount() {
        assertEquals(7, datastoreFileManager.subtaskCount());
    }

    @Test
    public void testModelFilesForTask() {
        Map<Path, String> modelFilesForTask = datastoreFileManager.modelTaskFilesByDatastorePath();
        assertNotNull(modelFilesForTask.get(modelMetadata.datastoreModelPath()));
        assertEquals("foo", modelFilesForTask.get(modelMetadata.datastoreModelPath()));
        assertEquals(1, modelFilesForTask.size());
    }

    @Test
    public void testCopyDatastoreFilesToTaskDirectory() {
        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager.subtaskDefinitions();
        List<Set<Path>> subtaskFiles = new ArrayList<>();
        for (SubtaskDefinition subtaskDefinition : subtaskDefinitions) {
            subtaskFiles.add(subtaskDefinition.getSubtaskFiles());
        }
        Map<Path, String> modelFilesForTask = datastoreFileManager.modelTaskFilesByDatastorePath();
        Map<Path, Set<Path>> copiedFiles = datastoreFileManager
            .copyDatastoreFilesToTaskDirectory(subtaskDefinitions, modelFilesForTask);
        Set<Path> subtaskDirs = copiedFiles.keySet();

        // We should wind up with 7 subtask directories.
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-0")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-1")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-2")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-3")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-4")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-5")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-6")));
        assertEquals(SUBTASK_DIR_COUNT, copiedFiles.size());

        // Each subtask directory should have a file for each of the files in the
        // corresponding Map value (note that the Map value is the Set of datastore
        // file paths, so we have to generate the equivalent subtask directory file
        // path and test for existence).
        for (Map.Entry<Path, Set<Path>> copiedFilesEntry : copiedFiles.entrySet()) {
            for (Path path : copiedFilesEntry.getValue()) {
                assertTrue(Files.exists(copiedFilesEntry.getKey().resolve(path.getFileName())));
            }

            // Check that each subtask's collection of datastore files matches one of
            // the ones that was produced by the copyDatastoreFilesToSubtaskDirectory method.
            assertTrue(subtaskFiles.contains(copiedFilesEntry.getValue()));
        }

        // Each subtask directory should have the test model in it, renamed to its original
        // filename ("foo").
        for (Path subtaskDir : subtaskDirs) {
            assertTrue(Files.exists(subtaskDir.resolve("foo")));
        }
    }

    @Test
    public void testCopyTaskDirectoryFilesToDatastore() throws IOException {
        createOutputFiles();
        when(pipelineDefinitionNode.getOutputDataFileTypes())
            .thenReturn(Set.of(calibratedCollateralPixelDataFileType));
        Set<Path> copiedFiles = datastoreFileManager.copyTaskDirectoryFilesToDatastore();
        Path datastorePath = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:A");
        for (int subtaskIndex = 0; subtaskIndex < SUBTASK_DIR_COUNT; subtaskIndex++) {
            assertTrue(copiedFiles.contains(
                datastorePath.toAbsolutePath().resolve("outputs-file-" + subtaskIndex + ".nc")));
        }
        assertEquals(SUBTASK_DIR_COUNT, copiedFiles.size());
    }

    private void createOutputFiles() throws IOException {
        for (int subtaskIndex = 0; subtaskIndex < SUBTASK_DIR_COUNT; subtaskIndex++) {
            SubtaskUtils.createSubtaskDirectory(taskDirectory, subtaskIndex);
            Path subtaskDir = taskDirectory.resolve(SubtaskUtils.subtaskDirName(subtaskIndex));
            Path outputsFile = subtaskDir.resolve("outputs-file-" + subtaskIndex + ".nc");
            datastoreFileManager.copyDatastoreRegexpValuesToSubtaskDir(subtaskDir,
                regexpValueByName);
            Files.createFile(outputsFile);
        }
    }

    @Test
    public void testInputFilesByOutputStatus() throws IOException {
        createOutputFiles();
        createInputFiles();
        setAlgorithmStateFiles();
        when(pipelineDefinitionNode.getOutputDataFileTypes())
            .thenReturn(Set.of(calibratedCollateralPixelDataFileType));
        Files.delete(taskDirectory.resolve(SubtaskUtils.subtaskDirName(SUBTASK_DIR_COUNT - 1))
            .resolve("outputs-file-" + (SUBTASK_DIR_COUNT - 1) + ".nc"));
        InputFiles inputFiles = datastoreFileManager.inputFilesByOutputStatus();
        Set<Path> strippedInputFilesWithOutputs = inputFiles.getFilesWithOutputs()
            .stream()
            .map(s -> DirectoryProperties.datastoreRootDir().toAbsolutePath().relativize(s))
            .collect(Collectors.toSet());
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/science/1:1:A/uncalibrated-pixels-0.science.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/science/1:1:A/uncalibrated-pixels-1.science.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/science/1:1:A/uncalibrated-pixels-2.science.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/science/1:1:A/uncalibrated-pixels-3.science.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/science/1:1:A/uncalibrated-pixels-4.science.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/science/1:1:A/uncalibrated-pixels-5.science.nc")));

        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-0.collateral.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-1.collateral.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-2.collateral.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-3.collateral.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-4.collateral.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-5.collateral.nc")));

        assertTrue(strippedInputFilesWithOutputs.contains(
            Paths.get("sector-0002/mda/cal/pixels/target/science/1:1:A/everyone-needs-me-0.nc")));
        assertTrue(strippedInputFilesWithOutputs.contains(
            Paths.get("sector-0002/mda/cal/pixels/target/science/1:1:A/everyone-needs-me-1.nc")));
        assertEquals(14, inputFiles.getFilesWithOutputs().size());

        Set<Path> strippedInputFilesWithoutOutputs = inputFiles.getFilesWithoutOutputs()
            .stream()
            .map(s -> DirectoryProperties.datastoreRootDir().toAbsolutePath().relativize(s))
            .collect(Collectors.toSet());
        assertTrue(strippedInputFilesWithoutOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/science/1:1:A/uncalibrated-pixels-6.science.nc")));
        assertTrue(strippedInputFilesWithoutOutputs.contains(Paths.get(
            "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-6.collateral.nc")));
        assertEquals(2, inputFiles.getFilesWithoutOutputs().size());
    }

    private void createInputFiles() throws IOException {
        for (int subtaskIndex = 0; subtaskIndex < SUBTASK_DIR_COUNT; subtaskIndex++) {
            Path subtaskPath = SubtaskUtils.subtaskDirectory(taskDirectory, subtaskIndex);
            Files.createFile(
                subtaskPath.resolve("uncalibrated-pixels-" + subtaskIndex + ".science.nc"));
            Files.createFile(
                subtaskPath.resolve("uncalibrated-pixels-" + subtaskIndex + ".collateral.nc"));
            Files.createFile(subtaskPath.resolve("everyone-needs-me-0.nc"));
            Files.createFile(subtaskPath.resolve("everyone-needs-me-1.nc"));
        }
    }

    private void setAlgorithmStateFiles() {
        for (int subtaskIndex = 0; subtaskIndex < SUBTASK_DIR_COUNT - 1; subtaskIndex++) {
            AlgorithmStateFiles stateFile = new AlgorithmStateFiles(
                SubtaskUtils.subtaskDirectory(taskDirectory, subtaskIndex).toFile());
            stateFile.updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
            stateFile.setOutputsFlag();
        }
        new AlgorithmStateFiles(
            SubtaskUtils.subtaskDirectory(taskDirectory, SUBTASK_DIR_COUNT - 1).toFile())
                .updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
    }

    @Test
    public void testSingleSubtaskNoPerSubtaskFiles() {
        when(pipelineDefinitionNodeOperations
            .inputDataFileTypes(ArgumentMatchers.any(PipelineDefinitionNode.class)))
                .thenReturn(Set.of(allFilesAllSubtasksDataFileType));
        doReturn(true).when(pipelineDefinitionNode).getSingleSubtask();
        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager.subtaskDefinitions();
        assertEquals(1, subtaskDefinitions.size());
        SubtaskDefinition subtaskDefinition = subtaskDefinitions.iterator().next();
        assertEquals(DatastoreFileManager.SINGLE_SUBTASK_BASE_NAME,
            subtaskDefinition.getRegexpValuesHash());
        Set<Path> paths = subtaskDefinition.getSubtaskFiles();
        assertTrue(paths.contains(DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("everyone-needs-me-0.nc")));
        assertTrue(paths.contains(DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("everyone-needs-me-1.nc")));
        assertEquals(2, paths.size());
        assertEquals(1, subtaskDefinitions.size());
    }

    @Test
    public void testFilterOutFilesAlreadyProcessed() {
        configureForFilteringTest();
        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager.subtaskDefinitions();
        Map<String, Set<Path>> filesForSubtasks = new HashMap<>();
        for (SubtaskDefinition subtaskDefinition : subtaskDefinitions) {
            filesForSubtasks.put(subtaskDefinition.mapEntry().getKey(),
                subtaskDefinition.mapEntry().getValue());
        }

        // There should only be 5 Map entries, for base names uncalibrated-pixels-2
        // through uncalibrated-pixels-6. Both of the uncalibrated data files in
        // uncalibrated-pixels-0 have been processed before. The collateral pixel file
        // for uncalibrated-pixels-1 has been processed before. The collateral pixel
        // file for uncalibrated-pixels-7 is missing.
        assertNotNull(filesForSubtasks.get("uncalibrated-pixels-2"));
        assertEquals(4, filesForSubtasks.get("uncalibrated-pixels-2").size());
        assertNotNull(filesForSubtasks.get("uncalibrated-pixels-3"));
        assertEquals(4, filesForSubtasks.get("uncalibrated-pixels-3").size());
        assertNotNull(filesForSubtasks.get("uncalibrated-pixels-4"));
        assertEquals(4, filesForSubtasks.get("uncalibrated-pixels-4").size());
        assertNotNull(filesForSubtasks.get("uncalibrated-pixels-5"));
        assertEquals(4, filesForSubtasks.get("uncalibrated-pixels-5").size());
        assertNotNull(filesForSubtasks.get("uncalibrated-pixels-6"));
        assertEquals(4, filesForSubtasks.get("uncalibrated-pixels-6").size());
        assertEquals(5, filesForSubtasks.size());
    }

    @Test
    public void testFilteringForSingleSubtask() {
        configureForFilteringTest();
        when(pipelineDefinitionNode.getSingleSubtask()).thenReturn(true);

        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager
            .subtaskDefinitions(pipelineDefinitionNode);
        assertEquals(1, subtaskDefinitions.size());
        SubtaskDefinition subtaskDefinition = subtaskDefinitions.iterator().next();
        assertEquals(DatastoreFileManager.SINGLE_SUBTASK_BASE_NAME,
            subtaskDefinition.getRegexpValuesHash());
        Set<Path> paths = subtaskDefinition.getSubtaskFiles();
        assertEquals(17, paths.size());
        assertEquals(1, subtaskDefinitions.size());
    }

    @Test
    public void testFilteringNoPriorProcessingDetected() {
        configureForFilteringTest();
        when(pipelineTaskOperations
            .tasksForPipelineDefinitionNode(ArgumentMatchers.any(PipelineTask.class)))
                .thenReturn(new ArrayList<>());
        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager.subtaskDefinitions();
        assertEquals(7, subtaskDefinitions.size());
    }

    private void configureForFilteringTest() {

        // Request processing of only new data.
        when(pipelineDefinitionOperations.processingMode(ArgumentMatchers.anyString()))
            .thenReturn(ProcessingMode.PROCESS_NEW);
        Set<String> scienceDatastoreFilenames = producerConsumerTableFilenames("science");
        Set<String> collateralDatastoreFilenames = producerConsumerTableFilenames("collateral");

        // Set up the retrieval of earlier consumer task IDs from the database.
        PipelineTask pipelineTask30 = mock(PipelineTask.class);
        when(pipelineTask30.getId()).thenReturn(30L);
        PipelineTask pipelineTask35 = mock(PipelineTask.class);
        when(pipelineTask35.getId()).thenReturn(35L);
        PipelineTask pipelineTask40 = mock(PipelineTask.class);
        when(pipelineTask40.getId()).thenReturn(40L);

        when(pipelineTaskOperations
            .tasksForPipelineDefinitionNode(ArgumentMatchers.any(PipelineTask.class)))
                .thenReturn(List.of(pipelineTask30, pipelineTask40))
                .thenReturn(List.of(pipelineTask30, pipelineTask35));

        // Set up the DatastoreProducerConsumer retieval mocks.
        when(datastoreProducerConsumerOperations.filesConsumedByTasks(
            List.of(pipelineTask30, pipelineTask40), scienceDatastoreFilenames)).thenReturn(Set.of(
                "sector-0002/mda/dr/pixels/target/science/1:1:A/uncalibrated-pixels-0.science.nc"));
        when(datastoreProducerConsumerOperations.filesConsumedByTasks(List.of(pipelineTask30,
            pipelineTask35), collateralDatastoreFilenames)).thenReturn(Set.of(
                "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-0.collateral.nc",
                "sector-0002/mda/dr/pixels/target/collateral/1:1:A/uncalibrated-pixels-1.collateral.nc"));
    }

    private Set<String> producerConsumerTableFilenames(String pixelType) {
        Path commonPath = DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target");
        return constructProducerConsumerPaths(commonPath.resolve(pixelType).resolve("1:1:A"));
    }

    private Set<String> constructProducerConsumerPaths(Path datastorePath) {
        Set<Path> dirFiles = ZiggyFileUtils.listFiles(datastorePath);
        return dirFiles.stream()
            .map(s -> DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .relativize(s)
                .toString())
            .collect(Collectors.toSet());
    }

    /** Tests that DatastoreCopyType COPY produces a recursive copy of a directory. */
    @Test
    public void testCopy() throws IOException {
        ZiggyFileUtils.CopyType.COPY.copy(DirectoryProperties.datastoreRootDir(),
            ziggyDirectoryRule.directory().resolve("copydir"));
        assertTrue(Files.isDirectory(ziggyDirectoryRule.directory().resolve("copydir")));
        assertTrue(Files.isDirectory(DirectoryProperties.datastoreRootDir()));
        assertFalse(Files.isSameFile(ziggyDirectoryRule.directory().resolve("copydir"),
            DirectoryProperties.datastoreRootDir()));
        assertFalse(Files.isSymbolicLink(ziggyDirectoryRule.directory().resolve("copydir")));
        Path copiedFile = ziggyDirectoryRule.directory()
            .resolve("copydir")
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("uncalibrated-pixels-0.science.nc");
        assertTrue(Files.isRegularFile(copiedFile));
        assertFalse(Files.isSymbolicLink(copiedFile));
        Path originalFile = DirectoryProperties.datastoreRootDir()
            .resolve(ziggyDirectoryRule.directory().resolve("copydir").relativize(copiedFile));
        assertTrue(Files.isRegularFile(originalFile));
        assertFalse(Files.isSameFile(copiedFile, originalFile));
    }

    /** Tests that DatastoreCopyType MOVE moves a file or directory to a new location. */
    @Test
    public void testMove() {
        ZiggyFileUtils.CopyType.MOVE.copy(DirectoryProperties.datastoreRootDir(),
            ziggyDirectoryRule.directory().resolve("copydir"));
        assertTrue(Files.isDirectory(ziggyDirectoryRule.directory().resolve("copydir")));
        assertFalse(Files.exists(DirectoryProperties.datastoreRootDir()));
        assertFalse(Files.isSymbolicLink(ziggyDirectoryRule.directory().resolve("copydir")));
        Path copiedFile = ziggyDirectoryRule.directory()
            .resolve("copydir")
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("uncalibrated-pixels-0.science.nc");
        assertTrue(Files.isRegularFile(copiedFile));
        assertFalse(Files.isSymbolicLink(copiedFile));
    }

    /**
     * Tests that DatastoreCopyType LINK produces a hard link of a file to a new location, and
     * produces copies of directories (which cannot be hard link targets).
     */
    @Test
    public void testLink() throws IOException {
        ZiggyFileUtils.CopyType.LINK.copy(DirectoryProperties.datastoreRootDir(),
            ziggyDirectoryRule.directory().resolve("copydir"));
        assertTrue(Files.isDirectory(ziggyDirectoryRule.directory().resolve("copydir")));
        assertTrue(Files.isDirectory(DirectoryProperties.datastoreRootDir()));
        assertFalse(Files.isSameFile(ziggyDirectoryRule.directory().resolve("copydir"),
            DirectoryProperties.datastoreRootDir()));
        assertFalse(Files.isSymbolicLink(ziggyDirectoryRule.directory().resolve("copydir")));
        Path copiedFile = ziggyDirectoryRule.directory()
            .resolve("copydir")
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("uncalibrated-pixels-0.science.nc");
        assertTrue(Files.isRegularFile(copiedFile));
        assertFalse(Files.isSymbolicLink(copiedFile));
        Path originalFile = DirectoryProperties.datastoreRootDir()
            .resolve(ziggyDirectoryRule.directory().resolve("copydir").relativize(copiedFile));
        assertTrue(Files.isRegularFile(originalFile));
        assertTrue(Files.isSameFile(copiedFile, originalFile));
    }
}
