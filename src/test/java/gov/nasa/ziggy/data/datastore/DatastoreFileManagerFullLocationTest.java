package gov.nasa.ziggy.data.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import gov.nasa.ziggy.data.datastore.DatastoreFileManager.SubtaskDefinition;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionProcessingOptions.ProcessingMode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Unit tests for {@link DatastoreFileManager} for cases in which the data file types have a full
 * location that is longer than their location (i.e., we want the UOW to get input files from
 * multiple datastore directories under the data file type location).
 *
 * @author PT
 */
public class DatastoreFileManagerFullLocationTest {

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

    private PipelineTaskOperations pipelineTaskOperations = Mockito
        .mock(PipelineTaskOperations.class);
    private PipelineDefinitionOperations pipelineDefinitionOperations = Mockito
        .mock(PipelineDefinitionOperations.class);
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = Mockito
        .mock(PipelineDefinitionNodeOperations.class);

    @Before
    public void setUp() throws IOException {
        taskDirectory = DirectoryProperties.taskDataDir().toAbsolutePath();
        pipelineTask = Mockito.mock(PipelineTask.class);
        datastoreFileManager = Mockito.spy(new DatastoreFileManager(pipelineTask, taskDirectory));
        Mockito.doReturn(Mockito.mock(AlertService.class))
            .when(datastoreFileManager)
            .alertService();

        // Create datastore directories.
        DatastoreTestUtils.createDatastoreDirectories();

        // Get defined DataFileTypes and add file name regular expressions.
        // We use the "calibrated science pixels" to store files that we use as
        // all-files-all-subtasks files for input, in the interest of not rewriting
        // the entire DataFileUtils infrastructure.
        Map<String, DataFileType> dataFileTypes = DatastoreTestUtils
            .dataFileTypesByNameRegexpsInFileName();
        uncalibratedSciencePixelDataFileType = dataFileTypes
            .get("uncalibrated science pixel values");
        uncalibratedCollateralPixelDataFileType = dataFileTypes
            .get("uncalibrated collateral pixel values");
        allFilesAllSubtasksDataFileType = Mockito
            .spy(dataFileTypes.get("calibrated science pixel values"));
        Mockito.when(allFilesAllSubtasksDataFileType.isIncludeAllFilesInAllSubtasks())
            .thenReturn(true);
        calibratedCollateralPixelDataFileType = dataFileTypes
            .get("calibrated collateral pixel values");

        // Construct datastore files.
        regexpsByName = DatastoreTestUtils.regexpsByName();
        datastoreWalker = new DatastoreWalker(regexpsByName,
            DatastoreTestUtils.datastoreNodesByFullPath());
        Mockito.doReturn(datastoreWalker).when(datastoreFileManager).datastoreWalker();
        Mockito.doReturn(pipelineTaskOperations)
            .when(datastoreFileManager)
            .pipelineTaskOperations();
        Mockito
            .when(pipelineTaskOperations
                .pipelineDefinitionName(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn("test pipeline");
        Mockito.doReturn(pipelineDefinitionOperations)
            .when(datastoreFileManager)
            .pipelineDefinitionOperations();
        Mockito
            .when(pipelineDefinitionOperations.processingMode(ArgumentMatchers.any(String.class)))
            .thenReturn(ProcessingMode.PROCESS_ALL);
        Mockito.doReturn(pipelineDefinitionNodeOperations)
            .when(datastoreFileManager)
            .pipelineDefinitionNodeOperations();

        // Construct the Map from regexp name to value. Note that we need to include the pixel type
        // in the way that DatastoreWalker would include it.
        regexpValueByName.put("sector", "sector-0002");
        regexpValueByName.put("cadenceType", "target");
        for (Map.Entry<String, String> regexpEntry : regexpValueByName.entrySet()) {
            regexpsByName.get(regexpEntry.getKey()).setInclude(regexpEntry.getValue());
        }
        regexpValueByName.put("pixelType", "pixelType$science");
        regexpValueByName.put("channel", "1:1:A");
        constructDatastoreFiles(uncalibratedSciencePixelDataFileType, SUBTASK_DIR_COUNT + 1,
            "uncalibrated-pixels-", ".science.nc");
        constructDatastoreFiles(uncalibratedCollateralPixelDataFileType, SUBTASK_DIR_COUNT,
            "uncalibrated-pixels-", ".collateral.nc");
        constructDatastoreFiles(allFilesAllSubtasksDataFileType, 2, "everyone-needs-me-", ".nc");

        regexpValueByName.put("channel", "1:1:B");
        constructDatastoreFiles(uncalibratedSciencePixelDataFileType, SUBTASK_DIR_COUNT + 1,
            "uncalibrated-pixels-", ".science.nc");
        constructDatastoreFiles(uncalibratedCollateralPixelDataFileType, SUBTASK_DIR_COUNT,
            "uncalibrated-pixels-", ".collateral.nc");

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
        pipelineInstanceNode = Mockito.mock(PipelineInstanceNode.class);
        pipelineDefinitionNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito
            .when(pipelineTaskOperations
                .pipelineDefinitionNode(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(pipelineDefinitionNode);
        Mockito.when(pipelineDefinitionNode.getPipelineName()).thenReturn("test pipeline");
        Mockito
            .when(
                pipelineTaskOperations.inputDataFileTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(uncalibratedSciencePixelDataFileType,
                uncalibratedCollateralPixelDataFileType, allFilesAllSubtasksDataFileType));

        Mockito
            .when(pipelineTaskOperations
                .outputDataFileTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(calibratedCollateralPixelDataFileType));
        modelRegistry = Mockito.mock(ModelRegistry.class);
        Mockito.when(pipelineTaskOperations.modelRegistry(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(modelRegistry);
        Mockito.when(modelRegistry.getModels()).thenReturn(Map.of(modelType, modelMetadata));
        Mockito.when(pipelineTaskOperations.modelTypes(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(Set.of(modelType));
        Mockito
            .when(pipelineDefinitionNodeOperations
                .inputDataFileTypes(ArgumentMatchers.any(PipelineDefinitionNode.class)))
            .thenReturn(Set.of(uncalibratedSciencePixelDataFileType,
                uncalibratedCollateralPixelDataFileType, allFilesAllSubtasksDataFileType));

        // Construct the UOW.
        DatastoreDirectoryUnitOfWorkGenerator uowGenerator = Mockito
            .spy(DatastoreDirectoryUnitOfWorkGenerator.class);
        Mockito.doReturn(datastoreWalker).when(uowGenerator).datastoreWalker();
        Mockito.when(pipelineInstanceNode.getPipelineDefinitionNode())
            .thenReturn(pipelineDefinitionNode);
        Mockito.when(uowGenerator.pipelineDefinitionNodeOperations())
            .thenReturn(pipelineDefinitionNodeOperations);

        List<UnitOfWork> uows = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);
        Mockito.doReturn(uows.get(0)).when(pipelineTask).getUnitOfWork();
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
     * Tests that datastore files are correctly copied to the task directory. We expect to see 14
     * subtasks, where both 1:1:A and 1:1:B datastore files are included in the subtasks.
     */
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

        // We should wind up with 14 subtask directories.
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-0")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-1")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-2")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-3")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-4")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-5")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-6")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-7")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-8")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-9")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-10")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-11")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-12")));
        assertTrue(subtaskDirs.contains(taskDirectory.resolve("st-13")));
        assertEquals(2 * SUBTASK_DIR_COUNT, copiedFiles.size());

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

        // Test that the subtask directories all have the correct files in them.
        testSubtaskDirectoryContents(copiedFiles, Set.of("1:1:A", "1:1:B"));
    }

    /**
     * Tests that all the expected files are present in the subtask directories, with the expected
     * organization.
     */
    private void testSubtaskDirectoryContents(Map<Path, Set<Path>> subtaskFilesBySubtaskDir,
        Set<String> channels) {

        Map<Path, Set<Path>> relativizedSubtaskFilesBySubtaskDir = new HashMap<>();
        Path relativizationPath = DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda");
        for (Map.Entry<Path, Set<Path>> entry : subtaskFilesBySubtaskDir.entrySet()) {
            Set<Path> relativizedPaths = new HashSet<>();
            for (Path path : entry.getValue()) {
                relativizedPaths.add(relativizationPath.relativize(path.toAbsolutePath()));
            }
            relativizedSubtaskFilesBySubtaskDir.put(entry.getKey(), relativizedPaths);
        }
        Path everyoneNeedsMe1 = Paths.get("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("everyone-needs-me-1.nc");
        Path everyoneNeedsMe0 = Paths.get("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .resolve("everyone-needs-me-0.nc");

        for (int dataFileIndex = 0; dataFileIndex < SUBTASK_DIR_COUNT; dataFileIndex++) {
            for (String channel : channels) {
                Path uncalibratedScienceFile = Paths.get("dr")
                    .resolve("pixels")
                    .resolve("target")
                    .resolve("science")
                    .resolve(channel)
                    .resolve("uncalibrated-pixels-" + dataFileIndex + ".science.nc");
                Path uncalibratedCollateralFile = Paths.get("dr")
                    .resolve("pixels")
                    .resolve("target")
                    .resolve("collateral")
                    .resolve(channel)
                    .resolve("uncalibrated-pixels-" + dataFileIndex + ".collateral.nc");

                // Find the correct subtask
                boolean correctSubtaskFound = false;
                for (Map.Entry<Path, Set<Path>> entry : relativizedSubtaskFilesBySubtaskDir
                    .entrySet()) {
                    Set<Path> subtaskPaths = entry.getValue();
                    if (subtaskPaths.contains(uncalibratedScienceFile)) {
                        assertTrue(subtaskPaths.contains(uncalibratedCollateralFile));
                        assertTrue(subtaskPaths.contains(everyoneNeedsMe0));
                        assertTrue(subtaskPaths.contains(everyoneNeedsMe1));
                        correctSubtaskFound = true;
                        Map<String, String> regexpValues = datastoreFileManager
                            .copyDatastoreRegexpValuesFromSubtaskDir(entry.getKey());
                        assertEquals("sector-0002", regexpValues.get("sector"));
                        assertEquals("target", regexpValues.get("cadenceType"));
                        assertEquals(channel, regexpValues.get("channel"));
                        assertEquals(3, regexpValues.size());
                        break;
                    }
                }
                assertTrue(correctSubtaskFound);
            }
        }
    }

    @Test
    public void testCopyTaskDirectoryFilesToDatastore() throws IOException {

        // Start by populating the subtask directories.
        Set<SubtaskDefinition> subtaskDefinitions = datastoreFileManager.subtaskDefinitions();
        List<Set<Path>> subtaskFiles = new ArrayList<>();
        for (SubtaskDefinition subtaskDefinition : subtaskDefinitions) {
            subtaskFiles.add(subtaskDefinition.getSubtaskFiles());
        }
        Map<Path, String> modelFilesForTask = datastoreFileManager.modelTaskFilesByDatastorePath();
        Map<Path, Set<Path>> copiedFiles = datastoreFileManager
            .copyDatastoreFilesToTaskDirectory(subtaskDefinitions, modelFilesForTask);

        // Add output files to the directories.
        constructOutputFiles(copiedFiles.keySet());

        Set<Path> copiedOutputFiles = datastoreFileManager.copyTaskDirectoryFilesToDatastore();
        assertEquals(14, copiedOutputFiles.size());

        // Make sure that every file got copied according to the regexp values in the
        // subtask directory.
        Path datastoreParentPath = DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral");
        for (Path subtaskDir : copiedFiles.keySet()) {
            Map<String, String> regexpValues = datastoreFileManager
                .copyDatastoreRegexpValuesFromSubtaskDir(subtaskDir);
            Path outputFile = ZiggyFileUtils
                .listFiles(subtaskDir,
                    DatastoreWalker.fileNameRegexpBaseName(calibratedCollateralPixelDataFileType))
                .iterator()
                .next();
            Path absoluteOutputPath = datastoreParentPath.resolve(regexpValues.get("channel"))
                .resolve(outputFile.getFileName());
            assertTrue(copiedOutputFiles.contains(absoluteOutputPath));
        }

        // Make sure every file that was supposed to wind up in the datastore, did so.
        for (Path copiedOutputFile : copiedOutputFiles) {
            assertTrue(Files.exists(copiedOutputFile));
        }
    }

    private void constructOutputFiles(Set<Path> subtaskDirs) throws IOException {
        int outputCounter = 0;
        for (Path path : subtaskDirs) {
            Files.createDirectories(path.resolve("outputs-file-" + outputCounter + ".nc"));
            outputCounter++;
        }
    }

    /**
     * Tests that applying include / exclude regexps to datastore nodes in the full location but not
     * in the location correctly limits the subtasks that are generated.
     */
    @Test
    public void testSublocationsLimitedByRegexp() {
        regexpsByName.get("channel").setInclude("1:1:A");
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

        // Test that the subtask directories all have the correct files in them.
        testSubtaskDirectoryContents(copiedFiles, Set.of("1:1:A"));
    }
}
