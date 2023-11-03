package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_TEST_WORKING_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.data.management.DataFileManager;
import gov.nasa.ziggy.data.management.DataFileType;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumer;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.models.ModelImporter;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.TaskConfigurationParameters;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Unit test class for DefaultPipelineInputs.
 *
 * @author PT
 */
public class DefaultPipelineInputsTest {

    private DataFileType fluxDataFileType, centroidDataFileType;
    private DataFileType resultsDataFileType;
    private PipelineTask pipelineTask;
    private PipelineDefinitionNode pipelineDefinitionNode;
    private PipelineInstance pipelineInstance;
    private PipelineInstanceNode pipelineInstanceNode;
    private File datastore;
    private File taskWorkspace;
    private File taskDir;
    private DataFileManager mockedDataFileManager;
    private DefaultPipelineInputs defaultPipelineInputs;
    private ModelType modelType1, modelType2, modelType3;
    private ModelRegistry modelRegistry;
    private Set<ModelType> modelTypes;
    private UnitOfWork uow;
    private File dataDir;
    private AlertService alertService;
    private DatastoreProducerConsumerCrud datastoreProducerConsumerCrud;
    private PipelineTaskCrud pipelineTaskCrud;
    private TaskConfigurationParameters taskConfigurationParameters;
    private Set<Path> centroidPaths;
    private Set<Path> fluxPaths;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, directoryRule, "datastore");

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR, (String) null);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(datastoreRootDirPropertyRule);

    @Before
    public void setup() throws IOException {

        uow = new UnitOfWork();
        uow.addParameter(new TypedParameter(UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
            DatastoreDirectoryUnitOfWorkGenerator.class.getCanonicalName(),
            ZiggyDataType.ZIGGY_STRING));
        uow.addParameter(new TypedParameter(DirectoryUnitOfWorkGenerator.DIRECTORY_PROPERTY_NAME,
            "sector-0001/ccd-1:1/pa", ZiggyDataType.ZIGGY_STRING));
        uow.addParameter(new TypedParameter(UnitOfWork.BRIEF_STATE_PARAMETER_NAME,
            "sector-0001/ccd-1:1/pa", ZiggyDataType.ZIGGY_STRING));
        uow.addParameter(
            new TypedParameter(DatastoreDirectoryUnitOfWorkGenerator.SINGLE_SUBTASK_PROPERTY_NAME,
                Boolean.toString(false), ZiggyDataType.ZIGGY_BOOLEAN));

        datastore = new File(datastoreRootDirPropertyRule.getProperty());
        // Set up a temporary directory for the datastore and one for the task-directory
        dataDir = new File(datastore, "sector-0001/ccd-1:1/pa");
        dataDir.mkdirs();
        taskWorkspace = directoryRule.directory().resolve("taskspace").toFile();
        taskDir = new File(taskWorkspace, "10-20-csci");
        taskDir.mkdirs();

        // Set up the data file types
        initializeDataFileTypes();

        // set up the model registry and model files
        initializeModelRegistry();

        // Set up a mocked TaskConfigurationParameters instance
        taskConfigurationParameters = Mockito.mock(TaskConfigurationParameters.class);
        Mockito.when(taskConfigurationParameters.isReprocess()).thenReturn(true);

        // Set up a dummied PipelineTask and a dummied PipelineDefinitionNode
        pipelineTask = Mockito.mock(PipelineTask.class);
        pipelineDefinitionNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(pipelineDefinitionNode);
        Mockito.when(pipelineDefinitionNode.getInputDataFileTypes())
            .thenReturn(Sets.newHashSet(fluxDataFileType, centroidDataFileType));
        Mockito.when(pipelineDefinitionNode.getOutputDataFileTypes())
            .thenReturn(Sets.newHashSet(resultsDataFileType));
        Mockito.when(pipelineTask.getModuleName()).thenReturn("csci");
        Mockito.when(pipelineDefinitionNode.getModelTypes()).thenReturn(modelTypes);
        Mockito
            .when(
                pipelineTask.getParameters(ArgumentMatchers.eq(TaskConfigurationParameters.class)))
            .thenReturn(taskConfigurationParameters);
        Mockito
            .when(pipelineTask.getParameters(ArgumentMatchers.eq(TaskConfigurationParameters.class),
                ArgumentMatchers.anyBoolean()))
            .thenReturn(taskConfigurationParameters);

        // Set up the dummied PipelineInstance
        pipelineInstance = Mockito.mock(PipelineInstance.class);
        Mockito.when(pipelineTask.getPipelineInstance()).thenReturn(pipelineInstance);
        Mockito.when(pipelineInstance.getPipelineParameterSets()).thenReturn(parametersMap());
        Mockito.when(pipelineInstance.getModelRegistry()).thenReturn(modelRegistry);

        // Set up the dummied PipelineInstanceNode
        pipelineInstanceNode = Mockito.mock(PipelineInstanceNode.class);
        Mockito.when(pipelineTask.getPipelineInstanceNode()).thenReturn(pipelineInstanceNode);
        Mockito.when(pipelineInstanceNode.getModuleParameterSets()).thenReturn(new HashMap<>());

        // Create some "data files" for the process
        initializeDataFiles();

        // We need an instance of DatastoreProducerConsumerCrud that's mocked
        datastoreProducerConsumerCrud = Mockito.mock(DatastoreProducerConsumerCrud.class);

        // Also an instance of PipelineTaskCrud that's mocked
        pipelineTaskCrud = Mockito.mock(PipelineTaskCrud.class);

        // We need a DataFileManager that's had its crud methods mocked out
        mockedDataFileManager = new DataFileManager(datastore.toPath(), taskDir.toPath(),
            pipelineTask);
        mockedDataFileManager = Mockito.spy(mockedDataFileManager);
        Mockito.when(mockedDataFileManager.datastoreProducerConsumerCrud())
            .thenReturn(datastoreProducerConsumerCrud);
        Mockito.when(mockedDataFileManager.pipelineTaskCrud()).thenReturn(pipelineTaskCrud);

        // We need a mocked AlertService.
        AlertService alertService = Mockito.mock(AlertService.class);

        // We can't use a Spy on the DefaultPipelineInputs instance because it has to get
        // serialized via HDF5, and the HDF5 module interface can't figure out how to do that
        // for a mocked object. Instead we resort to the tried-and-true approach of a
        // constructor that takes as argument the objects we want to replace.
        defaultPipelineInputs = new DefaultPipelineInputs(mockedDataFileManager, alertService);
    }

    /**
     * Exercises the copyDatastoreFilesToTaskDirectory() method for the case of multiple subtasks.
     */
    @Test
    public void testCopyDatastoreFilesToTaskDirectory() throws IOException {

        performCopyToTaskDir(false);

        // Let's see what wound up in the task directory!
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(taskDir.toPath())) {
            List<String> taskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());

            // Should be 2 sub-directories
            assertTrue(taskDirFileNames.contains("st-0"));
            assertTrue(taskDirFileNames.contains("st-1"));

            // Should be 4 data files
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));

            // Should be 2 model files, both with their original file names
            assertTrue(taskDirFileNames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertTrue(taskDirFileNames.contains("calibration-4.12.9.h5"));

            // Should be an HDF5 file of the partial inputs
            assertTrue(taskDirFileNames.contains("csci-inputs.h5"));
        }

        // Load the HDF5 file
        Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();
        DefaultPipelineInputs storedInputs = new DefaultPipelineInputs();
        hdf5ModuleInterface.readFile(new File(taskDir, "csci-inputs.h5"), storedInputs, true);

        List<ParametersInterface> pars = storedInputs.getModuleParameters().getModuleParameters();
        assertEquals(2, pars.size());
        if (pars.get(0) instanceof Params1) {
            assertTrue(pars.get(1) instanceof Params2);
        } else {
            assertTrue(pars.get(0) instanceof Params2);
            assertTrue(pars.get(1) instanceof Params1);
        }

        List<DataFileType> outputTypes = storedInputs.getOutputDataFileTypes();
        assertEquals(1, outputTypes.size());
        assertEquals("results", outputTypes.get(0).getName());

        List<String> modelFilenames = storedInputs.getModelFilenames();
        assertEquals(2, modelFilenames.size());
        assertTrue(modelFilenames.contains("tess2020234101112-12345_023-geometry.xml"));
        assertTrue(modelFilenames.contains("calibration-4.12.9.h5"));
    }

    @Test
    public void testCopyDatastoreFilesMissingFiles() throws IOException {

        // Delete one of the flux files from the datastore
        new File(dataDir, "001234567.flux.h5").delete();
        performCopyToTaskDir(false);

        // Let's see what wound up in the task directory!
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(taskDir.toPath())) {
            List<String> taskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());

            // Should be 1 sub-directory
            assertTrue(taskDirFileNames.contains("st-0"));
            assertFalse(taskDirFileNames.contains("st-1"));

            // Should be 2 data files
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));
            assertFalse(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
            assertFalse(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
        }
    }

    /**
     * Tests the situation in which the user wants to perform keep-up processing (i.e., do not
     * reprocess any files that were already processed), but none of the files in the datastore have
     * already been processed (i.e., keep-up processing requires all files to be processed anyway).
     */
    @Test
    public void testDatastoreKeepUpProcessingNoOldFiles() throws IOException {

        // We don't want to reprocess.
        Mockito.when(taskConfigurationParameters.isReprocess()).thenReturn(false);

        // We do want to return a set of DatastoreProducerConsumer instances.
        Mockito
            .when(datastoreProducerConsumerCrud.retrieveByFilename(ArgumentMatchers.eq(fluxPaths)))
            .thenReturn(fluxDatastoreProducerConsumers());
        Mockito
            .when(datastoreProducerConsumerCrud
                .retrieveByFilename(ArgumentMatchers.eq(centroidPaths)))
            .thenReturn(centroidDatastoreProducerConsumers());

        // None of the consumers will have the correct pipeline definition node.
        Mockito
            .when(pipelineTaskCrud.retrieveIdsForPipelineDefinitionNode(ArgumentMatchers.anySet(),
                ArgumentMatchers.eq(pipelineDefinitionNode)))
            .thenReturn(new ArrayList<>());

        // Perform the copy
        performCopyToTaskDir(false);

        // Let's see what wound up in the task directory!
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(taskDir.toPath())) {
            List<String> taskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());

            // Should be 2 sub-directories
            assertTrue(taskDirFileNames.contains("st-0"));
            assertTrue(taskDirFileNames.contains("st-1"));

            // Should be 4 data files
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));

            // Should be 2 model files, both with their original file names
            assertTrue(taskDirFileNames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertTrue(taskDirFileNames.contains("calibration-4.12.9.h5"));

            // Should be an HDF5 file of the partial inputs
            assertTrue(taskDirFileNames.contains("csci-inputs.h5"));
        }
    }

    /**
     * Constructs a {@link List} of {@link DatastoreProducerConsumer} instances for the data files
     * used in the unit tests.
     */
    private List<DatastoreProducerConsumer> fluxDatastoreProducerConsumers() {

        List<DatastoreProducerConsumer> datastoreProducerConsumers = new ArrayList<>();

        // Add producer-consumer instances for each data file.
        datastoreProducerConsumers
            .add(new DatastoreProducerConsumer(5L, "sector-0001/ccd-1:1/pa/001234567.flux.h5",
                DatastoreProducerConsumer.DataReceiptFileType.DATA));
        datastoreProducerConsumers
            .add(new DatastoreProducerConsumer(5L, "sector-0001/ccd-1:1/pa/765432100.flux.h5",
                DatastoreProducerConsumer.DataReceiptFileType.DATA));

        // Set the 001234567 files to have one consumer, the 765432100 files to have a
        // different one.
        datastoreProducerConsumers.get(0).addConsumer(6L);
        datastoreProducerConsumers.get(1).addConsumer(7L);

        return datastoreProducerConsumers;
    }

    /**
     * Constructs a {@link List} of {@link DatastoreProducerConsumer} instances for the data files
     * used in the unit tests.
     */
    private List<DatastoreProducerConsumer> centroidDatastoreProducerConsumers() {

        List<DatastoreProducerConsumer> datastoreProducerConsumers = new ArrayList<>();

        // Add producer-consumer instances for each data file.
        datastoreProducerConsumers
            .add(new DatastoreProducerConsumer(5L, "sector-0001/ccd-1:1/pa/001234567.centroid.h5",
                DatastoreProducerConsumer.DataReceiptFileType.DATA));
        datastoreProducerConsumers
            .add(new DatastoreProducerConsumer(5L, "sector-0001/ccd-1:1/pa/765432100.centroid.h5",
                DatastoreProducerConsumer.DataReceiptFileType.DATA));

        // Set the 001234567 files to have one consumer, the 765432100 files to have a
        // different one.
        datastoreProducerConsumers.get(0).addConsumer(6L);
        datastoreProducerConsumers.get(1).addConsumer(7L);

        return datastoreProducerConsumers;
    }

    /**
     * Tests the situation in which the user wants to perform keep-up processing (i.e., do not
     * reprocess any files that were already processed), and there are some files that have already
     * been processed and don't need to be processed again.
     */
    @Test
    public void testDatastoreKeepUpProcessing() throws IOException {

        // We don't want to reprocess.
        Mockito.when(taskConfigurationParameters.isReprocess()).thenReturn(false);

        // We do want to return a set of DatastoreProducerConsumer instances.
        Mockito
            .when(datastoreProducerConsumerCrud.retrieveByFilename(ArgumentMatchers.eq(fluxPaths)))
            .thenReturn(fluxDatastoreProducerConsumers());
        Mockito
            .when(datastoreProducerConsumerCrud
                .retrieveByFilename(ArgumentMatchers.eq(centroidPaths)))
            .thenReturn(centroidDatastoreProducerConsumers());

        // None of the consumers will have the correct pipeline definition node.
        Mockito
            .when(pipelineTaskCrud.retrieveIdsForPipelineDefinitionNode(ArgumentMatchers.anySet(),
                ArgumentMatchers.eq(pipelineDefinitionNode)))
            .thenReturn(List.of(6L));

        // Perform the copy
        performCopyToTaskDir(false);

        // Let's see what wound up in the task directory!
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(taskDir.toPath())) {
            List<String> taskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());

            // Should be 2 data files
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));

            // Should be 1 sub-directory
            assertTrue(taskDirFileNames.contains("st-0"));
            assertFalse(taskDirFileNames.contains("st-1"));

            // Should be 2 model files, both with their original file names
            assertTrue(taskDirFileNames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertTrue(taskDirFileNames.contains("calibration-4.12.9.h5"));

            // Should be an HDF5 file of the partial inputs
            assertTrue(taskDirFileNames.contains("csci-inputs.h5"));
        }
    }

    @Test
    public void testFindDatastoreFilesForInputs() {

        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(uow);

        Set<Path> paths = defaultPipelineInputs.findDatastoreFilesForInputs(pipelineTask);
        assertEquals(4, paths.size());
        Set<String> filenames = paths.stream()
            .map(s -> s.getFileName().toString())
            .collect(Collectors.toSet());
        assertTrue(filenames.contains("001234567.flux.h5"));
        assertTrue(filenames.contains("765432100.flux.h5"));
        assertTrue(filenames.contains("001234567.centroid.h5"));
        assertTrue(filenames.contains("765432100.centroid.h5"));
    }

    @Test
    public void testSubtaskInformation() {

        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(uow);

        SubtaskInformation subtaskInformation = defaultPipelineInputs
            .subtaskInformation(pipelineTask);
        assertEquals("csci", subtaskInformation.getModuleName());
        assertEquals("sector-0001/ccd-1:1/pa", subtaskInformation.getUowBriefState());
        assertEquals(2, subtaskInformation.getSubtaskCount());
        assertEquals(2, subtaskInformation.getMaxParallelSubtasks());

        TypedParameter singleSubtask = uow
            .getParameter(DatastoreDirectoryUnitOfWorkGenerator.SINGLE_SUBTASK_PROPERTY_NAME);
        singleSubtask.setValue(Boolean.TRUE);
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(uow);
        subtaskInformation = defaultPipelineInputs.subtaskInformation(pipelineTask);
        assertEquals("csci", subtaskInformation.getModuleName());
        assertEquals("sector-0001/ccd-1:1/pa", subtaskInformation.getUowBriefState());
        assertEquals(1, subtaskInformation.getSubtaskCount());
        assertEquals(1, subtaskInformation.getMaxParallelSubtasks());
    }

    /**
     * Exercises the copyDatastoreFilesToTaskDirectory() method for the case of a single subtask.
     */
    @Test
    public void testCopyDatastoreFilesToDirectorySingleSubtask() throws IOException {

        performCopyToTaskDir(true);

        // Let's see what wound up in the task directory!
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(taskDir.toPath())) {
            List<String> taskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());

            // Should be 1 sub-directories
            assertTrue(taskDirFileNames.contains("st-0"));

            // Should be 4 data files
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));

            // Should be 2 model files, both with their original file names
            assertTrue(taskDirFileNames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertTrue(taskDirFileNames.contains("calibration-4.12.9.h5"));

            // Should be an HDF5 file of the partial inputs
            assertTrue(taskDirFileNames.contains("csci-inputs.h5"));

            // Load the HDF5 file
            Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();
            DefaultPipelineInputs storedInputs = new DefaultPipelineInputs();
            hdf5ModuleInterface.readFile(new File(taskDir, "csci-inputs.h5"), storedInputs, true);

            List<ParametersInterface> pars = storedInputs.getModuleParameters()
                .getModuleParameters();
            assertEquals(2, pars.size());
            if (pars.get(0) instanceof Params1) {
                assertTrue(pars.get(1) instanceof Params2);
            } else {
                assertTrue(pars.get(0) instanceof Params2);
                assertTrue(pars.get(1) instanceof Params1);
            }

            List<DataFileType> outputTypes = storedInputs.getOutputDataFileTypes();
            assertEquals(1, outputTypes.size());
            assertEquals("results", outputTypes.get(0).getName());

            List<String> modelFilenames = storedInputs.getModelFilenames();
            assertEquals(2, modelFilenames.size());
            assertTrue(modelFilenames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertTrue(modelFilenames.contains("calibration-4.12.9.h5"));
        }
    }

    /**
     * Tests that populateSubTaskInputs() works correctly when multiple subtasks are specified.
     */
    @Test
    public void testPopulateSubTaskInputs() throws IOException {

        performCopyToTaskDir(false);

        // move to the st-0 subtask directory
        Path subtaskDir = Paths.get(taskDir.getAbsolutePath(), "st-0");
        System.setProperty(ZIGGY_TEST_WORKING_DIR.property(), subtaskDir.toString());

        new DefaultPipelineInputs(mockedDataFileManager, alertService).populateSubTaskInputs();

        String subtask0DataFiles = null;
        // Let's see what wound up in the subtask directory!
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(subtaskDir)) {
            List<String> subtaskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());
            assertTrue(subtaskDirFileNames.contains("csci-inputs.h5"));
            if (subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5")) {
                subtask0DataFiles = "001234567";
                assertTrue(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
                assertFalse(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
                assertFalse(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));
            } else {
                subtask0DataFiles = "765432100";
                assertFalse(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
                assertFalse(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
                assertTrue(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
                assertTrue(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));
            }
            assertTrue(subtaskDirFileNames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertTrue(subtaskDirFileNames.contains("calibration-4.12.9.h5"));
        }

        // Now do the same thing in the st-1 directory to make sure that all of the
        // data is getting processed someplace

        subtaskDir = Paths.get(taskDir.getAbsolutePath(), "st-1");
        System.setProperty(ZIGGY_TEST_WORKING_DIR.property(), subtaskDir.toString());

        new DefaultPipelineInputs(mockedDataFileManager, alertService).populateSubTaskInputs();

        // Let's see what wound up in the subtask directory!
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(subtaskDir)) {
            List<String> subtaskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());
            if (subtask0DataFiles.equals("001234567")) {
                assertFalse(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
                assertFalse(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
                assertTrue(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
                assertTrue(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));
            } else {
                assertTrue(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
                assertTrue(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
                assertFalse(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
                assertFalse(
                    subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));
            }
            assertTrue(subtaskDirFileNames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertTrue(subtaskDirFileNames.contains("calibration-4.12.9.h5"));
        }
    }

    /**
     * Tests that populateSubTaskInputs() works correctly in the single-subtask use case.
     */
    @Test
    public void testPopulateSubTaskInputsSingleSubtask() throws IOException {

        performCopyToTaskDir(true);

        // move to the st-0 subtask directory
        Path subtaskDir = Paths.get(taskDir.getAbsolutePath(), "st-0");
        System.setProperty(ZIGGY_TEST_WORKING_DIR.property(), subtaskDir.toString());

        defaultPipelineInputs.populateSubTaskInputs();

        // all the data files should be in st-0
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(subtaskDir)) {
            List<String> subtaskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());
            assertTrue(subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
            assertTrue(
                subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
            assertTrue(subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
            assertTrue(
                subtaskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));
            assertTrue(subtaskDirFileNames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertTrue(subtaskDirFileNames.contains("calibration-4.12.9.h5"));
        }
    }

    /**
     * Tests the deleteTempInputsFromTaskDirectory() method.
     */
    @Test
    public void testDeleteTempInputsFromTaskDirectory() throws IOException {

        performCopyToTaskDir(false);
        defaultPipelineInputs.deleteTempInputsFromTaskDirectory(pipelineTask, taskDir.toPath());

        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(taskDir.toPath())) {
            List<String> taskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());

            assertTrue(taskDirFileNames.contains("st-0"));
            assertTrue(taskDirFileNames.contains("st-1"));
            assertFalse(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-flux.h5"));
            assertFalse(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-centroid.h5"));
            assertFalse(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-flux.h5"));
            assertFalse(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-centroid.h5"));
            assertFalse(taskDirFileNames.contains("tess2020234101112-12345_023-geometry.xml"));
            assertFalse(taskDirFileNames.contains("calibration-4.12.9.h5"));
        }
    }

    /**
     * Executes the copy of files to the task directory. Extracted to a separate method as all of
     * the tests depend on it.
     *
     * @param singleSubtask indicates whether a single subtask per task is desired.
     */
    private void performCopyToTaskDir(boolean singleSubtask) {

        TypedParameter singleSubtaskProp = uow
            .getParameter(DatastoreDirectoryUnitOfWorkGenerator.SINGLE_SUBTASK_PROPERTY_NAME);
        singleSubtaskProp.setValue(Boolean.valueOf(singleSubtask));

        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(uow);

        // Create a TaskConfigurationManager
        TaskConfigurationManager tcm = new TaskConfigurationManager(taskDir);

        defaultPipelineInputs.copyDatastoreFilesToTaskDirectory(tcm, pipelineTask,
            taskDir.toPath());
        tcm.persist();
    }

    private void initializeDataFileTypes() {

        fluxDataFileType = new DataFileType();
        fluxDataFileType.setName("flux");
        fluxDataFileType.setFileNameRegexForTaskDir(
            "sector-([0-9]{4})-ccd-([1234]:[1234])-tic-([0-9]{9})-flux.h5");
        fluxDataFileType.setFileNameWithSubstitutionsForDatastore("sector-$1/ccd-$2/pa/$3.flux.h5");

        centroidDataFileType = new DataFileType();
        centroidDataFileType.setName("centroid");
        centroidDataFileType.setFileNameRegexForTaskDir(
            "sector-([0-9]{4})-ccd-([1234]:[1234])-tic-([0-9]{9})-centroid.h5");
        centroidDataFileType
            .setFileNameWithSubstitutionsForDatastore("sector-$1/ccd-$2/pa/$3.centroid.h5");

        resultsDataFileType = new DataFileType();
        resultsDataFileType.setName("results");
        resultsDataFileType.setFileNameRegexForTaskDir(
            "sector-([0-9]{4})-ccd-([1234]:[1234])-tic-([0-9]{9})-results.h5");
        resultsDataFileType
            .setFileNameWithSubstitutionsForDatastore("sector-$1/ccd-$2/results/$3.results.h5");

        // Set up the model type 1 to have a model ID in its name, which is a simple integer,
        // and a timestamp in its name
        modelType1 = new ModelType();
        modelType1.setFileNameRegex("tess([0-9]{13})-([0-9]{5})_([0-9]{3})-geometry.xml");
        modelType1.setType("geometry");
        modelType1.setVersionNumberGroup(3);
        modelType1.setTimestampGroup(1);
        modelType1.setSemanticVersionNumber(false);

        // Set up the model type 2 to have a semantic model ID in its name but no timestamp
        modelType2 = new ModelType();
        modelType2.setFileNameRegex("calibration-([0-9]+\\.[0-9]+\\.[0-9]+).h5");
        modelType2.setTimestampGroup(-1);
        modelType2.setType("calibration");
        modelType2.setVersionNumberGroup(1);
        modelType2.setSemanticVersionNumber(true);

        // Set up the model type 3 to have neither ID nor timestamp
        modelType3 = new ModelType();
        modelType3.setFileNameRegex("simple-text.h5");
        modelType3.setType("ravenswood");
        modelType3.setTimestampGroup(-1);
        modelType3.setVersionNumberGroup(-1);
    }

    private void initializeModelRegistry() throws IOException {

        // First construct the registry itself
        modelRegistry = new ModelRegistry();

        // Construct the model metadata objects
        ModelMetadata m1 = new ModelMetadata(modelType1, "tess2020234101112-12345_023-geometry.xml",
            "DefaultModuleParametersTest", null);
        ModelMetadata m2 = new ModelMetadata(modelType2, "calibration-4.12.9.h5",
            "DefaultModuleParametersTest", null);
        ModelMetadata m3 = new ModelMetadata(modelType3, "simple-text.h5",
            "DefaultModuleParametersTest", null);

        // add the metadata objects to the registry
        Map<ModelType, ModelMetadata> metadataMap = modelRegistry.getModels();
        metadataMap.put(modelType1, m1);
        metadataMap.put(modelType2, m2);
        metadataMap.put(modelType3, m3);

        // create the files for the metadata objects in the datastore
        File modelsDir = new File(datastore, ModelImporter.DATASTORE_MODELS_SUBDIR_NAME);
        File geometryDir = new File(modelsDir, "geometry");
        geometryDir.mkdirs();
        new File(geometryDir, m1.getDatastoreFileName()).createNewFile();
        File calibrationDir = new File(modelsDir, "calibration");
        calibrationDir.mkdirs();
        new File(calibrationDir, m2.getDatastoreFileName()).createNewFile();
        File ravenswoodDir = new File(modelsDir, "ravenswood");
        ravenswoodDir.mkdirs();
        new File(ravenswoodDir, m3.getDatastoreFileName()).createNewFile();

        // Make the pipeline task depend on types 1 and 2 but not 3
        modelTypes = new HashSet<>();
        modelTypes.add(modelType1);
        modelTypes.add(modelType2);
    }

    private void initializeDataFiles() throws IOException {

        // Create the sets of paths.
        centroidPaths = new HashSet<>();
        Path dataDirRelativePath = datastore.toPath().relativize(dataDir.toPath());
        centroidPaths.add(dataDirRelativePath.resolve("001234567.centroid.h5"));
        centroidPaths.add(dataDirRelativePath.resolve("765432100.centroid.h5"));

        fluxPaths = new HashSet<>();
        fluxPaths.add(dataDirRelativePath.resolve("001234567.flux.h5"));
        fluxPaths.add(dataDirRelativePath.resolve("765432100.flux.h5"));

        // Create the datastore files as zero-length regular files.
        for (Path path : centroidPaths) {
            Files.createFile(datastore.toPath().resolve(path));
        }

        // Create the datastore files as zero-length regular files.
        for (Path path : fluxPaths) {
            Files.createFile(datastore.toPath().resolve(path));
        }
    }

    private Map<ClassWrapper<ParametersInterface>, ParameterSet> parametersMap() {

        Map<ClassWrapper<ParametersInterface>, ParameterSet> parMap = new HashMap<>();

        ClassWrapper<ParametersInterface> c1 = new ClassWrapper<>(Params1.class);
        ParameterSet s1 = new ParameterSet("params1");
        s1.populateFromParametersInstance(new Params1());
        parMap.put(c1, s1);

        ClassWrapper<ParametersInterface> c2 = new ClassWrapper<>(Params2.class);
        ParameterSet s2 = new ParameterSet("params2");
        s2.populateFromParametersInstance(new Params2());
        parMap.put(c2, s2);

        return parMap;
    }

    public static class Params1 extends Parameters {
        private int dmy1 = 500;
        private double dmy2 = 2856.3;

        public int getDmy1() {
            return dmy1;
        }

        public void setDmy1(int dmy1) {
            this.dmy1 = dmy1;
        }

        public double getDmy2() {
            return dmy2;
        }

        public void setDmy2(double dmy2) {
            this.dmy2 = dmy2;
        }
    }

    public static class Params2 extends Parameters {
        private String dmy3 = "dummy string";
        private boolean[] dmy4 = { true, false };

        public String getDmy3() {
            return dmy3;
        }

        public void setDmy3(String dmy3) {
            this.dmy3 = dmy3;
        }

        public boolean[] getDmy4() {
            return dmy4;
        }

        public void setDmy4(boolean[] dmy4) {
            this.dmy4 = dmy4;
        }
    }
}
