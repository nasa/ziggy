package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.DataFileManager;
import gov.nasa.ziggy.data.management.DataFileType;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.PropertyNames;

public class DefaultPipelineOutputsTest {

    private DataFileType fluxDataFileType, centroidDataFileType;
    private DataFileType resultsDataFileType;
    private PipelineTask pipelineTask;
    private PipelineDefinitionNode pipelineDefinitionNode;
    private File datastore;
    private File taskWorkspace;
    private File taskDir;
    private DataFileManager mockedDataFileManager;
    private DefaultPipelineInputs defaultPipelineInputs;
    private DefaultPipelineOutputs defaultPipelineOutputs;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR_PROP_NAME, directoryRule, "datastore");

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR_PROP_NAME, (String) null);

    @Before
    public void setup() throws IOException {

        datastore = new File(datastoreRootDirPropertyRule.getProperty());
        // Set up a temporary directory for the datastore and one for the task-directory
        datastore.mkdirs();
        File dataDir = new File(datastore, "sector-0001/ccd-1:1/pa");
        dataDir.mkdirs();
        taskWorkspace = directoryRule.directory().resolve("taskspace").toFile();
        taskWorkspace.mkdirs();
        taskDir = new File(taskWorkspace, "10-20-csci");
        taskDir.mkdirs();

        // Set up the data file types
        initializeDataFileTypes();

        // Set up a dummied PipelineTask and a dummied PipelineDefinitionNode
        pipelineTask = Mockito.mock(PipelineTask.class);
        pipelineDefinitionNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(pipelineDefinitionNode);
        Mockito.when(pipelineDefinitionNode.getInputDataFileTypes())
            .thenReturn(Sets.newHashSet(fluxDataFileType, centroidDataFileType));
        Mockito.when(pipelineDefinitionNode.getOutputDataFileTypes())
            .thenReturn(Sets.newHashSet(resultsDataFileType));
        Mockito.when(pipelineTask.getModuleName()).thenReturn("csci");

        // We need a DataFileManager that's had its ResultsOriginatorCrud mocked out
        mockedDataFileManager = new DataFileManager(datastore.toPath(), taskDir.toPath(),
            pipelineTask);
        mockedDataFileManager = Mockito.spy(mockedDataFileManager);
        Mockito.when(mockedDataFileManager.datastoreProducerConsumerCrud())
            .thenReturn(Mockito.mock(DatastoreProducerConsumerCrud.class));

        // We can't use a Spy on the DefaultPipelineInputs instance because it has to get
        // serialized via HDF5, and the HDF5 module interface can't figure out how to do that
        // for a mocked object. Instead we resort to the tried-and-true approach of a
        // constructor that takes as argument the object we want to replace.
        defaultPipelineInputs = new DefaultPipelineInputs(mockedDataFileManager,
            Mockito.mock(AlertService.class));
        defaultPipelineInputs.setOutputDataFileTypes(Arrays.asList(resultsDataFileType));
        defaultPipelineInputs.writeToTaskDir(pipelineTask, taskDir);

        // the DefaultPipelineOutputs has the same DataFileManager issue as DefaultPipelineInputs
        defaultPipelineOutputs = new DefaultPipelineOutputs(mockedDataFileManager);

        // create the subtask directory and put a couple of results files therein
        File subtaskDir = new File(taskDir, "st-0");
        subtaskDir.mkdirs();
        new File(subtaskDir, "sector-0001-ccd-1:1-tic-001234567-results.h5").createNewFile();
        new File(subtaskDir, "sector-0001-ccd-1:1-tic-765432100-results.h5").createNewFile();

    }

    /**
     * Tests the populateTaskResults() method, which copies task results from the subtask directory
     * to the task directory.
     */
    @Test
    public void testPopulateTaskResults() throws IOException {

        // pull the directory listing and make sure no results files are present

        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(taskDir.toPath())) {
            List<String> taskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());
            assertFalse(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-results.h5"));
            assertFalse(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-results.h5"));
        }

        // Go to the subtask directory
        Path subtaskDir = taskDir.toPath().resolve("st-0");
        System.setProperty(PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir.toString());

        // Populate the task results
        defaultPipelineOutputs.populateTaskResults();

        // Pull the directory listing and check for results files
        try (Stream<Path> taskDirPaths = java.nio.file.Files.list(taskDir.toPath())) {
            List<String> taskDirFileNames = taskDirPaths.map(s -> s.getFileName().toString())
                .collect(Collectors.toList());
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-001234567-results.h5"));
            assertTrue(taskDirFileNames.contains("sector-0001-ccd-1:1-tic-765432100-results.h5"));
        }
    }

    /**
     * Tests the copyTaskDirectoryResultsToDatastore() method, which persists results from
     * processing.
     */
    @Test
    public void testCopyTaskDirectoryResultsToDatastore() throws IOException {

        // put the results files in the task directory
        new File(taskDir, "sector-0001-ccd-1:1-tic-001234567-results.h5").createNewFile();
        new File(taskDir, "sector-0001-ccd-1:1-tic-765432100-results.h5").createNewFile();

        // execute the copy
        defaultPipelineOutputs.copyTaskDirectoryResultsToDatastore(null, pipelineTask,
            taskDir.toPath());

        // check that the files are gone from the task directory
        assertFalse(new File(taskDir, "sector-0001-ccd-1:1-tic-001234567-results.h5").exists());
        assertFalse(new File(taskDir, "sector-0001-ccd-1:1-tic-765432100-results.h5").exists());

        // check that the files are present in the datastore
        String datastoreResultsPath = "sector-0001/ccd-1:1/results";
        assertTrue(java.nio.file.Files.exists(
            Paths.get(datastore.getAbsolutePath(), datastoreResultsPath, "001234567.results.h5")));
        assertTrue(java.nio.file.Files.exists(
            Paths.get(datastore.getAbsolutePath(), datastoreResultsPath, "765432100.results.h5")));

    }

    private void initializeDataFileTypes() {

        resultsDataFileType = new DataFileType();
        resultsDataFileType.setName("results");
        resultsDataFileType.setFileNameRegexForTaskDir(
            "sector-([0-9]{4})-ccd-([1234]:[1234])-tic-([0-9]{9})-results.h5");
        resultsDataFileType
            .setFileNameWithSubstitutionsForDatastore("sector-$1/ccd-$2/results/$3.results.h5");
    }

}
