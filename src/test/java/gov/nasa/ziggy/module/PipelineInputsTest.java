package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.DataFileInfo;
import gov.nasa.ziggy.data.management.DataFileTestUtils.DataFileInfoSample1;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineResultsSample1;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineResultsSample2;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Test class for PipelineInputs.
 *
 * @author PT
 */
public class PipelineInputsTest {

    private Path taskDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR_PROP_NAME, (String) null);

    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR_PROP_NAME, "/dev/null");

    @Before
    public void setup() throws IOException {

        taskDir = directoryRule.directory().resolve("1-2-pa");
        Path workingDir = taskDir.resolve("st-12");
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, workingDir.toString());
        // Create the task dir and the subtask dir
        Files.createDirectories(workingDir);

        PipelineResultsSample1 p = new PipelineResultsSample1();
        p.setOriginator(100L);
        p.setValue(-50);
        Hdf5ModuleInterface h = new Hdf5ModuleInterface();
        h.writeFile(taskDir.resolve("pa-001234567-20-results.h5").toFile(), p, true);
        p = new PipelineResultsSample1();
        p.setOriginator(100L);
        p.setValue(-30);
        h.writeFile(taskDir.resolve("pa-765432100-20-results.h5").toFile(), p, true);
        PipelineResultsSample2 p2 = new PipelineResultsSample2();
        p2.setOriginator(99L);
        p2.setFvalue(92.7F);
        h.writeFile(taskDir.resolve("cal-1-1-B-20-results.h5").toFile(), p, true);

        // add a task configuration file
        TaskConfigurationManager taskConfigurationManager = new TaskConfigurationManager(
            taskDir.toFile());
        Set<String> wrongInstance = new TreeSet<>();
        wrongInstance.add("wrong");
        Set<String> rightInstance = new TreeSet<>();
        rightInstance.add("right");
        for (int i = 0; i < 12; i++) {
            taskConfigurationManager.addFilesForSubtask(wrongInstance);
        }
        taskConfigurationManager.addFilesForSubtask(rightInstance);
        for (int i = 0; i < 12; i++) {
            taskConfigurationManager.addFilesForSubtask(wrongInstance);
        }
        taskConfigurationManager.persist();
    }

    /**
     * Tests the resultsFiles() method and the requiredDatastoreClasses() method.
     */
    @Test
    public void testResultsFiles() {

        // Start by checking that the required classes are as expected
        PipelineInputsSample pipelineInputs = new PipelineInputsSample();
        Set<Class<? extends DataFileInfo>> datastoreClasses = pipelineInputs
            .requiredDataFileInfoClasses();
        assertEquals(1, datastoreClasses.size());
        assertTrue(datastoreClasses.contains(DataFileInfoSample1.class));

        // Get the map and check its contents
        Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> sourcesMap = pipelineInputs
            .resultsFiles();
        Set<Class<? extends DataFileInfo>> keys = sourcesMap.keySet();
        assertEquals(1, sourcesMap.size());
        assertTrue(keys.contains(DataFileInfoSample1.class));

        Set<? extends DataFileInfo> datastoreIds = sourcesMap.get(DataFileInfoSample1.class);
        assertEquals(2, datastoreIds.size());
        Set<String> filenames = dataFileInfosToNames(datastoreIds);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));

    }

    /**
     * Tests the write() method. Also exercises the go() method.
     */
    @Test
    public void testWrite() {

        PipelineInputsSample pipelineInputs = new PipelineInputsSample();
        pipelineInputs.populateSubTaskInputs();
        pipelineInputs.writeSubTaskInputs(0);
        Hdf5ModuleInterface h = new Hdf5ModuleInterface();
        PipelineInputsSample inputsFromFile = new PipelineInputsSample();
        Path subTaskDir = taskDir.resolve("st-12");
        h.readFile(subTaskDir.resolve("pa-inputs-0.h5").toFile(), inputsFromFile, true);
        assertEquals(105.3, inputsFromFile.getDvalue(), 1e-9);
    }

    /**
     * Tests the subTaskIndex() method.
     */
    @Test
    public void testSubTaskIndex() {
        PipelineInputsSample pipelineInputs = new PipelineInputsSample();
        int st = pipelineInputs.subtaskIndex();
        assertEquals(12, st);
    }

    /**
     * Tests the filesForSubtask method.
     */
    @Test
    public void testFilesForSubtask() {
        PipelineInputsSample pipelineInputs = new PipelineInputsSample();
        List<String> u = new ArrayList<>(pipelineInputs.filesForSubtask());
        assertEquals(1, u.size());
        assertEquals("right", u.get(0));
    }

    /**
     * Tests the readResults() method.
     */
    @Test
    public void testReadResults() {
        PipelineInputsSample pipelineInputs = new PipelineInputsSample();
        Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> sourcesMap = pipelineInputs
            .resultsFiles();
        Set<? extends DataFileInfo> datastoreIds = sourcesMap.get(DataFileInfoSample1.class);
        for (DataFileInfo datastoreId : datastoreIds) {
            PipelineResultsSample1 r = new PipelineResultsSample1();
            pipelineInputs.readResultsFile(datastoreId, r);
            assertEquals(100L, r.getOriginator());
        }
    }

    /**
     * Tests the readFromTaskDir() and writeToTaskDir() methods.
     */
    @Test
    public void testReadWriteToTaskDir() {
        PipelineInputsSample pipelineInputs = new PipelineInputsSample();
        pipelineInputs.populateSubTaskInputs();
        File taskDirFile = taskDir.toFile();
        String taskDirRoot = taskDirFile.getParent();
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, taskDirRoot);
        PipelineTask pipelineTask = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask.getModuleName()).thenReturn("pa");
        pipelineInputs.writeToTaskDir(pipelineTask, taskDirFile);
        File writtenInputsFile = new File(taskDirFile, "pa-inputs.h5");
        assertTrue(writtenInputsFile.exists());

        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME,
            new File(taskDirFile, "st-12").getAbsolutePath());
        pipelineInputs = new PipelineInputsSample();
        assertEquals(0.0, pipelineInputs.getDvalue(), 1e-9);
        pipelineInputs.readFromTaskDir();
        assertEquals(105.3, pipelineInputs.getDvalue(), 1e-9);

    }

    private Set<String> dataFileInfosToNames(Set<? extends DataFileInfo> dataFileInfos) {
        Set<String> names = new HashSet<>();
        for (DataFileInfo d : dataFileInfos) {
            names.add(d.getName().toString());
        }
        return names;
    }
}
