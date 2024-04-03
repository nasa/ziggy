package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;

/**
 * Unit tests for {@link TestMonitor} class.
 *
 * @author PT
 */
public class TaskMonitorTest {

    private Path taskDir;
    private Path stateFileDir;
    private StateFile stateFile;
    private TaskMonitor taskMonitor;
    private List<AlgorithmStateFiles> algorithmStateFiles = new ArrayList<>();

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule pipelineResultsRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        directoryRule, "pipeline-results");

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule).around(pipelineResultsRule);

    @Before
    public void setUp() throws IOException, ConfigurationException {

        taskDir = DirectoryProperties.taskDataDir().resolve("10-20-modulename");
        stateFileDir = DirectoryProperties.stateFilesDir();
        Files.createDirectories(taskDir);
        Files.createDirectories(stateFileDir);
        Files.createFile(taskDir.resolve(StateFile.LOCK_FILE_NAME));

        // Create 6 subtask directories
        List<File> subtaskDirectories = new ArrayList<>();
        for (int subtask = 0; subtask < 6; subtask++) {
            Path subtaskDir = taskDir.resolve("st-" + Integer.toString(subtask));
            Files.createDirectories(subtaskDir);
            subtaskDirectories.add(subtaskDir.toFile());
            algorithmStateFiles.add(new AlgorithmStateFiles(subtaskDir.toFile()));
        }

        stateFile = StateFile.of(taskDir);
        stateFile.setActiveCoresPerNode(1);
        stateFile.setNumTotal(subtaskDirectories.size());
        stateFile.persist();

        taskMonitor = new TaskMonitor(stateFile, taskDir.toFile());
    }

    @Test
    public void testUpdateState() throws IOException {
        stateFile = taskMonitor.getStateFile();
        taskMonitor.updateState();
        assertEquals(StateFile.State.INITIALIZED, stateFile.getState());
        assertEquals(6, stateFile.getNumTotal());
        assertEquals(0, stateFile.getNumComplete());
        assertEquals(0, stateFile.getNumFailed());

        // Setting a subtask to PROCESSING should not affect the counts.
        algorithmStateFiles.get(0).updateCurrentState(AlgorithmStateFiles.SubtaskState.PROCESSING);
        taskMonitor.updateState();
        assertEquals(StateFile.State.INITIALIZED, stateFile.getState());
        assertEquals(6, stateFile.getNumTotal());
        assertEquals(0, stateFile.getNumComplete());
        assertEquals(0, stateFile.getNumFailed());

        // Setting a subtask to COMPLETED will affect the results.
        algorithmStateFiles.get(1).updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        taskMonitor.updateState();
        assertEquals(StateFile.State.INITIALIZED, stateFile.getState());
        assertEquals(6, stateFile.getNumTotal());
        assertEquals(1, stateFile.getNumComplete());
        assertEquals(0, stateFile.getNumFailed());

        // Setting a subtask to FAILED will affect the results.
        algorithmStateFiles.get(2).updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
        taskMonitor.updateState();
        assertEquals(StateFile.State.INITIALIZED, stateFile.getState());
        assertEquals(6, stateFile.getNumTotal());
        assertEquals(1, stateFile.getNumComplete());
        assertEquals(1, stateFile.getNumFailed());

        // Changing the state of the on-disk state file should get reflected in the state file
        // stored in the monitor.
        new StateFile(stateFile).setStateAndPersist(StateFile.State.PROCESSING);
        taskMonitor.updateState();
        assertEquals(StateFile.State.PROCESSING, stateFile.getState());
        assertEquals(6, stateFile.getNumTotal());
        assertEquals(1, stateFile.getNumComplete());
        assertEquals(1, stateFile.getNumFailed());
    }

    /**
     * Tests whether marking a state file as complete when it has subtasks that never got run moves
     * those subtasks to being counted in the state file as failed.
     *
     * @throws IOException
     */
    @Test
    public void testMarkStateFileCompleteWithSkippedTasks() throws IOException {
        taskMonitor.markStateFileDone();
        assertEquals(StateFile.State.COMPLETE, stateFile.getState());
        assertEquals(6, stateFile.getNumTotal());
        assertEquals(0, stateFile.getNumComplete());
        assertEquals(6, stateFile.getNumFailed());
        StateFile diskStateFile = stateFile.newStateFileFromDiskFile();
        assertEquals(StateFile.State.COMPLETE, diskStateFile.getState());
        assertEquals(6, diskStateFile.getNumTotal());
        assertEquals(0, diskStateFile.getNumComplete());
        assertEquals(6, diskStateFile.getNumFailed());
    }

    @Test
    public void markStateFileComplete() throws IOException {
        for (AlgorithmStateFiles algorithmStateFile : algorithmStateFiles) {
            algorithmStateFile.updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        }
        taskMonitor.updateState();
        taskMonitor.markStateFileDone();
        assertEquals(StateFile.State.COMPLETE, stateFile.getState());
        assertEquals(6, stateFile.getNumTotal());
        assertEquals(6, stateFile.getNumComplete());
        assertEquals(0, stateFile.getNumFailed());
        StateFile diskStateFile = stateFile.newStateFileFromDiskFile();
        assertEquals(StateFile.State.COMPLETE, diskStateFile.getState());
        assertEquals(6, diskStateFile.getNumTotal());
        assertEquals(6, diskStateFile.getNumComplete());
        assertEquals(0, diskStateFile.getNumFailed());
    }

    @Test
    public void testAllSubtasksProcessed() throws IOException {
        assertFalse(taskMonitor.allSubtasksProcessed());
        for (AlgorithmStateFiles algorithmStateFile : algorithmStateFiles) {
            algorithmStateFile.updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        }
        taskMonitor.updateState();
        assertTrue(taskMonitor.allSubtasksProcessed());
        algorithmStateFiles.get(0).updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
        taskMonitor.updateState();
        assertTrue(taskMonitor.allSubtasksProcessed());
    }
}
