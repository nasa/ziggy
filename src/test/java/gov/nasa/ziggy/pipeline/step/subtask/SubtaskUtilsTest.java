package gov.nasa.ziggy.pipeline.step.subtask;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.pipeline.step.AlgorithmStateFiles;
import gov.nasa.ziggy.pipeline.step.AlgorithmStateFiles.AlgorithmState;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;

public class SubtaskUtilsTest {

    @Rule
    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();

    @Test
    public void testClearStaleAlgorithmStates() throws IOException {

        // Create a task directory and three subdirectories.
        Path taskDir = ziggyDirectoryRule.directory().resolve("1-2-task-dir");

        // Subtask st-0 is complete.
        Path st0 = taskDir.resolve("st-0");
        Files.createDirectories(st0);
        new AlgorithmStateFiles(st0.toFile()).updateCurrentState(AlgorithmState.COMPLETE);
        Files.createFile(st0.resolve(TaskConfiguration.LOCK_FILE_NAME));

        // Subtask st-1 failed.
        Path st1 = taskDir.resolve("st-1");
        Files.createDirectories(st1);
        new AlgorithmStateFiles(st1.toFile()).updateCurrentState(AlgorithmState.FAILED);
        Files.createFile(st1.resolve(TaskConfiguration.LOCK_FILE_NAME));
        Files.createFile(st1.resolve("task-dir-error.h5"));

        // Subtask st-2 is "processing" (actually stuck, the algorithm failed before finishing).
        Path st2 = taskDir.resolve("st-2");
        Files.createDirectories(st2);
        new AlgorithmStateFiles(st2.toFile()).updateCurrentState(AlgorithmState.PROCESSING);
        Files.createFile(st2.resolve(TaskConfiguration.LOCK_FILE_NAME));

        SubtaskUtils.clearStaleAlgorithmStates(taskDir.toFile());

        // The st-0 directory has no lock file but has the .COMPLETE file.
        assertTrue(Files.exists(st0.resolve(".COMPLETE")));
        assertFalse(Files.exists(st0.resolve(".FAILED")));
        assertFalse(Files.exists(st0.resolve(".PROCESSING")));
        assertFalse(Files.exists(st0.resolve(TaskConfiguration.LOCK_FILE_NAME)));
        assertFalse(Files.exists(st0.resolve("task-dir-error.h5")));

        // The st-1 and st-2 directories have no state file and no lock file.
        for (Path subtaskDir : List.of(st1, st2)) {
            assertFalse(Files.exists(subtaskDir.resolve(".COMPLETE")));
            assertFalse(Files.exists(subtaskDir.resolve(".FAILED")));
            assertFalse(Files.exists(subtaskDir.resolve(".PROCESSING")));
            assertFalse(Files.exists(subtaskDir.resolve(TaskConfiguration.LOCK_FILE_NAME)));
            assertFalse(Files.exists(st0.resolve("task-dir-error.h5")));
        }
    }
}
