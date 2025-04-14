package gov.nasa.ziggy.pipeline.step;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;

/**
 * Unit test class for ProcessingFailureSummary class.
 *
 * @author PT
 */
public class ProcessingFailureSummaryTest {

    private File taskDir;
    private final String pipelineStepName = "pipelineStepName";

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() {

        taskDir = directoryRule.directory().toFile();
        File subtaskDir = new File(taskDir, "st-0");
        subtaskDir.mkdirs();
        subtaskDir = new File(taskDir, "st-3");
        subtaskDir.mkdirs();
        subtaskDir = new File(taskDir, "st-200");
        subtaskDir.mkdirs();
    }

    /**
     * Test case with no errors.
     */
    @Test
    public void testNoErrors() {

        ProcessingFailureSummary p = new ProcessingFailureSummary(pipelineStepName, taskDir);
        assertTrue(p.isAllTasksSucceeded());
        assertFalse(p.isAllTasksFailed());
        assertEquals(0, p.getFailedSubtaskDirs().size());
    }

    /**
     * Test case with some errors.
     *
     * @throws IOException
     */
    @Test
    public void testSomeErrors() throws IOException {
        new File(new File(taskDir, "st-3"), "pipelineStepName-error.h5").createNewFile();
        new File(new File(taskDir, "st-200"), "pipelineStepName-error.h5").createNewFile();
        ProcessingFailureSummary p = new ProcessingFailureSummary(pipelineStepName, taskDir);
        assertFalse(p.isAllTasksSucceeded());
        assertFalse(p.isAllTasksFailed());
        assertEquals(2, p.getFailedSubtaskDirs().size());
        assertEquals("st-3", p.getFailedSubtaskDirs().get(0));
        assertEquals("st-200", p.getFailedSubtaskDirs().get(1));
    }

    /**
     * Test case with all subtasks errored.
     *
     * @throws IOException
     */
    @Test
    public void testAllErrored() throws IOException {
        new File(new File(taskDir, "st-3"), "pipelineStepName-error.h5").createNewFile();
        new File(new File(taskDir, "st-200"), "pipelineStepName-error.h5").createNewFile();
        new File(new File(taskDir, "st-0"), "pipelineStepName-error.h5").createNewFile();
        ProcessingFailureSummary p = new ProcessingFailureSummary(pipelineStepName, taskDir);
        assertFalse(p.isAllTasksSucceeded());
        assertTrue(p.isAllTasksFailed());
        assertEquals(3, p.getFailedSubtaskDirs().size());
        assertEquals("st-0", p.getFailedSubtaskDirs().get(0));
        assertEquals("st-3", p.getFailedSubtaskDirs().get(1));
        assertEquals("st-200", p.getFailedSubtaskDirs().get(2));
    }
}
