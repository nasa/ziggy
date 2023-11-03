package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_TEST_WORKING_DIR;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;

/**
 * Test class for PipelineInputsOutputsUtils methods.
 *
 * @author PT
 */
public class PipelineInputsOutputsUtilsTest {

    private Path taskDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR, (String) null);

    @Before
    public void setup() throws IOException {

        taskDir = directoryRule.directory().resolve("1-2-pa");
        Path workingDir = taskDir.resolve("st-12");
        System.setProperty(ZIGGY_TEST_WORKING_DIR.property(), workingDir.toString());
        // Create the task dir and the subtask dir
        Files.createDirectories(workingDir);
    }

    /**
     * Tests the taskDir() method.
     */
    @Test
    public void testTaskDir() {
        Path taskDir = PipelineInputsOutputsUtils.taskDir();
        assertEquals(this.taskDir, taskDir);
    }

    /**
     * Tests the moduleExeName() method.
     */
    @Test
    public void testModuleName() {
        assertEquals("pa", PipelineInputsOutputsUtils.moduleName());
    }
}
