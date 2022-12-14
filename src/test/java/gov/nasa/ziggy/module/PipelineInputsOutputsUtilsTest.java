package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;

import java.io.File;
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

    private String taskDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR_PROP_NAME, (String) null);

    @Before
    public void setup() {

        taskDir = directoryRule.directory().resolve("1-2-pa").toString();
        String workingDir = directoryRule.directory().resolve("1-2-pa").resolve("st-12").toString();
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, workingDir);
        // Create the task dir and the subtask dir
        new File(workingDir).mkdirs();
    }

    /**
     * Tests the taskDir() method.
     */
    @Test
    public void testTaskDir() {
        Path taskDir = PipelineInputsOutputsUtils.taskDir();
        assertEquals(this.taskDir, taskDir.toString());
    }

    /**
     * Tests the moduleExeName() method.
     */
    @Test
    public void testModuleName() {
        assertEquals("pa", PipelineInputsOutputsUtils.moduleName());
    }

}
