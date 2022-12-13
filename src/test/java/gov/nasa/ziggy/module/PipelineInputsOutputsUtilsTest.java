package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.util.io.Filenames;

/**
 * Test class for PipelineInputsOutputsUtils methods.
 *
 * @author PT
 */
public class PipelineInputsOutputsUtilsTest {

    private static final int subTaskIndex = 12;
    private static final String subTaskDirName = "st-" + subTaskIndex;
    private String taskDir = new File(new File(Filenames.BUILD_TEST), "1-2-pa").getAbsolutePath();

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR_PROP_NAME, new File(taskDir, subTaskDirName).getAbsolutePath());

    @Before
    public void setup() {

        // Create the task dir and the subtask dir
        new File(ziggyTestWorkingDirPropertyRule.getProperty()).mkdirs();
    }

    /**
     * Tests the taskDir() method.
     */
    @Test
    public void testTaskDir() {
        Path taskDir = PipelineInputsOutputsUtils.taskDir();
        assertEquals(this.taskDir, taskDir.toAbsolutePath().toString());
    }

    /**
     * Tests the moduleExeName() method.
     */
    @Test
    public void testModuleName() {
        assertEquals("pa", PipelineInputsOutputsUtils.moduleName());
    }

}
