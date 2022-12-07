package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.util.io.Filenames;

/**
 * Test class for PipelineInputsOutputsUtils methods.
 *
 * @author PT
 */
public class PipelineInputsOutputsUtilsTest {

    private static final int subTaskIndex = 12;
    private static final String subTaskDirName = "st-" + subTaskIndex;
    private String taskDir;

    @Before
    public void setup() {

        // Create the task dir and the subtask dir
        File taskDirRoot = new File(Filenames.BUILD_TEST);
        File taskDir = new File(taskDirRoot, "1-2-pa");
        this.taskDir = taskDir.getAbsolutePath();
        File subTaskDir = new File(taskDir, subTaskDirName);
        subTaskDir.mkdirs();

        // change to the task dir and save a results file
        System.setProperty(PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME,
            subTaskDir.getAbsolutePath());
    }

    @After
    public void teardown() throws InterruptedException, IOException {

        System.clearProperty(PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME);
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
