package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_TEST_WORKING_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;

/**
 * Test class for PipelineInputsOutputsUtils methods.
 *
 * @author PT
 */
public class PipelineInputsOutputsUtilsTest {

    private Path taskDir;
    private Path workingDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR, (String) null);

    @Before
    public void setup() throws IOException {

        taskDir = directoryRule.directory().resolve("1-2-pa");
        workingDir = taskDir.resolve("st-12");
        ziggyTestWorkingDirPropertyRule.setValue(workingDir.toString());

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

    @Test
    public void testWritePipelineInputs() {
        PipelineInputsOutputsUtils.writePipelineInputsToDirectory(
            new PipelineInputsOutputsForTest(), PipelineInputsOutputsUtils.moduleName(),
            workingDir);
        assertTrue(Files.isRegularFile(workingDir.resolve("pa-inputs.h5")));
        PipelineInputsOutputsForTest inputsForTest = new PipelineInputsOutputsForTest();
        inputsForTest.setIntValue(0);
        inputsForTest.setFloatValue(0);
        new Hdf5ModuleInterface().readFile(workingDir.resolve("pa-inputs.h5").toFile(),
            inputsForTest, false);
        assertEquals(7, inputsForTest.getIntValue());
        assertEquals(12.5F, inputsForTest.getFloatValue(), 1e-6);
    }

    @Test
    public void testReadPipelineInputs() {
        new Hdf5ModuleInterface().writeFile(workingDir.resolve("pa-inputs.h5").toFile(),
            new PipelineInputsOutputsForTest(), false);
        PipelineInputsOutputsForTest inputsForTest = new PipelineInputsOutputsForTest();
        inputsForTest.setIntValue(0);
        inputsForTest.setFloatValue(0);
        PipelineInputsOutputsUtils.readPipelineInputsFromDirectory(inputsForTest, "pa", workingDir);
        assertEquals(7, inputsForTest.getIntValue());
        assertEquals(12.5F, inputsForTest.getFloatValue(), 1e-6);
    }

    @Test
    public void testWritePipelineOutputs() {
        PipelineInputsOutputsUtils.writePipelineOutputsToDirectory(
            new PipelineInputsOutputsForTest(), PipelineInputsOutputsUtils.moduleName(),
            workingDir);
        assertTrue(Files.isRegularFile(workingDir.resolve("pa-outputs.h5")));
        PipelineInputsOutputsForTest outputsForTest = new PipelineInputsOutputsForTest();
        outputsForTest.setIntValue(0);
        outputsForTest.setFloatValue(0);
        new Hdf5ModuleInterface().readFile(workingDir.resolve("pa-outputs.h5").toFile(),
            outputsForTest, false);
        assertEquals(7, outputsForTest.getIntValue());
        assertEquals(12.5F, outputsForTest.getFloatValue(), 1e-6);
    }

    @Test
    public void testReadPipelineOutputs() {
        new Hdf5ModuleInterface().writeFile(workingDir.resolve("pa-outputs.h5").toFile(),
            new PipelineInputsOutputsForTest(), false);
        PipelineInputsOutputsForTest outputsForTest = new PipelineInputsOutputsForTest();
        outputsForTest.setIntValue(0);
        outputsForTest.setFloatValue(0);
        PipelineInputsOutputsUtils.readPipelineOutputsFromDirectory(outputsForTest, "pa",
            workingDir);
        assertEquals(7, outputsForTest.getIntValue());
        assertEquals(12.5F, outputsForTest.getFloatValue(), 1e-6);
    }

    @Test
    public void testIdsAndModuleNameFromTaskDir() {
        assertEquals(1L, PipelineInputsOutputsUtils.instanceId(taskDir));
        assertEquals(2L, PipelineInputsOutputsUtils.taskId(taskDir));
        assertEquals("pa", PipelineInputsOutputsUtils.moduleName(taskDir));
    }
}
