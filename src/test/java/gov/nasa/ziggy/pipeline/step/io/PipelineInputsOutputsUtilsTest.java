package gov.nasa.ziggy.pipeline.step.io;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_TEST_WORKING_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;
import gov.nasa.ziggy.pipeline.step.hdf5.Hdf5AlgorithmInterface;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskInformation;

/**
 * Test class for PipelineInputsOutputsUtils methods.
 *
 * @author PT
 */
public class PipelineInputsOutputsUtilsTest {

    private PipelineTask pipelineTask;
    private Path taskDirectory;
    private Path workingDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR, (String) null);

    @Before
    public void setup() throws IOException {

        pipelineTask = mock(PipelineTask.class);
        taskDirectory = directoryRule.directory().resolve("1-2-pa");
        workingDir = taskDirectory.resolve("st-12");
        ziggyTestWorkingDirPropertyRule.setValue(workingDir.toString());

        // Create the task dir and the subtask dir
        Files.createDirectories(workingDir);
    }

    @Test
    public void testTaskDir() {
        Path taskDir = PipelineInputsOutputsUtils.taskDir();
        assertEquals(taskDirectory, taskDir);
    }

    @Test
    public void testPipelineStepName() {
        assertEquals("pa", PipelineInputsOutputsUtils.pipelineStepName());
    }

    @Test
    public void testWritePipelineInputs() {
        PipelineInputsOutputsUtils.writePipelineInputsToDirectory(
            new TestPipelineInputs(pipelineTask, taskDirectory),
            PipelineInputsOutputsUtils.pipelineStepName(), workingDir);
        assertTrue(Files.isRegularFile(workingDir.resolve("pa-inputs.h5")));
        TestPipelineInputs inputsForTest = new TestPipelineInputs(pipelineTask, taskDirectory);
        inputsForTest.setIntValue(0);
        inputsForTest.setFloatValue(0);
        new Hdf5AlgorithmInterface().readFile(workingDir.resolve("pa-inputs.h5").toFile(),
            inputsForTest, false);
        assertEquals(7, inputsForTest.getIntValue());
        assertEquals(12.5F, inputsForTest.getFloatValue(), 1e-6);
    }

    @Test
    public void testReadPipelineInputs() {
        new Hdf5AlgorithmInterface().writeFile(workingDir.resolve("pa-inputs.h5").toFile(),
            new TestPipelineInputs(pipelineTask, taskDirectory), false);
        TestPipelineInputs inputsForTest = new TestPipelineInputs(pipelineTask, taskDirectory);
        inputsForTest.setIntValue(0);
        inputsForTest.setFloatValue(0);
        PipelineInputsOutputsUtils.readPipelineInputsFromDirectory(inputsForTest, "pa", workingDir);
        assertEquals(7, inputsForTest.getIntValue());
        assertEquals(12.5F, inputsForTest.getFloatValue(), 1e-6);
    }

    @Test
    public void testWritePipelineOutputs() {
        PipelineInputsOutputsUtils.writePipelineOutputsToDirectory(
            new TestPipelineOutputs(pipelineTask, taskDirectory),
            PipelineInputsOutputsUtils.pipelineStepName(), workingDir);
        assertTrue(Files.isRegularFile(workingDir.resolve("pa-outputs.h5")));
        TestPipelineOutputs outputsForTest = new TestPipelineOutputs(pipelineTask, taskDirectory);
        outputsForTest.setIntValue(0);
        outputsForTest.setFloatValue(0);
        new Hdf5AlgorithmInterface().readFile(workingDir.resolve("pa-outputs.h5").toFile(),
            outputsForTest, false);
        assertEquals(7, outputsForTest.getIntValue());
        assertEquals(12.5F, outputsForTest.getFloatValue(), 1e-6);
    }

    @Test
    public void testReadPipelineOutputs() {
        new Hdf5AlgorithmInterface().writeFile(workingDir.resolve("pa-outputs.h5").toFile(),
            new TestPipelineOutputs(pipelineTask, taskDirectory), false);
        TestPipelineOutputs outputsForTest = new TestPipelineOutputs(pipelineTask, taskDirectory);
        outputsForTest.setIntValue(0);
        outputsForTest.setFloatValue(0);
        PipelineInputsOutputsUtils.readPipelineOutputsFromDirectory(outputsForTest, "pa",
            workingDir);
        assertEquals(7, outputsForTest.getIntValue());
        assertEquals(12.5F, outputsForTest.getFloatValue(), 1e-6);
    }

    @Test
    public void testIdsAndPipelineStepNameFromTaskDir() {
        assertEquals(1L, PipelineInputsOutputsUtils.instanceId(taskDirectory));
        assertEquals(2L, PipelineInputsOutputsUtils.taskId(taskDirectory));
        assertEquals("pa", PipelineInputsOutputsUtils.pipelineStepName(taskDirectory));
    }

    private static class TestPipelineInputs extends PipelineInputs {

        private int intValue = 7;
        private float floatValue = 12.5F;

        public TestPipelineInputs(PipelineTask pipelineTask, Path taskDirectory) {
            super(pipelineTask, taskDirectory);
        }

        @Override
        public void copyDatastoreFilesToTaskDirectory(TaskConfiguration taskConfiguration,
            Path taskDirectory) {
        }

        @Override
        public SubtaskInformation subtaskInformation(PipelineNode pipelineNode) {
            return null;
        }

        @Override
        public void beforeAlgorithmExecution() {
        }

        @Override
        public void writeParameterSetsToTaskDirectory() {
        }

        public void setIntValue(int intValue) {
            this.intValue = intValue;
        }

        public int getIntValue() {
            return intValue;
        }

        public void setFloatValue(float floatValue) {
            this.floatValue = floatValue;
        }

        public float getFloatValue() {
            return floatValue;
        }
    }

    private static class TestPipelineOutputs extends PipelineOutputs {

        private int intValue = 7;
        private float floatValue = 12.5F;

        public TestPipelineOutputs(PipelineTask pipelineTask, Path taskDirectory) {
            super(pipelineTask, taskDirectory);
        }

        @Override
        public Set<Path> copyTaskFilesToDatastore() {
            return null;
        }

        @Override
        public boolean subtaskProducedOutputs() {
            return false;
        }

        @Override
        public void afterAlgorithmExecution() {
        }

        public void setIntValue(int intValue) {
            this.intValue = intValue;
        }

        public int getIntValue() {
            return intValue;
        }

        public void setFloatValue(float floatValue) {
            this.floatValue = floatValue;
        }

        public float getFloatValue() {
            return floatValue;
        }
    }
}
