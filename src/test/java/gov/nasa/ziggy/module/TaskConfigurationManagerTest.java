package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineOutputsSample1;

public class TaskConfigurationManagerTest {

    private File taskDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() {
        taskDir = directoryRule.directory().toFile();
    }

    /**
     * Tests the two constructor signatures and the getTaskDir() public method.
     */
    @Test
    public void testConstructors() {
        TaskConfiguration h = new TaskConfiguration();
        assertNull(h.getTaskDir());
        h = new TaskConfiguration(taskDir);
        assertEquals(taskDir, h.getTaskDir());
    }

    @Test
    public void testInputOutputClassHandling() {
        TaskConfiguration h1 = new TaskConfiguration(taskDir);
        h1.setInputsClass(PipelineInputsSample.class);
        h1.setOutputsClass(PipelineOutputsSample1.class);
        Class<?> ci = h1.getInputsClass();
        assertEquals(PipelineInputsSample.class.getCanonicalName(), ci.getCanonicalName());
        Class<?> co = h1.getOutputsClass();
        assertEquals(PipelineOutputsSample1.class.getCanonicalName(), co.getCanonicalName());
    }

    /**
     * Tests the persist() and restore() methods. Also exercises the
     * isPersistedInputsHandlerPresent() method and the persistedFile() method.
     */
    @Test
    public void testPersistRestore() {
        TaskConfiguration h1 = new TaskConfiguration(taskDir);
        h1.setInputsClass(PipelineInputsSample.class);
        h1.setOutputsClass(PipelineOutputsSample1.class);
        assertFalse(TaskConfiguration.isSerializedTaskConfigurationPresent(h1.getTaskDir()));
        h1.serialize();
        assertTrue(TaskConfiguration.isSerializedTaskConfigurationPresent(h1.getTaskDir()));
        TaskConfiguration h2 = TaskConfiguration.deserialize(h1.getTaskDir());
        assertEquals(h1, h2);
    }
}
