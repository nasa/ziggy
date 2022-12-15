package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineOutputsSample1;

public class TaskConfigurationManagerTest {

    private File taskDir;
    private Set<String> t1, t2, t3, t4, t5, t6, single;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() {
        taskDir = directoryRule.directory().toFile();
        t1 = new TreeSet<>();
        t2 = new TreeSet<>();
        t3 = new TreeSet<>();
        t4 = new TreeSet<>();
        t5 = new TreeSet<>();
        t6 = new TreeSet<>();
        single = new TreeSet<>();
    }

    @After
    public void teardown() throws IOException {
    }

    /**
     * Tests the two constructor signatures and the getTaskDir() public method.
     */
    @Test
    public void testConstructors() {
        TaskConfigurationManager h = new TaskConfigurationManager();
        assertNull(h.getTaskDir());
        h = new TaskConfigurationManager(taskDir);
        assertEquals(taskDir, h.getTaskDir());
    }

    /**
     * Tests the addSubTaskInputs method. Also exercises the getCurrentSubTaskIndex(),
     * subTaskDirectory(), and numInputs() methods.
     */
    @Test
    public void testAddSubTaskInputs() {
        TaskConfigurationManager h = new TaskConfigurationManager(taskDir);
        h.addFilesForSubtask(t1);
        h.addFilesForSubtask(t2);
        h.addFilesForSubtask(t3);
        h.addFilesForSubtask(single);

        assertTrue(new File(taskDir, "st-0").exists());
        assertTrue(new File(taskDir, "st-1").exists());
        assertTrue(new File(taskDir, "st-2").exists());
        assertTrue(new File(taskDir, "st-3").exists());

        assertEquals(4, h.getSubtaskCount());
        assertEquals(4, h.numInputs());
    }

    /**
     * Tests the subTaskUnitOfWork method.
     */
    @Test
    public void testSubTaskUnitOfWork() {
        TaskConfigurationManager h = new TaskConfigurationManager(taskDir);
        h.addFilesForSubtask(t1);
        h.addFilesForSubtask(t2);
        h.addFilesForSubtask(t3);

        Set<String> u = h.filesForSubtask(2);
        assertEquals(t3, u);
    }

    /**
     * Tests the validate() method, including the case in which it sets the default processing to
     * cover all sub-tasks in parallel.
     */
    @Test
    public void testValidate() {
        TaskConfigurationManager h = new TaskConfigurationManager(taskDir);
        h.addFilesForSubtask(t1);
        h.addFilesForSubtask(t2);
        h.addFilesForSubtask(t3);
        h.addFilesForSubtask(t4);
        h.addFilesForSubtask(t5);
        h.addFilesForSubtask(t6);
        h.validate();
        assertEquals(6, h.getSubtaskCount());
    }

    /**
     * Exercises the subTaskDirectory() method.
     */
    @Test
    public void testSubTaskDirectory() {
        TaskConfigurationManager h = new TaskConfigurationManager(taskDir);
        File f = h.subtaskDirectory();
        assertEquals(new File(taskDir, "st-0").getAbsolutePath(), f.getAbsolutePath());
        h.addFilesForSubtask(t1);
        f = h.subtaskDirectory();
        assertEquals(new File(taskDir, "st-1").getAbsolutePath(), f.getAbsolutePath());
    }

    /**
     * Exercises the isEmpty() method.
     */
    @Test
    public void testIsEmpty() {
        TaskConfigurationManager h = new TaskConfigurationManager(taskDir);
        assertTrue(h.isEmpty());
        h.addFilesForSubtask(t1);
        assertFalse(h.isEmpty());
    }

    /**
     * Exercises the toString() method.
     */
    @Test
    public void testToString() {
        TaskConfigurationManager h = new TaskConfigurationManager(taskDir);
        h.addFilesForSubtask(t1);
        h.addFilesForSubtask(t2);
        h.addFilesForSubtask(t3);
        h.addFilesForSubtask(t4);
        h.addFilesForSubtask(t5);
        h.addFilesForSubtask(t6);
        String s = h.toString();
        assertEquals("SINGLE:[0,5]", s);
    }

    @Test
    public void testInputOutputClassHandling() {
        TaskConfigurationManager h1 = new TaskConfigurationManager(taskDir);
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
        TaskConfigurationManager h1 = new TaskConfigurationManager(taskDir);
        h1.addFilesForSubtask(t1);
        h1.addFilesForSubtask(t2);
        h1.addFilesForSubtask(t3);
        h1.addFilesForSubtask(t4);
        h1.addFilesForSubtask(t5);
        h1.addFilesForSubtask(t6);
        h1.setInputsClass(PipelineInputsSample.class);
        h1.setOutputsClass(PipelineOutputsSample1.class);
        assertFalse(TaskConfigurationManager.isPersistedInputsHandlerPresent(h1.getTaskDir()));
        h1.persist();
        assertTrue(TaskConfigurationManager.isPersistedInputsHandlerPresent(h1.getTaskDir()));
        TaskConfigurationManager h2 = TaskConfigurationManager.restore(h1.getTaskDir());
        assertEquals(h1, h2);
    }

}
