package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import gov.nasa.ziggy.module.SubtaskDirectoryIterator.GroupSubtaskDirectory;

/**
 * @author PT
 */
public class SubtaskDirectoryIteratorTest {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SubtaskDirectoryIteratorTest.class);

    @Test
    public void test() throws IOException {
        File taskDir = Files.createTempDir();
        File subtaskDir = new File(taskDir, "st-0");
        subtaskDir.mkdirs();
        subtaskDir = new File(taskDir, "st-3");
        subtaskDir.mkdirs();
        subtaskDir = new File(taskDir, "st-200");
        subtaskDir.mkdirs();
        SubtaskDirectoryIterator it = new SubtaskDirectoryIterator(taskDir);

        assertEquals(it.numSubtasks(), 3);
        assertEquals(-1, it.getCurrentIndex());
        assertTrue(it.hasNext());
        GroupSubtaskDirectory p = it.next();
        assertEquals(0, it.getCurrentIndex());
        assertEquals(taskDir.getAbsolutePath(), p.getGroupDir().getAbsolutePath());
        assertEquals(taskDir.getAbsolutePath(), p.getSubtaskDir().getParent());
        assertEquals("st-0", p.getSubtaskDir().getName());

        assertTrue(it.hasNext());
        p = it.next();
        assertEquals(1, it.getCurrentIndex());
        assertEquals(taskDir.getAbsolutePath(), p.getGroupDir().getAbsolutePath());
        assertEquals(taskDir.getAbsolutePath(), p.getSubtaskDir().getParent());
        assertEquals("st-3", p.getSubtaskDir().getName());

        assertTrue(it.hasNext());
        p = it.next();
        assertEquals(2, it.getCurrentIndex());
        assertEquals(taskDir.getAbsolutePath(), p.getGroupDir().getAbsolutePath());
        assertEquals(taskDir.getAbsolutePath(), p.getSubtaskDir().getParent());
        assertEquals("st-200", p.getSubtaskDir().getName());

        assertFalse(it.hasNext());

        FileUtils.deleteDirectory(taskDir);
    }
}
