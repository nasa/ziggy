package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineOutputsSample1;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineResultsSample1;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Test class for PipelineOutputs.
 *
 * @author PT
 */
public class PipelineOutputsTest {

    private String taskDir;
    private String filename = ModuleInterfaceUtils.outputsFileName("pa", 0);
    private String otherFilename = ModuleInterfaceUtils.outputsFileName("pa", 2);

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR_PROP_NAME, "/dev/null");

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR_PROP_NAME, (String) null);

    @Before
	public void setup() throws IOException {

        taskDir = directoryRule.directory().resolve("100-200-pa").toString();
        String workingDir = directoryRule.directory()
            .resolve("100-200-pa")
            .resolve("st-12")
            .toString();
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, workingDir);
        // Create the task dir and the subtask dir
        new File(workingDir).mkdirs();

        // create the outputs object and save to a file
        PipelineOutputsSample1 p = new PipelineOutputsSample1();
        p.populateTaskResults();
        Hdf5ModuleInterface h = new Hdf5ModuleInterface();
        h.writeFile(DirectoryProperties.workingDir().resolve(filename).toFile(), p, true);
        h.writeFile(DirectoryProperties.workingDir().resolve(otherFilename).toFile(), p, true);
    }

    /**
     * Tests the read() method.
     */
    @Test
    public void testRead() {
        PipelineOutputsSample1 p = new PipelineOutputsSample1();
        int[] ivalues = p.getIvalues();
        assertNull(ivalues);
        p.readSubTaskOutputs(DirectoryProperties.workingDir().resolve(filename).toFile());
        ivalues = p.getIvalues();
        assertEquals(3, ivalues.length);
        assertEquals(27, ivalues[0]);
        assertEquals(-9, ivalues[1]);
        assertEquals(5, ivalues[2]);
    }

    /**
     * Tests the originator() method.
     */
    @Test
    public void testOriginator() {
        PipelineOutputsSample1 p = new PipelineOutputsSample1();
        long originator = p.originator();
        assertEquals(200L, originator);
    }

    /**
     * Tests the saveResults() method.
     */
    @Test
    public void testSaveResults() {
        PipelineOutputsSample1 p = new PipelineOutputsSample1();
        p.readSubTaskOutputs(DirectoryProperties.workingDir().resolve(filename).toFile());
        p.saveResultsToTaskDir();
        int[] ivalues = p.getIvalues();

        // The results should be saved to 3 files in the task directory, with
        // names given by "pa-001234567-s", the index number, and ".h5". Each
        // result should be of class PipelineResultsSample1, and should contain
        // the i'th value from the ivalues array of the PipelineOutputsExample1
        // instance.
        Hdf5ModuleInterface h = new Hdf5ModuleInterface();
        for (int i = 0; i < 3; i++) {
            String fname = "pa-001234567-" + i + "-results.h5";
            PipelineResultsSample1 pr = new PipelineResultsSample1();
            h.readFile(new File(taskDir, fname), pr, true);
            assertEquals(200L, pr.getOriginator());
            assertEquals(ivalues[i], pr.getValue());
        }

    }

    /**
     * Tests the outputFiles() method.
     */
    @Test
    public void testOutputFiles() {
        PipelineOutputsSample1 p = new PipelineOutputsSample1();
        File[] files = p.outputFiles();
        assertEquals(2, files.length);
        Set<String> filenames = new HashSet<>();
        for (File f : files) {
            filenames.add(f.getName());
        }
        assertTrue(filenames.contains(filename));
        assertTrue(filenames.contains(otherFilename));
    }
}
