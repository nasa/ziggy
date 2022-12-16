package gov.nasa.ziggy.worker;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyNames.RESULTS_DIR_PROP_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;

public class TaskFileCopyTest {

    private static final String SOURCE_DIR = TEST_DATA.resolve("TaskFileCopy")
        .resolve("src")
        .toString();

    private static final int PIPELINE_INSTANCE_ID = 17;
    private static final int PIPELINE_TASK_ID = 65;

    private static final String TASK_DIR_NAME = PIPELINE_INSTANCE_ID + "-" + PIPELINE_TASK_ID
        + "-debug";

    private File workingDir;
    private File archiveDir;
    private File binFile;
    private File metricsFile;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR_PROP_NAME,
        directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(resultsDirPropertyRule);

    public void setUp() throws Exception {

        archiveDir = directoryRule.directory().resolve("archive").toFile();
        File src = new File(SOURCE_DIR, TASK_DIR_NAME);
        workingDir = DirectoryProperties.taskDataDir().resolve(TASK_DIR_NAME).toFile();
        archiveDir = new File(archiveDir, TASK_DIR_NAME);
        binFile = new File(archiveDir.getAbsolutePath(), "st-0/debug-outputs-0.bin");
        metricsFile = new File(archiveDir.getAbsolutePath(), "st-0/metrics-0.ser");

        if (workingDir.exists()) {
            FileUtils.forceDelete(workingDir);
        }

        if (archiveDir.exists()) {
            FileUtils.forceDelete(archiveDir);
        }

        FileUtils.copyDirectory(src, workingDir);
    }

    @Test
    public void testWildcardsDelete() throws Exception {
        doTestCopy(new String[] { "*.bin", "*.ser" }, true);
    }

    @Test
    public void testWildcardsNoDelete() throws Exception {
        doTestCopy(new String[] { "*.bin", "*.ser" }, false);
    }

    @Test
    public void testNoWildcardsDelete() throws Exception {
        doTestCopy(new String[0], true);
    }

    @Test
    public void testNoWildcardsNoDelete() throws Exception {
        doTestCopy(new String[0], false);
    }

    public void doTestCopy(String[] excludeWildcards, boolean deleteAfterCopy) throws Exception {
        setUp();

        TaskFileCopyParameters copyParams = new TaskFileCopyParameters();
        copyParams.setEnabled(true);
        copyParams.setDestinationPath(archiveDir.getParentFile().getAbsolutePath());
        copyParams.setExcludeWildcards(excludeWildcards);
        copyParams.setDeleteAfterCopy(deleteAfterCopy);
        copyParams.setFailTaskOnError(true);

        PipelineTask task = new PipelineTask();
        task.setId(PIPELINE_TASK_ID);

        PipelineInstanceNode in = new PipelineInstanceNode();
        PipelineModuleDefinition pmd = new PipelineModuleDefinition("debug");
        in.setPipelineModuleDefinition(pmd);
        task.setPipelineInstanceNode(in);

        PipelineInstance instance = new PipelineInstance();
        instance.setId(PIPELINE_INSTANCE_ID);
        task.setPipelineInstance(instance);

        TaskFileCopy copier = new TaskFileCopy(task, copyParams);

        copier.copyTaskFiles();

        if (excludeWildcards.length > 0) {
            assertFalse("binFile exist = FALSE", binFile.exists());
            assertFalse("metricsFile exist = FALSE", metricsFile.exists());
        } else {
            assertTrue("binFile exist = TRUE", binFile.exists());
            assertTrue("metricsFile exist = TRUE", metricsFile.exists());
        }

        if (deleteAfterCopy) {
            assertFalse("workingDir exist = FALSE", workingDir.exists());
        } else {
            assertTrue("workingDir exist = TRUE", workingDir.exists());
        }

    }
}
