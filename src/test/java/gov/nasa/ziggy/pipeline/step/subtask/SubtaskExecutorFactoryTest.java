package gov.nasa.ziggy.pipeline.step.subtask;

import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HOME_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.PropertyName;

/** Unit tests for {@link SubtaskExecutorFactory}. */
public class SubtaskExecutorFactoryTest {

    private File rootDir;
    private File taskDir;
    private File subtaskDir;
    private File buildDir;
    private File binDir;
    private SubtaskMaster subtaskMaster = Mockito.mock(SubtaskMaster.class);

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule pipelineHomeDirPropertyRule = new ZiggyPropertyRule(PIPELINE_HOME_DIR,
        directoryRule, "build");

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule, "results");

    public ZiggyPropertyRule ziggyDirPropertyRule = new ZiggyPropertyRule(
        PropertyName.ZIGGY_HOME_DIR, directoryRule, "build");

    @Rule
    public RuleChain chain = RuleChain.outerRule(directoryRule)
        .around(pipelineHomeDirPropertyRule)
        .around(resultsDirPropertyRule)
        .around(ziggyDirPropertyRule);

    @Before
    public void setup() throws IOException, ConfigurationException {

        rootDir = directoryRule.directory().toFile();
        taskDir = new File(rootDir, "10-20-pa");
        subtaskDir = new File(taskDir, "st-0");
        subtaskDir.mkdirs();
        buildDir = new File(pipelineHomeDirPropertyRule.getValue());
        binDir = new File(buildDir, "bin");
        binDir.mkdirs();
        File paFile = new File(binDir, "pa");
        paFile.createNewFile();

        new File(resultsDirPropertyRule.getValue()).mkdirs();
        Mockito.when(subtaskMaster.getBinaryName()).thenReturn("pa");
        Mockito.when(subtaskMaster.taskDir()).thenReturn(taskDir);
        Mockito.when(subtaskMaster.getSubtaskIndex()).thenReturn(0);
        Mockito.when(subtaskMaster.getTimeoutSecs()).thenReturn(1000000);
    }

    @Test
    public void testGenericNewInstance() {
        SubtaskExecutor executor = SubtaskExecutorFactory.uninitializedNewInstance(subtaskMaster);
        assertFalse(executor instanceof MatlabSubtaskExecutor);
        assertFalse(executor instanceof PythonSubtaskExecutor);
        assertFalse(executor instanceof JavaSubtaskExecutor);
    }

    @Test
    public void testPythonNewInstance() throws IOException {
        Mockito.when(subtaskMaster.getBinaryName()).thenReturn("pa/pa.py");
        SubtaskExecutor executor = SubtaskExecutorFactory.uninitializedNewInstance(subtaskMaster);
        assertFalse(executor instanceof MatlabSubtaskExecutor);
        assertTrue(executor instanceof PythonSubtaskExecutor);
        assertFalse(executor instanceof JavaSubtaskExecutor);
    }

    @Test
    public void testMatlabNewInstance() throws IOException {
        Files.createFile(binDir.toPath().resolve("pa-requiredMCRProducts.txt"));
        SubtaskExecutor executor = SubtaskExecutorFactory.uninitializedNewInstance(subtaskMaster);
        assertTrue(executor instanceof MatlabSubtaskExecutor);
        assertFalse(executor instanceof PythonSubtaskExecutor);
        assertFalse(executor instanceof JavaSubtaskExecutor);
    }

    @Test
    public void testNewJavaInstance() {
        Mockito.when(subtaskMaster.getBinaryName()).thenReturn(this.getClass().getCanonicalName());
        SubtaskExecutor executor = SubtaskExecutorFactory.uninitializedNewInstance(subtaskMaster);
        assertFalse(executor instanceof MatlabSubtaskExecutor);
        assertFalse(executor instanceof PythonSubtaskExecutor);
        assertTrue(executor instanceof JavaSubtaskExecutor);
    }
}
