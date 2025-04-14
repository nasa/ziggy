package gov.nasa.ziggy.pipeline.step.subtask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;

public class PythonSubtaskExecutorTest {

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule homeDirPropertyRule = new ZiggyPropertyRule(
        PropertyName.PIPELINE_HOME_DIR, directoryRule, "build");
    public ZiggyPropertyRule ziggyDirPropertyRule = new ZiggyPropertyRule(
        PropertyName.ZIGGY_HOME_DIR, directoryRule, "build");

    @Rule
    public RuleChain ziggyRuleChain = RuleChain.outerRule(directoryRule)
        .around(homeDirPropertyRule)
        .around(ziggyDirPropertyRule);

    private SubtaskMaster subtaskMaster = Mockito.mock(SubtaskMaster.class);

    @Before
    public void setUp() throws IOException {
        Files.createDirectories(DirectoryProperties.pipelineHomeDir());
        Files.createDirectories(DirectoryProperties.pipelineBinDir());
        Files.createFile(
            DirectoryProperties.pipelineBinDir().resolve(PythonSubtaskExecutor.PYTHON_BINARY));
        Mockito.when(subtaskMaster.taskDir())
            .thenReturn(directoryRule.directory().resolve("1-2-pa-initializer").toFile());
        Mockito.when(subtaskMaster.getSubtaskIndex()).thenReturn(0);
        Mockito.when(subtaskMaster.getTimeoutSecs()).thenReturn(1_000_000);
        Mockito.when(subtaskMaster.getBinaryName()).thenReturn("pa/pa.py");
    }

    @Test
    public void testNoVirtualEnvironment() {
        SubtaskExecutor subtaskExecutor = SubtaskExecutorFactory.newInstance(subtaskMaster);
        Map<String, String> environment = subtaskExecutor.getEnvironment();
        assertFalse(
            environment.containsKey(PythonSubtaskExecutor.VIRTUAL_ENVIRONMENT_ENV_VAR_NAME));
        assertEquals("pa.pa", environment.get(PythonSubtaskExecutor.PYTHON_MODULE_ENV_VAR_NAME));
        assertEquals("pa-initializer",
            environment.get(PythonSubtaskExecutor.PYTHON_FUNCTION_ENV_VAR_NAME));
        assertEquals("ziggy-python", subtaskExecutor.binaryName());
    }

    @Test
    public void testEnvDirIsVirtualEnvironment() throws IOException {
        Files.createDirectories(DirectoryProperties.pythonEnvDir());
        SubtaskExecutor subtaskExecutor = SubtaskExecutorFactory.newInstance(subtaskMaster);
        Map<String, String> environment = subtaskExecutor.getEnvironment();
        assertEquals(DirectoryProperties.pythonEnvDir().toString(),
            environment.get(PythonSubtaskExecutor.VIRTUAL_ENVIRONMENT_ENV_VAR_NAME));
        assertEquals("pa.pa", environment.get(PythonSubtaskExecutor.PYTHON_MODULE_ENV_VAR_NAME));
        assertEquals("pa-initializer",
            environment.get(PythonSubtaskExecutor.PYTHON_FUNCTION_ENV_VAR_NAME));
        assertEquals("ziggy-python", subtaskExecutor.binaryName());
    }

    @Test
    public void defaultEnvDirIsVirtualEnvironment() throws IOException {
        Files.createDirectories(DirectoryProperties.pythonEnvDir()
            .resolve(PythonSubtaskExecutor.PYTHON_DEFAULT_VENV_NAME));
        Files.createDirectories(DirectoryProperties.pythonEnvDir().resolve("cal"));
        SubtaskExecutor subtaskExecutor = SubtaskExecutorFactory.newInstance(subtaskMaster);
        Map<String, String> environment = subtaskExecutor.getEnvironment();
        assertEquals(
            DirectoryProperties.pythonEnvDir()
                .resolve(PythonSubtaskExecutor.PYTHON_DEFAULT_VENV_NAME)
                .toString(),
            environment.get(PythonSubtaskExecutor.VIRTUAL_ENVIRONMENT_ENV_VAR_NAME));
        assertEquals("pa.pa", environment.get(PythonSubtaskExecutor.PYTHON_MODULE_ENV_VAR_NAME));
        assertEquals("pa-initializer",
            environment.get(PythonSubtaskExecutor.PYTHON_FUNCTION_ENV_VAR_NAME));
        assertEquals("ziggy-python", subtaskExecutor.binaryName());
    }

    @Test
    public void testPythonModuleNameIsVirtualEnvironment() throws IOException {
        Files.createDirectories(DirectoryProperties.pythonEnvDir()
            .resolve(PythonSubtaskExecutor.PYTHON_DEFAULT_VENV_NAME));
        Files.createDirectories(DirectoryProperties.pythonEnvDir().resolve("cal"));
        Files.createDirectories(DirectoryProperties.pythonEnvDir().resolve("pa"));

        SubtaskExecutor subtaskExecutor = SubtaskExecutorFactory.newInstance(subtaskMaster);
        Map<String, String> environment = subtaskExecutor.getEnvironment();
        assertEquals(DirectoryProperties.pythonEnvDir().resolve("pa").toString(),
            environment.get(PythonSubtaskExecutor.VIRTUAL_ENVIRONMENT_ENV_VAR_NAME));
        assertEquals("pa.pa", environment.get(PythonSubtaskExecutor.PYTHON_MODULE_ENV_VAR_NAME));
        assertEquals("pa-initializer",
            environment.get(PythonSubtaskExecutor.PYTHON_FUNCTION_ENV_VAR_NAME));
        assertEquals("ziggy-python", subtaskExecutor.binaryName());
    }

    @Test
    public void testFunctionNameIsVirtualEnvironment() throws IOException {
        Files.createDirectories(DirectoryProperties.pythonEnvDir()
            .resolve(PythonSubtaskExecutor.PYTHON_DEFAULT_VENV_NAME));
        Files.createDirectories(DirectoryProperties.pythonEnvDir().resolve("cal"));
        Files.createDirectories(DirectoryProperties.pythonEnvDir().resolve("pa"));
        Files.createDirectories(DirectoryProperties.pythonEnvDir().resolve("pa-initializer"));

        SubtaskExecutor subtaskExecutor = SubtaskExecutorFactory.newInstance(subtaskMaster);
        Map<String, String> environment = subtaskExecutor.getEnvironment();
        assertEquals(DirectoryProperties.pythonEnvDir().resolve("pa-initializer").toString(),
            environment.get(PythonSubtaskExecutor.VIRTUAL_ENVIRONMENT_ENV_VAR_NAME));
        assertEquals("pa.pa", environment.get(PythonSubtaskExecutor.PYTHON_MODULE_ENV_VAR_NAME));
        assertEquals("pa-initializer",
            environment.get(PythonSubtaskExecutor.PYTHON_FUNCTION_ENV_VAR_NAME));
        assertEquals("ziggy-python", subtaskExecutor.binaryName());
    }
}
