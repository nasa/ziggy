package gov.nasa.ziggy.pipeline.step.subtask;

import static gov.nasa.ziggy.services.config.PropertyName.BINPATH;
import static gov.nasa.ziggy.services.config.PropertyName.LIBPATH;
import static gov.nasa.ziggy.services.config.PropertyName.MCRROOT;
import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HOME_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_LOG_SINGLE_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.PropertyName;

public class MatlabSubtaskExecutorTest {

    private File rootDir;
    private File taskDir;
    private File subtaskDir;
    private File buildDir;
    private File binDir;
    private String user = System.getenv("USER");
    private SubtaskMaster subtaskMaster = Mockito.mock(SubtaskMaster.class);

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule binpathPropertyRule = new ZiggyPropertyRule(BINPATH, (String) null);

    @Rule
    public ZiggyPropertyRule libpathPropertyRule = new ZiggyPropertyRule(LIBPATH,
        "path1" + File.pathSeparator + "path2");

    @Rule
    public ZiggyPropertyRule mcrrootPropertyRule = new ZiggyPropertyRule(MCRROOT,
        "/path/to/mcr/v22");

    @Rule
    public ZiggyPropertyRule userRule = new ZiggyPropertyRule(PropertyName.USER_NAME, user);

    // This rule can be set to anything, but if it's not set at all the unit tests fail.
    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        "/path/to/ziggy/build");

    public ZiggyPropertyRule pipelineHomeDirPropertyRule = new ZiggyPropertyRule(PIPELINE_HOME_DIR,
        directoryRule, "build");

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule, "results");

    public ZiggyPropertyRule logFilePropertyRule = new ZiggyPropertyRule(ZIGGY_LOG_SINGLE_FILE,
        directoryRule, "ziggy.log");

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(pipelineHomeDirPropertyRule)
        .around(resultsDirPropertyRule)
        .around(logFilePropertyRule);

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
        Files.createFile(binDir.toPath().resolve("pa-requiredMCRProducts.txt"));

        new File(resultsDirPropertyRule.getValue()).mkdirs();
        Mockito.when(subtaskMaster.getBinaryName()).thenReturn("pa");
        Mockito.when(subtaskMaster.taskDir()).thenReturn(taskDir);
        Mockito.when(subtaskMaster.getSubtaskIndex()).thenReturn(0);
        Mockito.when(subtaskMaster.getTimeoutSecs()).thenReturn(1000000);
    }

    @Test
    public void testConstructor() throws IOException {

        // Basic construction
        SubtaskExecutor e = SubtaskExecutorFactory.newInstance(subtaskMaster);
        assertTrue(e instanceof MatlabSubtaskExecutor);
        String[] libPaths = e.libPath().split(File.pathSeparator);

        // NB: there's no way to mock out the OS detection from this package, and the
        // size and content of the resulting lib path depends on the OS, so:
        assertTrue(libPaths.length >= 2);
        assertEquals("path1", libPaths[0]);
        assertEquals("path2", libPaths[1]);
        assertTrue(libPaths[2].startsWith("/path/to/mcr/v22/runtime"));
        assertTrue(libPaths[3].startsWith("/path/to/mcr/v22/bin"));
        assertTrue(libPaths[4].startsWith("/path/to/mcr/v22/sys/os"));
        if (libPaths.length == 6) {
            assertTrue(libPaths[5].startsWith("/path/to/mcr/v22/sys/opengl/lib"));
        }

        Map<String, String> environment = e.getEnvironment();
        assertEquals("/dev/null", environment.get("LM_LICENSE_FILE"));
        assertEquals("/tmp/" + user + "/mcr_cache_pa", environment.get("MCR_CACHE_ROOT"));
    }
}
