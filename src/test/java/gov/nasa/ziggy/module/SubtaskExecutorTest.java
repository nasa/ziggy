package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyNames.MODULE_EXE_BINPATH_PROPERTY_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.MODULE_EXE_LIBPATH_PROPERTY_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.MODULE_EXE_MCRROOT_PROPERTY_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.PIPELINE_HOME_DIR_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.RESULTS_DIR_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_HOME_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.os.OperatingSystemType;

/**
 * Unit test class for {@link SubtaskExecutor}.
 *
 * @author PT
 */
public class SubtaskExecutorTest {

    // Note: this is a relatively limited unit test class. Unfortunately, some of the methods
    // (in particular execAlgorithm) simply do too much for me to write a tractable unit test
    // at the present time.
    private File rootDir;
    private File taskDir;
    private File subTaskDir;
    private SubtaskExecutor externalProcessExecutor;
    private ExternalProcess externalProcess;
    private TaskConfigurationManager taskConfigurationManager = new TaskConfigurationManager();
    private File buildDir;
    private File binDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule moduleExeBinpathPropertyRule = new ZiggyPropertyRule(
        MODULE_EXE_BINPATH_PROPERTY_NAME, (String) null);

    @Rule
    public ZiggyPropertyRule moduleExeLibpathPropertyRule = new ZiggyPropertyRule(
        MODULE_EXE_LIBPATH_PROPERTY_NAME, "path1" + File.pathSeparator + "path2");

    @Rule
    public ZiggyPropertyRule moduleExeMcrrootPropertyRule = new ZiggyPropertyRule(
        MODULE_EXE_MCRROOT_PROPERTY_NAME, (String) null);

    @Rule
    public ZiggyPropertyRule pipelineHomeDirPropertyRule = new ZiggyPropertyRule(
        PIPELINE_HOME_DIR_PROP_NAME, (String) null);

    @Rule
    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR_PROP_NAME,
        (String) null);

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_HOME_DIR_PROP_NAME, "/path/to/ziggy/build");

    @Before
    public void setup() throws IOException, ConfigurationException {

        rootDir = directoryRule.directory().toFile();
        taskDir = new File(rootDir, "10-20-pa");
        subTaskDir = new File(taskDir, "st-0");
        subTaskDir.mkdirs();
        buildDir = new File(rootDir, "build");
        binDir = new File(buildDir, "bin");
        binDir.mkdirs();
        File paFile = new File(binDir, "pa");
        paFile.createNewFile();

        System.setProperty(PIPELINE_HOME_DIR_PROP_NAME, buildDir.toString());
        System.setProperty(RESULTS_DIR_PROP_NAME, new File(rootDir, "results").getAbsolutePath());
        new File(System.getProperty(RESULTS_DIR_PROP_NAME)).mkdirs();

        // Create the state file directory
        Files.createDirectories(DirectoryProperties.stateFilesDir());

        // Create the state file
        StateFile stateFile = new StateFile("pa", 10, 20);
        stateFile.setPfeArrivalTimeMillis(System.currentTimeMillis());
        stateFile.persist();
    }

    @Test
    public void testConstructor() throws IOException {

        // Basic construction
        SubtaskExecutor e = new SubtaskExecutor.Builder().binaryName("pa")
            .taskDir(taskDir)
            .subtaskIndex(0)
            .timeoutSecs(1000000)
            .pipelineHomeDir(buildDir.getAbsolutePath())
            .pipelineConfigPath("/dev/null")
            .build();
        assertEquals("pa", e.binaryName());
        assertEquals(binDir.getCanonicalPath(), e.binaryDir().getCanonicalPath());
        assertEquals(1000000, e.timeoutSecs());

        assertEquals("path1" + File.pathSeparator + "path2", e.libPath());

        // with MATLAB paths defined
        System.setProperty(PropertyNames.MODULE_EXE_MCRROOT_PROPERTY_NAME, "/path/to/mcr/v22");
        e = new SubtaskExecutor.Builder().binaryName("pa")
            .taskDir(taskDir)
            .subtaskIndex(0)
            .timeoutSecs(1000000)
            .pipelineHomeDir(buildDir.getAbsolutePath())
            .pipelineConfigPath("/dev/null")
            .build();
        String[] libPaths = e.libPath().split(File.pathSeparator);

        // NB: there's no way to mock out the OS detection from this package, and the
        // size and content of the resulting lib path depends on the OS, so:
        assertTrue(libPaths.length >= 5);
        assertEquals("path1", libPaths[0]);
        assertEquals("path2", libPaths[1]);
        assertTrue(libPaths[2].startsWith("/path/to/mcr/v22/runtime"));
        assertTrue(libPaths[3].startsWith("/path/to/mcr/v22/bin"));
        assertTrue(libPaths[4].startsWith("/path/to/mcr/v22/sys/os"));
        if (libPaths.length == 6) {
            assertTrue(libPaths[5].startsWith("/path/to/mcr/v22/sys/opengl/lib"));
        }

        // If this is a Mac, test the binaryFile + ".app" functionality
        if (OperatingSystemType.getInstance() == OperatingSystemType.MAC_OS_X) {
            File calFile = new File(binDir, "cal.app");
            calFile = new File(calFile, "Contents");
            calFile = new File(calFile, "MacOS");
            calFile.mkdirs();
            calFile = new File(calFile, "cal");
            calFile.createNewFile();
            e = new SubtaskExecutor.Builder().binaryName("cal")
                .taskDir(taskDir)
                .subtaskIndex(0)
                .timeoutSecs(1000000)
                .pipelineHomeDir(buildDir.getAbsolutePath())
                .pipelineConfigPath("/dev/null")
                .build();
            assertEquals("cal", e.binaryName());
        }

    }

    /**
     * Exercises the case in which the user has specified a binPath, and the executable is somewhere
     * on the binPath. In this case, it's on both the binPath and in the build/bin directory, so the
     * constructor should find the one on the binPath.
     */
    @Test
    public void testBinPath() throws IOException {

        String phonyBinDir1 = rootDir.getCanonicalPath() + "/phony1";
        String phonyBinDir2 = rootDir.getCanonicalPath() + "/phony2";
        String binDir3 = rootDir.getCanonicalPath() + "/binDir3";
        File binDir3File = new File(binDir3);
        binDir3File.mkdirs();
        new File(binDir3, "pa").createNewFile();
        String binPath = phonyBinDir1 + File.pathSeparator + phonyBinDir2 + File.pathSeparator
            + binDir3;
        System.setProperty(PropertyNames.MODULE_EXE_BINPATH_PROPERTY_NAME, binPath);
        SubtaskExecutor e = new SubtaskExecutor.Builder().binaryName("pa")
            .taskDir(taskDir)
            .subtaskIndex(0)
            .timeoutSecs(1000000)
            .pipelineHomeDir(buildDir.getAbsolutePath())
            .pipelineConfigPath("/dev/null")
            .build();
        assertEquals("pa", e.binaryName());
        assertEquals(binDir3File.getCanonicalPath(), e.binaryDir().getCanonicalPath());

    }

    @Test
    public void testRunInputsOutputsCommand() throws ExecuteException, IOException {

        setUpMockedObjects();

        Mockito.when(externalProcess.execute()).thenReturn(0);
        int retCode = externalProcessExecutor.runInputsOutputsCommand(PipelineInputsSample.class);
        CommandLine commandLine = externalProcessExecutor.commandLine();
        Mockito.verify(externalProcess).setWorkingDirectory(subTaskDir);
        String cmdString = commandLine.toString();
        String expectedCommandString = "[/path/to/ziggy/build/bin/runjava, --verbose, "
            + "-Djava.library.path=/path/to/ziggy/build/lib, "
            + "-Dlog4j2.configurationFile=/path/to/ziggy/build/etc/log4j2.xml, "
            + "gov.nasa.ziggy.module.TaskFileManager, "
            + "gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample]";
        assertEquals(expectedCommandString, cmdString);
        assertEquals(0, retCode);

    }

    /**
     * Tests that an error in the {@link ExternalProcess} for the inputs-outputs command results in
     * a non-zero return code for the method.
     */
    @Test
    public void testErrorInInputsCommand() throws IOException {
        setUpMockedObjects();

        Mockito.when(externalProcess.execute()).thenReturn(1);
        int retCode = externalProcessExecutor.runInputsOutputsCommand(PipelineInputsSample.class);
        assertEquals(1, retCode);
    }

    /**
     * Tests that an error in the inputs-outputs command results in the subtask being marked as
     * failed without any attempt to execute the main algorithm command.
     */
    @Test
    public void testInputsErrorSetsErrorStatus() throws Exception {
        setUpMockedObjects();
        Mockito.doReturn(taskConfigurationManager)
            .when(externalProcessExecutor)
            .taskConfigurationManager();
        taskConfigurationManager.setInputsClass(PipelineInputsSample.class);
        Mockito.doReturn(1)
            .when(externalProcessExecutor)
            .runInputsOutputsCommand(PipelineInputsSample.class);
        int retCode = externalProcessExecutor.execAlgorithmInternal(0);
        assertEquals(1, retCode);
        assertTrue(new File(subTaskDir, ".FAILED").exists());
        Mockito.verify(externalProcessExecutor, Mockito.never())
            .runCommandline(ArgumentMatchers.anyList(), ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class));
    }

    @Test
    public void testRunCommandLine() throws Exception {

        setUpMockedObjects();

        Mockito.when(externalProcess.execute()).thenReturn(0);
        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("dummyArg1");
        cmdLineArgs.add("dummyArg2");
        int retCode = externalProcessExecutor.runCommandline(cmdLineArgs, "a", "b");
        CommandLine commandLine = externalProcessExecutor.commandLine();
        Mockito.verify(externalProcess).setWorkingDirectory(subTaskDir);
        Mockito.verify(externalProcess).setCommandLine(commandLine);
        Mockito.verify(externalProcess).setEnvironment(ArgumentMatchers.anyMap());
        String cmdString = commandLine.toString();
        assertEquals(0, retCode);

        // Unfortunately there's some ambiguity in the command based on which OS we run, so
        // we'll have to do some kind of indirect checking of the command
        assertTrue(cmdString.startsWith("[" + binDir.getAbsolutePath() + "/pa"));
        assertTrue(cmdString.endsWith("pa, dummyArg1, dummyArg2]"));

    }

    private void setUpMockedObjects() throws IOException {
        externalProcess = Mockito.mock(MockableExternalProcess.class);
        externalProcessExecutor = new SubtaskExecutor.Builder().binaryName("pa")
            .taskDir(taskDir)
            .subtaskIndex(0)
            .timeoutSecs(1000000)
            .pipelineHomeDir(buildDir.getAbsolutePath())
            .pipelineConfigPath("/dev/null")
            .build();
        externalProcessExecutor = Mockito.spy(externalProcessExecutor);
        Mockito.when(externalProcessExecutor.externalProcess(ArgumentMatchers.isNull(),
            ArgumentMatchers.isNull())).thenReturn(externalProcess);
        Mockito.when(externalProcessExecutor.externalProcess(ArgumentMatchers.any(Writer.class),
            ArgumentMatchers.any(Writer.class))).thenReturn(externalProcess);
    }

    private static class MockableExternalProcess extends ExternalProcess {

        public MockableExternalProcess() {
            super(true, null, true, null);
        }
    }
}
