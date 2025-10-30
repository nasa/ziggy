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
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;
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
    private File subtaskDir;
    private SubtaskExecutor externalProcessExecutor;
    private ExternalProcess externalProcess;
    private TaskConfiguration taskConfigurationManager = new TaskConfiguration();
    private File buildDir;
    private File binDir;
    private SubtaskMaster subtaskMaster = Mockito.mock(SubtaskMaster.class);

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule binpathPropertyRule = new ZiggyPropertyRule(BINPATH, (String) null);

    @Rule
    public ZiggyPropertyRule libpathPropertyRule = new ZiggyPropertyRule(LIBPATH,
        "path1" + File.pathSeparator + "path2");

    @Rule
    public ZiggyPropertyRule mcrrootPropertyRule = new ZiggyPropertyRule(MCRROOT, (String) null);

    // This rule can be set to anything, but if it's not set at all the unit tests fail.
    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        "/path/to/ziggy/build");

    public ZiggyPropertyRule pipelineHomeDirPropertyRule = new ZiggyPropertyRule(PIPELINE_HOME_DIR,
        directoryRule, "build");

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule, "results");

    public ZiggyPropertyRule logFilePropertyRule = new ZiggyPropertyRule(ZIGGY_LOG_SINGLE_FILE,
        directoryRule, "ziggy.log.single.file");

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
        assertEquals("pa", e.binaryName());
        assertEquals(binDir.getCanonicalPath(), e.binaryDir().getCanonicalPath());
        assertEquals(1000000, e.timeoutSecs());

        assertEquals("path1" + File.pathSeparator + "path2", e.libPath());

        String[] libPaths = e.libPath().split(File.pathSeparator);

        // NB: there's no way to mock out the OS detection from this package, and the
        // size and content of the resulting lib path depends on the OS, so:
        assertTrue(libPaths.length >= 2);
        assertEquals("path1", libPaths[0]);
        assertEquals("path2", libPaths[1]);

        // If this is a Mac, test the binaryFile + ".app" functionality
        if (OperatingSystemType.newInstance() == OperatingSystemType.MAC_OS_X) {
            File calFile = new File(binDir, "cal.app");
            calFile = new File(calFile, "Contents");
            calFile = new File(calFile, "MacOS");
            calFile.mkdirs();
            calFile = new File(calFile, "cal");
            calFile.createNewFile();
            Mockito.when(subtaskMaster.getBinaryName()).thenReturn("cal");
            e = Mockito.spy(SubtaskExecutorFactory.newInstance(subtaskMaster));
            Mockito.doReturn("cal").when(e).pipelineStepName();
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
        binpathPropertyRule.setValue(binPath);
        SubtaskExecutor e = Mockito.spy(SubtaskExecutorFactory.newInstance(subtaskMaster));
        assertEquals("pa", e.binaryName());
        assertEquals(binDir3File.getCanonicalPath(), e.binaryDir().getCanonicalPath());
    }

    @Test
    public void testRunInputsOutputsCommand() throws ExecuteException, IOException {

        setUpMockedObjects();

        Mockito.when(externalProcess.execute()).thenReturn(0);
        int retCode = externalProcessExecutor.runInputsOutputsCommand(PipelineInputsSample.class);
        CommandLine commandLine = externalProcessExecutor
            .inputsOutputsCommandLine(PipelineInputsSample.class);
        Mockito.verify(externalProcess).setWorkingDirectory(subtaskDir);
        String cmdString = commandLine.toString();
        String expectedCommandString = """
            [/path/to/ziggy/build/bin/ziggy, --verbose,\s\
            -Xmx0M,\s\
            -Djava.library.path=path1:path2:/path/to/ziggy/build/lib,\s\
            -Dlog4j2.configurationFile=/path/to/ziggy/build/etc/log4j2.xml,\s\
            -Dziggy.log.single.file=""" + logFilePropertyRule.getValue() + """
            ,\s\
            -Dziggy.log.appender=singleFile,\s\
            --class=gov.nasa.ziggy.pipeline.step.subtask.BeforeAndAfterAlgorithmExecutor,\s\
            gov.nasa.ziggy.data.management.DataFileTestUtils.PipelineInputsSample]""";
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
            .getTaskConfiguration();
        taskConfigurationManager.setInputsClass(PipelineInputsSample.class);
        Mockito.doReturn(1)
            .when(externalProcessExecutor)
            .runInputsOutputsCommand(PipelineInputsSample.class);
        int retCode = externalProcessExecutor.execAlgorithmInternal();
        assertEquals(1, retCode);
        assertTrue(new File(subtaskDir, ".FAILED").exists());
        Mockito.verify(externalProcessExecutor, Mockito.never())
            .runCommandline(ArgumentMatchers.anyList(), ArgumentMatchers.any(String.class));
    }

    @Test
    public void testRunCommandLine() throws Exception {

        setUpMockedObjects();

        Mockito.when(externalProcess.execute()).thenReturn(0);
        List<String> cmdLineArgs = new ArrayList<>();
        cmdLineArgs.add("dummyArg1");
        cmdLineArgs.add("dummyArg2");
        int retCode = externalProcessExecutor.runCommandline(cmdLineArgs, "a");
        CommandLine commandLine = externalProcessExecutor.algorithmCommandLine(cmdLineArgs);
        Mockito.verify(externalProcess).setWorkingDirectory(subtaskDir);
        Mockito.verify(externalProcess).setCommandLine(commandLine);
        Mockito.verify(externalProcess).mergeWithEnvironment(ArgumentMatchers.anyMap());
        String cmdString = commandLine.toString();
        assertEquals(0, retCode);

        // Unfortunately there's some ambiguity in the command based on which OS we run, so
        // we'll have to do some kind of indirect checking of the command
        assertTrue(cmdString.startsWith("[" + binDir.getAbsolutePath() + "/pa"));
        assertTrue(cmdString.endsWith("pa, dummyArg1, dummyArg2]"));
    }

    private void setUpMockedObjects() throws IOException {
        externalProcess = Mockito.mock(MockableExternalProcess.class);
        externalProcessExecutor = SubtaskExecutorFactory.newInstance(subtaskMaster);
        externalProcessExecutor = Mockito.spy(externalProcessExecutor);
        Mockito.doReturn("pa").when(externalProcessExecutor).pipelineStepName();

        Mockito.when(externalProcessExecutor.externalProcess(ArgumentMatchers.isNull(),
            ArgumentMatchers.isNull())).thenReturn(externalProcess);
        Mockito.when(externalProcessExecutor.externalProcess(ArgumentMatchers.any(Writer.class),
            ArgumentMatchers.any(Writer.class))).thenReturn(externalProcess);
    }

    public static class MockableExternalProcess extends ExternalProcess {

        public MockableExternalProcess() {
            super(true, null, true, null);
        }
    }
}
