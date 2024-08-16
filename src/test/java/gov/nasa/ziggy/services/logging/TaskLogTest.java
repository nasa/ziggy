package gov.nasa.ziggy.services.logging;

import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.logging.TaskLog.LogType;
import gov.nasa.ziggy.services.process.ExternalProcess;

/**
 * Unit test for {@link TaskLog}
 *
 * @author Todd Klaus
 * @author PT
 */
public class TaskLogTest {
    private static final Path LOG4J_CONFIG_PATH = Paths.get("etc").resolve("log4j2.xml");
    private static final String LOG4J_CONFIG_FILE = LOG4J_CONFIG_PATH.toString();

    private static final String TEST_LOG_MESSAGE_1 = "test_log_message_1";

    private static final long INSTANCE_ID = 2;
    private static final long TASK_ID = 42;

    private static final int STEP_INDEX_0 = 0;
    private static final int STEP_INDEX_1 = 1;
    private static final int STEP_INDEX_2 = 2;
    private static final int STEP_INDEX_4 = 4;
    private static final int JOB_INDEX = 10;
    private static final int NEXT_JOB_INDEX = 11;

    private File algorithmLog1;

    private Long timestamp;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule log4j2ConfigurationFilePropertyRule = new ZiggyPropertyRule(
        PropertyName.LOG4J2_CONFIGURATION_FILE, LOG4J_CONFIG_FILE);

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(resultsDirPropertyRule);

    @After
    public void teardown() throws InterruptedException, URISyntaxException, IOException {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(new URI(LOG4J_CONFIG_FILE));
    }

    @Test
    public void testTaskLog() throws IOException {
        createAndPopulateZiggyTaskLog(INSTANCE_ID, TASK_ID, STEP_INDEX_0, TEST_LOG_MESSAGE_1);
        File expectedTaskLogFile1 = DirectoryProperties.taskLogDir()
            .resolve(createPipelineTask(INSTANCE_ID, TASK_ID, STEP_INDEX_0).logFilename(0))
            .toFile();
        assertTrue("log file exists", expectedTaskLogFile1.exists());

        List<String> logContents = FileUtils.readLines(expectedTaskLogFile1, (String) null);

        assertEquals("log file # lines", 1, logContents.size());
        assertTrue("log file contents", logContents.get(0).contains(TEST_LOG_MESSAGE_1));
    }

    @Test
    public void testTaskLogEnum() {
        createAndPopulateZiggyTaskLog(INSTANCE_ID, TASK_ID, STEP_INDEX_0, TEST_LOG_MESSAGE_1);
        File expectedTaskLogFile1 = DirectoryProperties.taskLogDir()
            .resolve(createPipelineTask(INSTANCE_ID, TASK_ID, STEP_INDEX_0).logFilename(0))
            .toFile();
        Matcher matcher = TaskLogInformation.LOG_FILE_NAME_PATTERN
            .matcher(expectedTaskLogFile1.getName());
        assertTrue(matcher.matches());
        assertEquals(INSTANCE_ID, Integer.parseInt(matcher.group(1)));
        assertEquals(TASK_ID, Integer.parseInt(matcher.group(2)));
        assertEquals(STEP_INDEX_0, Integer.parseInt(matcher.group(3)));
    }

    @Test
    public void testAlgorithmLogEnum() {
        PipelineTask task = createPipelineTask(INSTANCE_ID, TASK_ID, STEP_INDEX_4);
        String algorithmLogFilename = task.logFilename(JOB_INDEX);

        Matcher matcher = TaskLogInformation.LOG_FILE_NAME_PATTERN.matcher(algorithmLogFilename);
        assertTrue(matcher.matches());
        assertEquals(INSTANCE_ID, Integer.parseInt(matcher.group(1)));
        assertEquals(TASK_ID, Integer.parseInt(matcher.group(2)));
        assertEquals(JOB_INDEX, Integer.parseInt(matcher.group(3)));
        assertEquals(STEP_INDEX_4, Integer.parseInt(matcher.group(4)));
    }

    @Test
    public void testSearchForLogFiles() throws IOException {

        // Get current timestamp rounded down to nearest second,
        // since that's the granularity of the file last modified
        // time on Mac OS, at least.
        timestamp = System.currentTimeMillis() / 1000L * 1000L;

        // create a log file for the initial Java step
        createAndPopulateZiggyTaskLog(INSTANCE_ID, TASK_ID, STEP_INDEX_2, TEST_LOG_MESSAGE_1);

        // create a couple of algorithm logs
        createAlgorithmTaskLog(INSTANCE_ID, TASK_ID, JOB_INDEX, STEP_INDEX_1);
        createAlgorithmTaskLog(INSTANCE_ID, TASK_ID, NEXT_JOB_INDEX, STEP_INDEX_1);

        // create a log file for the Java persisting step
        createAndPopulateZiggyTaskLog(INSTANCE_ID, TASK_ID, STEP_INDEX_0,
            "extremely_long_test_message");

        // Try to get TaskLogInformation for each of the created files
        Set<TaskLogInformation> taskLogInformationSet = TaskLog.searchForLogFiles(TASK_ID);

        // Check values in the TaskLogInformation instances.
        assertEquals(4, taskLogInformationSet.size());
        int taskLogInfoCounter = 0;
        for (TaskLogInformation taskLogInfo : taskLogInformationSet) {
            checkTaskLogInformationValues(taskLogInfo, taskLogInfoCounter);
            taskLogInfoCounter++;
        }
    }

    @Test
    public void testAlgorithmTaskLog() throws IOException {

        // Create a log file for the algorithm
        createAndPopulateAlgorithmTaskLog(INSTANCE_ID, TASK_ID, STEP_INDEX_1, TEST_LOG_MESSAGE_1);
        algorithmLog1 = DirectoryProperties.algorithmLogsDir()
            .resolve(createPipelineTask(INSTANCE_ID, TASK_ID, STEP_INDEX_1).logFilename(0))
            .toFile();
        assertTrue("log file exists", algorithmLog1.exists());

        List<String> logContents = FileUtils.readLines(algorithmLog1, (String) null);

        assertEquals("log file # lines", 1, logContents.size());
        assertTrue("log file contents", logContents.get(0).contains(TEST_LOG_MESSAGE_1));
    }

    private void checkTaskLogInformationValues(TaskLogInformation taskLogInfo,
        int taskLogInfoCounter) {
        assertEquals(INSTANCE_ID, taskLogInfo.getInstanceId());
        assertEquals(TASK_ID, taskLogInfo.getTaskId());
        assertTrue(taskLogInfo.getLastModified() >= timestamp);
        LogType expectedLogType = null;
        String expectedFilename = null;
        int expectedTaskLogIndex = -1;
        int expectedJobIndex = -1;
        switch (taskLogInfoCounter) {
            case 0:
                expectedLogType = LogType.ZIGGY;
                expectedFilename = "2-42-testexename.0-0.log";
                expectedTaskLogIndex = 0;
                expectedJobIndex = 0;
                break;
            case 1:
                expectedLogType = LogType.ALGORITHM;
                expectedFilename = "2-42-testexename.10-1.log";
                expectedTaskLogIndex = 1;
                expectedJobIndex = 10;
                break;
            case 2:
                expectedLogType = LogType.ALGORITHM;
                expectedFilename = "2-42-testexename.11-1.log";
                expectedTaskLogIndex = 1;
                expectedJobIndex = 11;
                break;
            case 3:
                expectedLogType = LogType.ZIGGY;
                expectedFilename = "2-42-testexename.0-2.log";
                expectedTaskLogIndex = 2;
                expectedJobIndex = 0;
                break;
        }
        assertEquals(expectedLogType, taskLogInfo.getLogType());
        assertEquals(expectedFilename, taskLogInfo.getFilename());
        assertEquals(expectedTaskLogIndex, taskLogInfo.getTaskLogIndex());
        assertEquals(expectedJobIndex, taskLogInfo.getJobIndex());
        if (expectedLogType == LogType.ZIGGY) {
            assertTrue(taskLogInfo.getLogSizeBytes() > 0);
        } else {
            assertTrue(taskLogInfo.getLogSizeBytes() == 0);
        }
    }

    private void createAndPopulateZiggyTaskLog(long instanceId, long taskId, int stepIndex,
        String message) {
        CommandLine commandLine = new CommandLine(DirectoryProperties.ziggyHomeDir()
            .getParent()
            .resolve("src")
            .resolve("main")
            .resolve("perl")
            .resolve("ziggy.pl")
            .toString());
        commandLine.addArgument("--verbose");
        commandLine.addArgument(
            TaskLog.ziggyLogFileSystemProperty(createPipelineTask(instanceId, taskId, stepIndex)));
        commandLine.addArgument("-D" + PropertyName.LOG4J2_CONFIGURATION_FILE.property() + "="
            + LOG4J_CONFIG_PATH.toAbsolutePath().toString());
        commandLine.addArgument("--class=" + TaskLogCreator.class.getName());
        commandLine.addArgument(message);

        ExternalProcess externalProcess = ExternalProcess.simpleExternalProcess(commandLine);
        externalProcess.setEnvironment(Map.of("JAVA_HOME",
            ZiggyConfiguration.getInstance().getString(PropertyName.JAVA_HOME.property()),
            "PIPELINE_CONFIG_PATH",
            DirectoryProperties.ziggyHomeDir()
                .getParent()
                .resolve("sample-pipeline")
                .resolve("etc")
                .resolve("sample.properties")
                .toString(),
            "ZIGGY_ROOT", DirectoryProperties.ziggyHomeDir().getParent().toString()));

        externalProcess.execute();
    }

    private void createAndPopulateAlgorithmTaskLog(long instanceId, long taskId, int stepIndex,
        String message) {

        CommandLine commandLine = new CommandLine(DirectoryProperties.ziggyHomeDir()
            .getParent()
            .resolve("src")
            .resolve("main")
            .resolve("perl")
            .resolve("ziggy.pl")
            .toString());
        commandLine.addArgument(TaskLog
            .algorithmLogFileSystemProperty(createPipelineTask(instanceId, taskId, stepIndex)));
        commandLine.addArgument("-D" + PropertyName.LOG4J2_CONFIGURATION_FILE.property() + "="
            + LOG4J_CONFIG_PATH.toAbsolutePath().toString());
        commandLine.addArgument("--class=" + TaskLogCreator.class.getName());
        commandLine.addArgument(message);

        ExternalProcess externalProcess = ExternalProcess.simpleExternalProcess(commandLine);
        externalProcess.setEnvironment(Map.of("JAVA_HOME",
            ZiggyConfiguration.getInstance().getString(PropertyName.JAVA_HOME.property()),
            "PIPELINE_CONFIG_PATH",
            DirectoryProperties.ziggyHomeDir()
                .getParent()
                .resolve("sample-pipeline")
                .resolve("etc")
                .resolve("sample.properties")
                .toString(),
            "ZIGGY_ROOT", DirectoryProperties.ziggyHomeDir().getParent().toString()));

        externalProcess.execute();
    }

    private PipelineTask createPipelineTask(long instanceId, long taskId, int stepIndex) {
        PipelineInstance instance = new PipelineInstance();
        instance.setId(instanceId);
        PipelineModuleDefinition module = new PipelineModuleDefinition("testexename");
        PipelineTask task = Mockito
            .spy(new PipelineTask(instance, new PipelineInstanceNode(null, module)));
        Mockito.doReturn(taskId).when(task).getId();
        task.setTaskLogIndex(stepIndex);
        return task;
    }

    private File createAlgorithmTaskLog(long instanceId, long taskId, int jobIndex, int stepIndex)
        throws IOException {
        PipelineTask task = createPipelineTask(instanceId, taskId, stepIndex);
        String algorithmLogFilename = task.logFilename(jobIndex);
        Files.createDirectories(DirectoryProperties.algorithmLogsDir());
        File algorithmLogFile = DirectoryProperties.algorithmLogsDir()
            .resolve(algorithmLogFilename)
            .toFile();
        algorithmLogFile.createNewFile();
        return algorithmLogFile;
    }
}
