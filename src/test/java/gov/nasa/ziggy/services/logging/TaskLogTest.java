package gov.nasa.ziggy.services.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.logging.TaskLog.LogType;
import gov.nasa.ziggy.util.io.Filenames;

/**
 * Unit test for {@link TaskLog}
 *
 * @author Todd Klaus
 * @author PT
 */
public class TaskLogTest {
    private static final String LOG4J_CONFIG_FILE = "test/data/logging/log4j2.xml";
    private static Logger log;

    private static final String TEST_LOG_MESSAGE_1 = "test log message 1";
    private static final String TEST_LOG_MESSAGE_2 = "test log message 2";

    private static final int THREAD_NUMBER_1 = 5;
    private static final int THREAD_NUMBER_2 = 6;

    private static final int INSTANCE_ID = 2;
    private static final int TASK_ID = 42;

    private static final int STEP_INDEX_0 = 0;
    private static final int STEP_INDEX_1 = 1;
    private static final int STEP_INDEX_2 = 2;
    private static final int STEP_INDEX_4 = 4;
    private static final int JOB_INDEX = 10;
    private static final int NEXT_JOB_INDEX = 11;

    private File expectedTaskLogFile1;
    private File expectedTaskLogFile2;
    private File algorithmLog1;
    private File algorithmLog2;

    private File algorithmDirectory;
    private File taskLogDirectory;
    private File stateFileDirectory;

    private Long timestamp;

    @Before
    public void setup() {
        System.setProperty("log4j2.configurationFile", LOG4J_CONFIG_FILE);
        log = LoggerFactory.getLogger(FileAppenderTest.class);
        File resultsDir = new File(System.getProperty("user.dir"), "build/test");
        System.setProperty(PropertyNames.RESULTS_DIR_PROP_NAME, resultsDir.getAbsolutePath());
        algorithmDirectory = DirectoryProperties.algorithmLogsDir().toFile();
        algorithmDirectory.mkdirs();
        taskLogDirectory = DirectoryProperties.taskLogDir().toFile();
        taskLogDirectory.mkdirs();
        stateFileDirectory = DirectoryProperties.stateFilesDir().toFile();
    }

    @After
    public void teardown() throws InterruptedException, URISyntaxException, IOException {
        System.clearProperty("log4j2.configurationFile");
        if (expectedTaskLogFile1 != null) {
            expectedTaskLogFile1.delete();
        }
        if (expectedTaskLogFile2 != null) {
            expectedTaskLogFile2.delete();
        }
        if (algorithmLog1 != null) {
            algorithmLog1.delete();
        }
        if (algorithmLog2 != null) {
            algorithmLog2.delete();
        }
        if (algorithmDirectory != null) {
            algorithmDirectory.delete();
        }
        if (taskLogDirectory != null) {
            taskLogDirectory.delete();
        }
        if (stateFileDirectory != null) {
            stateFileDirectory.delete();
        }
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(new URI(LOG4J_CONFIG_FILE));
        System.clearProperty(PropertyNames.RESULTS_DIR_PROP_NAME);
        System.clearProperty("log4j2.configurationFile");
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
    }

    @Test
    public void testTaskLog() throws IOException {
        expectedTaskLogFile1 = createAndPopulateTaskLog(THREAD_NUMBER_1, INSTANCE_ID, TASK_ID,
            STEP_INDEX_0, TEST_LOG_MESSAGE_1, THREAD_NUMBER_2, TEST_LOG_MESSAGE_2).toFile();
        assertTrue("log file exists", expectedTaskLogFile1.exists());

        List<String> logContents = FileUtils.readLines(expectedTaskLogFile1, (String) null);

        assertEquals("log file # lines", 1, logContents.size());
        assertTrue("log file contents", logContents.get(0).contains(TEST_LOG_MESSAGE_1));
    }

    @Test
    public void testTaskLogEnum() {
        expectedTaskLogFile1 = createAndPopulateTaskLog(THREAD_NUMBER_1, INSTANCE_ID, TASK_ID,
            STEP_INDEX_0, TEST_LOG_MESSAGE_1, 0, null).toFile();
        Matcher matcher = TaskLogInformation.LOG_FILE_NAME_PATTERN
            .matcher(expectedTaskLogFile1.getName());
        assertTrue(matcher.matches());
        assertEquals(INSTANCE_ID, Integer.parseInt(matcher.group(1)));
        assertEquals(TASK_ID, Integer.parseInt(matcher.group(2)));
        assertEquals(STEP_INDEX_0, Integer.parseInt(matcher.group(3)));
    }

    @Test
    public void testAlgorithmLogEnum() {
        PipelineTask task = createPipelineTask(INSTANCE_ID, TASK_ID, JOB_INDEX, STEP_INDEX_4);
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
        expectedTaskLogFile1 = createAndPopulateTaskLog(THREAD_NUMBER_1, INSTANCE_ID, TASK_ID,
            STEP_INDEX_2, TEST_LOG_MESSAGE_1, THREAD_NUMBER_2, TEST_LOG_MESSAGE_2).toFile();

        // create a couple of algorithm logs
        algorithmLog1 = createAlgorithmTaskLog(INSTANCE_ID, TASK_ID, JOB_INDEX, STEP_INDEX_1);
        algorithmLog2 = createAlgorithmTaskLog(INSTANCE_ID, TASK_ID, NEXT_JOB_INDEX, STEP_INDEX_1);

        // create a log file for the Java persisting step
        expectedTaskLogFile2 = createAndPopulateTaskLog(THREAD_NUMBER_1, INSTANCE_ID, TASK_ID,
            STEP_INDEX_0, "extremely long test message", THREAD_NUMBER_2, TEST_LOG_MESSAGE_2)
                .toFile();

        // Try to get TaskLogInformation for each of the created files
        Set<TaskLogInformation> taskLogInformationSet = TaskLog.searchForLogFiles(INSTANCE_ID,
            TASK_ID);

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
        algorithmLog1 = createAndPopulateTaskLog(
            Paths.get("build/test/logs/algorithm/2-42-testexename.0-1.log"), THREAD_NUMBER_1,
            TEST_LOG_MESSAGE_1, THREAD_NUMBER_2, TEST_LOG_MESSAGE_2).toFile();

        assertTrue("log file exists", algorithmLog1.exists());

        List<String> logContents = FileUtils.readLines(algorithmLog1, (String) null);

        assertEquals("log file # lines", 2, logContents.size());
        assertTrue("log file contents", logContents.get(0).contains(TEST_LOG_MESSAGE_1));
        assertTrue("log file contents", logContents.get(1).contains(TEST_LOG_MESSAGE_2));

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

    private Path createAndPopulateTaskLog(Path taskFileName, int threadNumber, String message,
        int altThreadNumber, String altLogMessage) {
        TaskLog taskLog = new TaskLog(taskFileName.toString());
        populateTaskLog(taskLog, threadNumber, message, altThreadNumber, altLogMessage);
        return taskLog.getTaskLogFile();
    }

    private Path createAndPopulateTaskLog(int threadNumber, int instanceId, int taskId,
        int stepIndex, String message, int altThreadNumber, String altLogMessage) {
        TaskLog taskLog = new TaskLog(threadNumber,
            createPipelineTask(instanceId, taskId, 0, stepIndex));
        populateTaskLog(taskLog, threadNumber, message, altThreadNumber, altLogMessage);
        return taskLog.getTaskLogFile();
    }

    private void populateTaskLog(TaskLog taskLog, int threadNumber, String message,
        int altThreadNumber, String altLogMessage) {
        taskLog.startLogging();

        log.info(message);

        if (altLogMessage != null) {
            ThreadContext.put(TaskLog.THREAD_NAME_KEY, "thread-" + altThreadNumber);
            log.info(altLogMessage);
        }

        taskLog.endLogging();
    }

    private PipelineTask createPipelineTask(int instanceId, int taskId, int jobIndex,
        int stepIndex) {
        PipelineTask task = new PipelineTask();
        PipelineInstance instance = new PipelineInstance();
        instance.setId(instanceId);
        task.setId(taskId);
        task.setPipelineInstance(instance);
        PipelineModuleDefinition module = new PipelineModuleDefinition("testexename");
        PipelineInstanceNode node = new PipelineInstanceNode();
        node.setPipelineModuleDefinition(module);
        task.setPipelineInstanceNode(node);
        task.setTaskLogIndex(stepIndex);
        return task;
    }

    private File createAlgorithmTaskLog(int instanceId, int taskId, int jobIndex, int stepIndex)
        throws IOException {
        PipelineTask task = createPipelineTask(instanceId, taskId, jobIndex, stepIndex);
        String algorithmLogFilename = task.logFilename(jobIndex);
        File algorithmLogFile = new File(algorithmDirectory, algorithmLogFilename);
        algorithmLogFile.createNewFile();
        return algorithmLogFile;

    }

}
