package gov.nasa.ziggy.pipeline.step.remote.batch;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.exec.CommandLine;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.remote.Architecture;
import gov.nasa.ziggy.pipeline.step.remote.ArchitectureTestUtils;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueue;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueueTestUtils;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.pipeline.step.remote.RemoteJobInformation;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.process.ExternalProcess;

/** Unit tests for {@link PbsBatchManager} class. */

public class PbsBatchManagerTest {

    private static final Pattern commandLinePattern = Pattern.compile("""
        \\[qsub, -N, (1-2-pa\\.[0-9]+), -q, normal, -rn, \
        -l, \
        walltime=4:30:00,select=1:model=ivy, -W, group_list=12345, -W, umask=027, -v, \
        ENV1=foo,ENV2=bar,ENV3=baz,ENV4=bazbaz, -o, (\\S+?), \
        -j, oe, --, /path/to/ziggy/home/bin/ziggy, test, \
        -Dziggy.log.appender=singleFile, -Dziggy.algorithm.name=null, \
        compute-node-master, (\\S+)""");

    @Rule
    public ZiggyPropertyRule groupPropertyRule = new ZiggyPropertyRule("ziggy.remote.hecc.group",
        "12345");

    @Rule
    public ZiggyPropertyRule ziggyRuntimeEnvironmentRule = new ZiggyPropertyRule(
        PropertyName.ZIGGY_RUNTIME_ENVIRONMENT.property(), "ENV1=foo,ENV2=bar");

    @Rule
    public ZiggyPropertyRule userRuntimeEnvironmentRule = new ZiggyPropertyRule(
        PropertyName.RUNTIME_ENVIRONMENT.property(), "ENV3=baz,ENV4=bazbaz");

    @Rule
    public ZiggyPropertyRule ziggyHomeRule = new ZiggyPropertyRule(
        PropertyName.ZIGGY_HOME_DIR.property(), "/path/to/ziggy/home");

    @Rule
    public ZiggyPropertyRule remoteUserRule = new ZiggyPropertyRule(
        PropertyName.REMOTE_PROPERTY_PREFIX + "hecc.user", "user");

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule resultsDirRule = new ZiggyPropertyRule("ziggy.pipeline.results.dir",
        directoryRule);

    @Rule
    public RuleChain resultsDirRuleChain = RuleChain.outerRule(directoryRule)
        .around(resultsDirRule);

    private PipelineNodeExecutionResources executionResources;
    private RemoteEnvironment remoteEnvironment = Mockito.mock(RemoteEnvironment.class);
    private Map<String, Architecture> architectureByName;
    private PipelineTask pipelineTask = Mockito.mock(PipelineTask.class);
    private ExternalProcess qsubExternalProcess = Mockito.mock(ExternalProcess.class);
    private PipelineTaskOperations pipelineTaskOperations = Mockito
        .mock(PipelineTaskOperations.class);
    private PbsBatchManager pbsBatchManager = Mockito.spy(PbsBatchManager.class);
    private ExternalProcess qstatExternalProcess1 = Mockito.mock(ExternalProcess.class);
    private ExternalProcess qstatExternalProcess2 = Mockito.mock(ExternalProcess.class);
    private ExternalProcess qdelExternalProcess = Mockito.mock(ExternalProcess.class);

    @Before
    public void setUp() {
        architectureByName = ArchitectureTestUtils.architectureByName();
        executionResources = new PipelineNodeExecutionResources("dummy", "dummy");
        executionResources.setRemoteEnvironment(remoteEnvironment);
        List<Architecture> architectures = ArchitectureTestUtils.architectures();
        Mockito.when(remoteEnvironment.getArchitectures()).thenReturn(architectures);
        List<BatchQueue> batchQueues = BatchQueueTestUtils.batchQueues();
        Mockito.when(remoteEnvironment.getQueues()).thenReturn(batchQueues);
        Mockito.when(remoteEnvironment.getName()).thenReturn("hecc");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setSubtaskRamGigabytes(6);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setArchitecture(architectureByName.get("ivy"));
        Mockito.doReturn(pipelineTaskOperations).when(pbsBatchManager).pipelineTaskOperations();
        Mockito.doReturn(qsubExternalProcess)
            .when(pbsBatchManager)
            .qsubExternalProcess(ArgumentMatchers.any(CommandLine.class));
        Mockito
            .when(
                pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(executionResources);
        Mockito.when(qsubExternalProcess.run(ArgumentMatchers.any(boolean.class),
            ArgumentMatchers.any(long.class))).thenReturn(0);
        Mockito.when(pipelineTask.taskBaseName()).thenReturn("1-2-pa");
        Mockito.doNothing().when(pbsBatchManager).createPbsLogDirectory();
        Mockito.doReturn("test")
            .when(pbsBatchManager)
            .algorithmLogFileSystemProperty(ArgumentMatchers.any(int.class));
    }

    @Test
    public void testSubmitJobs() {
        List<RemoteJobInformation> remoteJobsInformation = pbsBatchManager.submitJobs(pipelineTask,
            500);
        TestEventDetector.detectTestEvent(500L, () -> remoteJobsInformation.size() == 12);
        Map<String, RemoteJobInformation> remoteJobInformationByJobName = new HashMap<>();
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            assertEquals(0, remoteJobInformation.getBatchSubmissionExitCode());
            remoteJobInformationByJobName.put(remoteJobInformation.getJobName(),
                remoteJobInformation);
        }
        Path resultsDir = Paths
            .get(ZiggyConfiguration.getInstance().getString("ziggy.pipeline.results.dir"))
            .toAbsolutePath();
        for (int jobIndex = 0; jobIndex < 6; jobIndex++) {
            String jobName = "1-2-pa." + jobIndex;
            assertTrue(remoteJobInformationByJobName.containsKey(jobName));
            RemoteJobInformation remoteJobInformation = remoteJobInformationByJobName.get(jobName);
            assertTrue(remoteJobInformation.getLogFile()
                .startsWith(resultsDir.resolve("logs")
                    .resolve("pbs")
                    .resolve("pbs-" + jobName)
                    .toString()));
            assertEquals("hecc", remoteJobInformation.getRemoteEnvironmentName());
            assertEquals(0.66, remoteJobInformation.getCostFactor(), 1e-6);
        }

        TestEventDetector.detectTestEvent(500L,
            () -> pbsBatchManager.getCommandLines().size() == 12);
        List<String> commandLines = pbsBatchManager.getCommandLines()
            .stream()
            .map(CommandLine::toString)
            .collect(Collectors.toList());
        for (String commandLine : commandLines) {
            Matcher matcher = commandLinePattern.matcher(commandLine);
            assertTrue(matcher.matches());
            String jobName = matcher.group(1);
            String logFile = matcher.group(2);
            String taskDir = matcher.group(3);
            assertEquals(directoryRule.directory()
                .resolve("task-data")
                .resolve("1-2-pa]")
                .toAbsolutePath()
                .toString(), taskDir);
            assertTrue(logFile.startsWith(directoryRule.directory()
                .resolve("logs")
                .resolve("pbs")
                .resolve("pbs-" + jobName)
                .toAbsolutePath()
                .toString()));
        }
    }

    @Test
    public void testFailuresInJobSubmission() {
        Mockito.when(qsubExternalProcess.run(ArgumentMatchers.any(boolean.class),
            ArgumentMatchers.any(long.class))).thenReturn(0).thenReturn(1).thenReturn(0);
        List<RemoteJobInformation> remoteJobsInformation = pbsBatchManager.submitJobs(pipelineTask,
            500);
        TestEventDetector.detectTestEvent(500L, () -> remoteJobsInformation.size() == 12);
        int exitCodeZeroCounter = 0;
        int exitCodeOneCounter = 0;
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            if (remoteJobInformation.getBatchSubmissionExitCode() == 0) {
                exitCodeZeroCounter++;
            }
            if (remoteJobInformation.getBatchSubmissionExitCode() == 1) {
                exitCodeOneCounter++;
            }
        }
        assertEquals(5, exitCodeZeroCounter);
        assertEquals(1, exitCodeOneCounter);
    }

    @Test
    public void testJobIdByName() {
        PipelineTask tps200PipelineTask = Mockito.mock(PipelineTask.class);
        Mockito.when(tps200PipelineTask.taskBaseName()).thenReturn("100-200-tps");
        Mockito.doReturn("host1").when(pbsBatchManager).host();
        Mockito.doReturn(qstatExternalProcess1)
            .when(pbsBatchManager)
            .qstatExternalProcess("-u user");
        Mockito.when(qstatExternalProcess1.stdout("100-200-tps"))
            .thenReturn(
                List.of("1234567.batch user low    100-200-tps.0  5   5 04:00 R 02:33  254%",
                    "1234568.batch user low    100-200-tps.1  5   5 04:00 R 02:33  254%",
                    "1234569.batch user low    100-200-tps.2  5   5 04:00 R 02:33  254%"));
        Mockito.doReturn(qstatExternalProcess2)
            .when(pbsBatchManager)
            .qstatExternalProcess("-xf 1234567 1234568 1234569");
        Mockito.when(qstatExternalProcess2.stdout("Job:", "Job_Owner"))
            .thenReturn(
                List.of("Job: 1234567.batch.example.com", "    Job_Owner = user@host1.example.com",
                    "Job: 1234568.batch.example.com", "    Job_Owner = user@host1.example.com",
                    "Job: 1234569.batch.example.com", "    Job_Owner = user@host2.example.com"));
        Map<String, Long> jobIdByName = pbsBatchManager.jobIdByName(tps200PipelineTask);
        assertEquals(2, jobIdByName.size());
        assertTrue(jobIdByName.containsKey("100-200-tps.0"));
        assertEquals(1234567L, jobIdByName.get("100-200-tps.0").longValue());
        assertTrue(jobIdByName.containsKey("100-200-tps.1"));
        assertEquals(1234568L, jobIdByName.get("100-200-tps.1").longValue());
    }

    @Test
    public void testRemoteJobInformationIsFinished() throws IOException {
        List<RemoteJobInformation> remoteJobsInformation = pbsBatchManager.submitJobs(pipelineTask,
            500);
        Path logFile1 = Paths.get(remoteJobsInformation.get(0).getLogFile());
        Path logFileDir = logFile1.getParent();
        Files.createDirectories(logFileDir);
        Files.createFile(logFile1);
        assertTrue(pbsBatchManager.isFinished(remoteJobsInformation.get(0)));
        for (int jobIndex = 1; jobIndex < remoteJobsInformation.size(); jobIndex++) {
            assertFalse(pbsBatchManager.isFinished(remoteJobsInformation.get(jobIndex)));
        }
    }

    @Test
    public void testRemoteJobIsFinished() {
        RemoteJob remoteJob = new RemoteJob(1234567L);
        Mockito.doReturn(qstatExternalProcess1)
            .when(pbsBatchManager)
            .qstatExternalProcess("-xf 1234567");
        Mockito.when(qstatExternalProcess1.stdout("Exit_status"))
            .thenReturn(List.of("    Exit_status = 0"));
        assertTrue(pbsBatchManager.isFinished(remoteJob));
        Mockito.when(qstatExternalProcess1.stdout("Exit_status")).thenReturn(new ArrayList<>());
        assertFalse(pbsBatchManager.isFinished(remoteJob));
    }

    @Test
    public void testExitStatus() {
        RemoteJobInformation job0Information = new RemoteJobInformation(
            TEST_DATA.resolve("pbs-log-comment-and-status").toString(), "test1", "hecc");
        job0Information.setJobId(1023L);
        RemoteJobInformation job1Information = new RemoteJobInformation(
            TEST_DATA.resolve("pbs-log-status-no-comment.txt").toString(), "test2", "hecc");
        job1Information.setJobId(1024L);
        RemoteJobInformation job2Information = new RemoteJobInformation("no-such-file", "test3",
            "hecc");
        job2Information.setJobId(1025L);

        assertEquals(271, pbsBatchManager.exitStatus(job0Information).intValue());
        assertEquals(0, pbsBatchManager.exitStatus(job1Information).intValue());
        assertNull(pbsBatchManager.exitStatus(job2Information));
    }

    @Test
    public void testExitComment() {
        RemoteJobInformation job0Information = new RemoteJobInformation(
            TEST_DATA.resolve("pbs-log-comment-and-status").toString(), "test1", "hecc");
        job0Information.setJobId(1023L);
        RemoteJobInformation job1Information = new RemoteJobInformation(
            TEST_DATA.resolve("pbs-log-status-no-comment.txt").toString(), "test2", "hecc");
        job1Information.setJobId(1024L);
        RemoteJobInformation job2Information = new RemoteJobInformation("no-such-file", "test3",
            "hecc");
        job2Information.setJobId(1025L);

        assertEquals("job killed: walltime 1818 exceeded limit 1800",
            pbsBatchManager.exitComment(job0Information));
        assertNull(pbsBatchManager.exitComment(job1Information));
        assertNull(pbsBatchManager.exitComment(job2Information));
    }

    @Test
    public void testRemoteJobInformation() {
        RemoteJob remoteJob = new RemoteJob();
        remoteJob.setFinished(false);
        remoteJob.setJobId(1234567L);
        remoteJob.setRemoteEnvironmentName("hecc");
        Mockito.doReturn(qstatExternalProcess1)
            .when(pbsBatchManager)
            .qstatExternalProcess("-xf 1234567");
        Mockito.when(qstatExternalProcess1.stdout("Job_Name", "Output_Path"))
            .thenReturn(List.of("    Job_Name = dv-118-36426.0",
                "    Output_Path = draco.nas.nasa.gov:/non/existent/path"));
        RemoteJobInformation remoteJobInformation = pbsBatchManager.remoteJobInformation(remoteJob);
        assertNotNull(remoteJobInformation);
        assertEquals("dv-118-36426.0", remoteJobInformation.getJobName());
        assertEquals(1234567L, remoteJobInformation.getJobId());
        assertEquals("/non/existent/path", remoteJobInformation.getLogFile());
        assertEquals("hecc", remoteJobInformation.getRemoteEnvironmentName());
        assertEquals(0, remoteJobInformation.getBatchSubmissionExitCode());
    }

    @Test
    public void testRemoteJobInformationJobLongGone() {
        RemoteJob remoteJob = new RemoteJob();
        remoteJob.setFinished(false);
        remoteJob.setJobId(1234567L);
        remoteJob.setRemoteEnvironmentName("hecc");
        Mockito.doReturn(qstatExternalProcess1)
            .when(pbsBatchManager)
            .qstatExternalProcess("-xf 1234567");
        Mockito.when(qstatExternalProcess1.stdout("Job_Name", "Output_Path"))
            .thenReturn(new ArrayList<>());
        RemoteJobInformation remoteJobInformation = pbsBatchManager.remoteJobInformation(remoteJob);
        assertNull(remoteJobInformation);
    }

    @Test
    public void testDeleteJobs() {
        Mockito.doReturn(qdelExternalProcess)
            .when(pbsBatchManager)
            .qdelExternalProcess(ArgumentMatchers.anyString());
        Mockito.doReturn(Map.of("1-2-pa.0", 1234567L, "1-2-pa.1", 1234568L, "1-2-pa.2", 1234587L))
            .when(pbsBatchManager)
            .jobIdByName(pipelineTask);
        pbsBatchManager.deleteJobs(pipelineTask);
        Mockito.verify(pbsBatchManager).qdelExternalProcess("1234567 1234568 1234587");
    }

    @Test
    public void testUpdateCostEstimate() {
        RemoteJob remoteJob = new RemoteJob(1234567L, "hecc", 0.47);
        Mockito.doReturn(qstatExternalProcess1)
            .when(pbsBatchManager)
            .qstatExternalProcess("-xf 1234567");
        Mockito.when(qstatExternalProcess1.stdout("resources_used.walltime"))
            .thenReturn(List.of("    resources_used.walltime = 14:15:00"));
        double estimate = pbsBatchManager.getUpdatedCostEstimate(remoteJob);
        assertEquals(6.6975, estimate, 1e-5);
        remoteJob.setCostEstimate(estimate);
        Mockito.when(qstatExternalProcess1.stdout("resources_used.walltime"))
            .thenReturn(new ArrayList<>());
        assertEquals(6.6975, pbsBatchManager.getUpdatedCostEstimate(remoteJob), 1e-5);
        Mockito.when(qstatExternalProcess1.stdout("resources_used.walltime"))
            .thenReturn(List.of("Something that doesn't match the regular expression"));
        assertEquals(6.6975, pbsBatchManager.getUpdatedCostEstimate(remoteJob), 1e-5);
    }
}
