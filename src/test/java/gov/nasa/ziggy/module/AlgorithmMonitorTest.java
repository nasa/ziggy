package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.AlgorithmMonitor.Disposition;
import gov.nasa.ziggy.module.remote.PbsLogParser;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.module.remote.RemoteJobInformation;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.messages.MonitorAlgorithmRequest;
import gov.nasa.ziggy.services.messages.TaskProcessingCompleteMessage;
import gov.nasa.ziggy.supervisor.TaskRequestHandlerLifecycleManager;
import gov.nasa.ziggy.supervisor.TaskRequestHandlerLifecycleManagerTest.InstrumentedTaskRequestHandlerLifecycleManager;

/**
 * Unit tests for {@link AlgorithmMonitor}.
 *
 * @author PT
 * @author Bill Wohler
 */
public class AlgorithmMonitorTest {

    private AlgorithmMonitorForTest monitor;
    private PipelineTask pipelineTask100;
    private PipelineTask pipelineTask101;
    private QueueCommandManager queueCommandManager;
    private PbsLogParser pbsLogParser;
    private PipelineExecutor pipelineExecutor;
    private PipelineTaskOperations pipelineTaskOperations;
    private PipelineTaskDataOperations pipelineTaskDataOperations;
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations;
    private AlertService alertService;
    private PipelineDefinitionNodeExecutionResources resources;
    private List<RemoteJobInformation> remoteJobsInformation;
    private RemoteJobInformation job0Information;
    private RemoteJobInformation job1Information;
    private Path taskDirectoryTask100;
    private Path taskDirectoryTask101;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
    public TaskRequestHandlerLifecycleManager lifecycleManager = new InstrumentedTaskRequestHandlerLifecycleManager();

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(resultsDirPropertyRule);

    @Before
    public void setUp() throws IOException, ConfigurationException {

        monitor = spy(new AlgorithmMonitorForTest());
        queueCommandManager = mock(QueueCommandManager.class);
        pbsLogParser = mock(PbsLogParser.class);
        resources = new PipelineDefinitionNodeExecutionResources("dummy", "dummy");
        remoteJobsInformation = new ArrayList<>();

        doReturn(queueCommandManager).when(monitor).queueCommandManager();
        doReturn(0L).when(monitor).finishedJobsPollingIntervalMillis();
        doReturn(0L).when(monitor).localPollIntervalMillis();
        doReturn(0L).when(monitor).remotePollIntervalMillis();
        doReturn(false).when(monitor).taskIsKilled(ArgumentMatchers.isA(PipelineTask.class));
        pipelineTask100 = mock(PipelineTask.class);
        doReturn(50L).when(pipelineTask100).getPipelineInstanceId();
        doReturn(50L).when(pipelineTask100).getPipelineInstanceId();
        doReturn(100L).when(pipelineTask100).getId();
        doReturn("dummy").when(pipelineTask100).getModuleName();
        pipelineTask101 = mock(PipelineTask.class);
        doReturn(101L).when(pipelineTask101).getId();
        pipelineExecutor = mock(PipelineExecutor.class);
        pipelineTaskOperations = mock(PipelineTaskOperations.class);
        when(pipelineTaskOperations.pipelineTask(100L)).thenReturn(pipelineTask100);
        when(pipelineTaskOperations.merge(ArgumentMatchers.isA(PipelineTask.class)))
            .thenReturn(pipelineTask100);
        pipelineTaskDataOperations = mock(PipelineTaskDataOperations.class);
        pipelineInstanceNodeOperations = mock(PipelineInstanceNodeOperations.class);
        doReturn(pipelineTaskOperations).when(pipelineExecutor).pipelineTaskOperations();
        doReturn(pipelineInstanceNodeOperations).when(pipelineExecutor)
            .pipelineInstanceNodeOperations();
        doReturn(pipelineTaskOperations).when(monitor).pipelineTaskOperations();
        doReturn(pipelineTaskDataOperations).when(monitor).pipelineTaskDataOperations();
        Mockito.doNothing()
            .when(pipelineExecutor)
            .removeTaskFromKilledTaskList(ArgumentMatchers.isA(PipelineTask.class));
        when(pipelineExecutor.taskRequestEnabled()).thenReturn(false);
        when(pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(resources);
        when(monitor.pipelineExecutor()).thenReturn(pipelineExecutor);
        when(monitor.pipelineTaskOperations()).thenReturn(pipelineTaskOperations);
        when(pipelineTaskOperations.pipelineTask(ArgumentMatchers.anyLong()))
            .thenReturn(pipelineTask100);
        alertService = mock(AlertService.class);
        when(monitor.alertService()).thenReturn(alertService);
        when(monitor.pbsLogParser()).thenReturn(pbsLogParser);
        taskDirectoryTask100 = Files
            .createDirectories(DirectoryProperties.taskDataDir().resolve("50-100-dummy"));
        Files.createDirectories(taskDirectoryTask100.resolve(SubtaskUtils.subtaskDirName(0)));
        job0Information = new RemoteJobInformation(
            DirectoryProperties.pbsLogDir().toAbsolutePath().resolve("pbsLogFile1").toString(),
            "50-100-dummy.0");
        job0Information.setJobId(1L);
        job1Information = new RemoteJobInformation(
            DirectoryProperties.pbsLogDir().toAbsolutePath().resolve("pbsLogFile2").toString(),
            "50-100-dummy.1");
        job1Information.setJobId(2L);
        remoteJobsInformation.add(job0Information);
        remoteJobsInformation.add(job1Information);
        taskDirectoryTask101 = Files
            .createDirectories(DirectoryProperties.taskDataDir().resolve("50-101-dummy"));
        Files.createDirectories(taskDirectoryTask101.resolve(SubtaskUtils.subtaskDirName(0)));
        Files.createDirectories(DirectoryProperties.pbsLogDir());
    }

    @Test
    public void testAddLocalTaskToMonitor() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask101);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        assertTrue(monitor.getJobsInformationByTask().isEmpty());
        Map<PipelineTask, TaskMonitor> taskMonitorByTaskId = monitor.getTaskMonitorByTask();
        assertNotNull(taskMonitorByTaskId.get(pipelineTask100));
        TaskMonitor taskMonitor = taskMonitorByTaskId.get(pipelineTask100);
        assertTrue(taskMonitor.getSubtaskDirectories()
            .contains(taskDirectoryTask101.resolve(SubtaskUtils.subtaskDirName(0))));
        assertEquals(1, taskMonitor.getSubtaskDirectories().size());
        assertEquals(taskDirectoryTask101, taskMonitor.getTaskDir());
        assertEquals(1, taskMonitorByTaskId.size());
        assertTrue(monitor.getJobsInformationByTask().isEmpty());
    }

    @Test
    public void testAddRemoteTaskToMonitor() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        Map<PipelineTask, TaskMonitor> taskMonitorByTaskId = monitor.getTaskMonitorByTask();
        assertNotNull(taskMonitorByTaskId.get(pipelineTask100));
        TaskMonitor taskMonitor = taskMonitorByTaskId.get(pipelineTask100);
        assertTrue(taskMonitor.getSubtaskDirectories()
            .contains(taskDirectoryTask100.resolve(SubtaskUtils.subtaskDirName(0))));
        assertEquals(1, taskMonitor.getSubtaskDirectories().size());
        assertEquals(taskDirectoryTask100, taskMonitor.getTaskDir());
        assertEquals(1, taskMonitorByTaskId.size());
        assertNotNull(monitor.getJobsInformationByTask().get(pipelineTask100));
        assertTrue(
            monitor.getJobsInformationByTask().get(pipelineTask100).contains(job0Information));
        assertTrue(
            monitor.getJobsInformationByTask().get(pipelineTask100).contains(job1Information));
        assertEquals(1, monitor.getJobsInformationByTask().size());
    }

    @Test
    public void testJobIdsByTaskId() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        Map<PipelineTask, List<Long>> jobIdsByTaskId = monitor
            .jobIdsByTaskId(List.of(pipelineTask101));
        assertNotNull(jobIdsByTaskId);
        assertTrue(jobIdsByTaskId.isEmpty());
        jobIdsByTaskId = monitor.jobIdsByTaskId(List.of(pipelineTask100));
        assertFalse(jobIdsByTaskId.isEmpty());
        assertNotNull(jobIdsByTaskId.get(pipelineTask100));
        assertTrue(jobIdsByTaskId.get(pipelineTask100).contains(1L));
        assertTrue(jobIdsByTaskId.get(pipelineTask100).contains(2L));
        assertEquals(2, jobIdsByTaskId.get(pipelineTask100).size());
        assertEquals(1, jobIdsByTaskId.size());
    }

    @Test
    public void testJobIdsByTaskIdOneJobFinished() throws IOException {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        Files.createFile(DirectoryProperties.pbsLogDir().resolve(job0Information.getLogFile()));
        Map<PipelineTask, List<Long>> jobIdsByTaskId = monitor
            .jobIdsByTaskId(List.of(pipelineTask100));
        assertFalse(jobIdsByTaskId.isEmpty());
        assertNotNull(jobIdsByTaskId.get(pipelineTask100));
        assertTrue(jobIdsByTaskId.get(pipelineTask100).contains(2L));
        assertEquals(1, jobIdsByTaskId.get(pipelineTask100).size());
        assertEquals(1, jobIdsByTaskId.size());
    }

    @Test
    public void testEndMonitoringNoSuchTask() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        monitor.endTaskMonitoring(pipelineTask101);
        assertNull(monitor.getDisposition());
        Mockito.verify(pipelineTaskDataOperations, Mockito.times(0))
            .updateProcessingStep(pipelineTask101, ProcessingStep.WAITING_TO_STORE);
        Mockito.verify(pipelineTaskDataOperations, Mockito.times(0))
            .updateJobs(pipelineTask100, true);
        Mockito.verify(queueCommandManager, Mockito.times(0))
            .deleteJobsByJobId(ArgumentMatchers.anyList());
        assertNotNull(monitor.getJobsInformationByTask().get(pipelineTask100));
        assertNotNull(monitor.getTaskMonitorByTask().get(pipelineTask100));
    }

    @Test
    public void testEndMonitoringLocalTaskSuccessful() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        monitorAlgorithmRequest = new MonitorAlgorithmRequest(pipelineTask101,
            taskDirectoryTask101);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        when(pipelineTaskDataOperations.subtaskCounts(pipelineTask101))
            .thenReturn(new SubtaskCounts(1, 1, 0));
        TaskProcessingCompleteMessage taskProcessingCompleteMessage = new TaskProcessingCompleteMessage(
            pipelineTask101);
        monitor.setTaskProcessingCompleteMessage(taskProcessingCompleteMessage);
        assertEquals(Disposition.PERSIST, monitor.getDisposition());
        Mockito.verify(pipelineExecutor).persistTaskResults(pipelineTask101);
        assertNotNull(monitor.getTaskMonitorByTask().get(pipelineTask100));
        assertNull(monitor.getTaskMonitorByTask().get(pipelineTask101));
        assertNotNull(monitor.getJobsInformationByTask().get(pipelineTask100));
        Mockito.verify(queueCommandManager, Mockito.times(0))
            .deleteJobsByJobId(ArgumentMatchers.anyList());
        Mockito.verify(pipelineTaskDataOperations, Mockito.times(0))
            .updateJobs(pipelineTask100, true);
    }

    @Test
    public void testEndMonitoringRemoteTaskSuccessful() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        monitorAlgorithmRequest = new MonitorAlgorithmRequest(pipelineTask101,
            taskDirectoryTask101);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        when(pipelineTaskDataOperations.subtaskCounts(pipelineTask100))
            .thenReturn(new SubtaskCounts(1, 1, 0));
        TaskProcessingCompleteMessage taskProcessingCompleteMessage = new TaskProcessingCompleteMessage(
            pipelineTask100);
        monitor.setTaskProcessingCompleteMessage(taskProcessingCompleteMessage);
        assertEquals(Disposition.PERSIST, monitor.getDisposition());
        Mockito.verify(pipelineExecutor).persistTaskResults(pipelineTask100);
        assertNull(monitor.getTaskMonitorByTask().get(pipelineTask100));
        assertNotNull(monitor.getTaskMonitorByTask().get(pipelineTask101));
        assertNull(monitor.getJobsInformationByTask().get(pipelineTask100));
        Mockito.verify(queueCommandManager).deleteJobsByJobId(ArgumentMatchers.anyList());
        Mockito.verify(pipelineTaskDataOperations).updateJobs(pipelineTask100, true);
    }

    @Test
    public void testDispositionHaltedTask() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        monitorAlgorithmRequest = new MonitorAlgorithmRequest(pipelineTask101,
            taskDirectoryTask101);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        when(pipelineTaskDataOperations.subtaskCounts(pipelineTask100))
            .thenReturn(new SubtaskCounts(1, 1, 0));
        doReturn(true).when(monitor).taskIsKilled(pipelineTask100);
        TaskProcessingCompleteMessage taskProcessingCompleteMessage = new TaskProcessingCompleteMessage(
            pipelineTask100);
        monitor.setTaskProcessingCompleteMessage(taskProcessingCompleteMessage);
        assertEquals(Disposition.FAIL, monitor.getDisposition());
    }

    @Test
    public void testDispositionTaskWithAcceptableErrors() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        monitorAlgorithmRequest = new MonitorAlgorithmRequest(pipelineTask101,
            taskDirectoryTask101);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        when(pipelineTaskDataOperations.subtaskCounts(pipelineTask100))
            .thenReturn(new SubtaskCounts(1, 0, 1));
        doReturn(false).when(monitor).taskIsKilled(pipelineTask100);
        resources.setMaxFailedSubtaskCount(1);
        TaskProcessingCompleteMessage taskProcessingCompleteMessage = new TaskProcessingCompleteMessage(
            pipelineTask100);
        monitor.setTaskProcessingCompleteMessage(taskProcessingCompleteMessage);
        assertEquals(Disposition.PERSIST, monitor.getDisposition());
    }

    @Test
    public void testResubmitDisposition() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        monitorAlgorithmRequest = new MonitorAlgorithmRequest(pipelineTask101,
            taskDirectoryTask101);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        when(pipelineTaskDataOperations.subtaskCounts(pipelineTask100))
            .thenReturn(new SubtaskCounts(1, 0, 1));
        when(pipelineTaskDataOperations.autoResubmitCount(pipelineTask100)).thenReturn(0);
        doReturn(false).when(monitor).taskIsKilled(pipelineTask100);
        resources.setMaxAutoResubmits(1);
        Mockito.doNothing()
            .when(pipelineExecutor)
            .restartFailedTasks(ArgumentMatchers.anyList(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.any());
        TaskProcessingCompleteMessage taskProcessingCompleteMessage = new TaskProcessingCompleteMessage(
            pipelineTask100);
        monitor.setTaskProcessingCompleteMessage(taskProcessingCompleteMessage);
        assertEquals(Disposition.RESUBMIT, monitor.getDisposition());
    }

    @Test
    public void testFailedDisposition() {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        monitorAlgorithmRequest = new MonitorAlgorithmRequest(pipelineTask101,
            taskDirectoryTask101);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        when(pipelineTaskDataOperations.subtaskCounts(pipelineTask100))
            .thenReturn(new SubtaskCounts(1, 0, 1));
        doReturn(false).when(monitor).taskIsKilled(pipelineTask100);
        TaskProcessingCompleteMessage taskProcessingCompleteMessage = new TaskProcessingCompleteMessage(
            pipelineTask100);
        monitor.setTaskProcessingCompleteMessage(taskProcessingCompleteMessage);
        assertEquals(Disposition.FAIL, monitor.getDisposition());
    }

    @Test
    public void testFinishedJobs() throws IOException {
        MonitorAlgorithmRequest monitorAlgorithmRequest = new MonitorAlgorithmRequest(
            pipelineTask100, taskDirectoryTask100, remoteJobsInformation);
        monitor.setMonitorAlgorithmRequest(monitorAlgorithmRequest);
        monitor.run();
        assertNull(monitor.allJobsFinishedMessage());
        Files.createFile(Paths.get(job0Information.getLogFile()));
        monitor.run();
        assertNull(monitor.allJobsFinishedMessage());
        Files.createFile(Paths.get(job1Information.getLogFile()));
        monitor.run();
        assertNotNull(monitor.allJobsFinishedMessage());
        // TODO Resurrect if AllJobsFinishedMessage.pipelineTask becomes non-transient
        // assertEquals(pipelineTask100, monitor.allJobsFinishedMessage().getPipelineTask());
    }

    private static class AlgorithmMonitorForTest extends AlgorithmMonitor {

        // Required for Mockito.
        @SuppressWarnings("unused")
        public AlgorithmMonitorForTest() {
        }

        private void setMonitorAlgorithmRequest(MonitorAlgorithmRequest request) {
            addToMonitor(request);
        }

        private void setTaskProcessingCompleteMessage(TaskProcessingCompleteMessage message) {
            endTaskMonitoring(message.getPipelineTask());
        }
    }
}
