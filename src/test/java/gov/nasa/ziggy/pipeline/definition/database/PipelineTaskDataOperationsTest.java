package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Units;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskCountsTest;
import gov.nasa.ziggy.pipeline.definition.TaskExecutionLog;
import gov.nasa.ziggy.pipeline.step.remote.BatchManager;
import gov.nasa.ziggy.pipeline.step.remote.RemoteJobInformation;

public class PipelineTaskDataOperationsTest {

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations;
    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations;
    private PipelineTaskOperationsTest pipelineTaskOperationsTest;
    private PipelineOperationsTestUtils pipelineOperationsTestUtils;

    @Before
    public void setUp() {
        pipelineTaskOperationsTest = new PipelineTaskOperationsTest();
        pipelineTaskOperationsTest.setUp();
        pipelineTaskDataOperations = Mockito.spy(PipelineTaskDataOperations.class);
        pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();

        pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
    }

    @Test
    public void testProcessingStep() {
        assertEquals(ProcessingStep.WAITING_TO_RUN, pipelineTaskDataOperations
            .processingStep(pipelineOperationsTestUtils.getPipelineTasks().get(0)));
    }

    @Test
    public void testPipelineTasks() {
        assertEquals(pipelineOperationsTestUtils.getPipelineTasks(),
            pipelineTaskDataOperations.pipelineTasks(pipelineOperationsTestUtils.pipelineInstance(),
                Set.of(ProcessingStep.WAITING_TO_RUN)));
        assertEquals(List.of(), pipelineTaskDataOperations.pipelineTasks(
            pipelineOperationsTestUtils.pipelineInstance(), Set.of(ProcessingStep.EXECUTING)));
    }

    @Test
    public void testUpdateProcessingStep() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.EXECUTING);
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        assertTrue(pipelineTaskDataOperations.executionClock(pipelineTask).isRunning());
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.COMPLETE);
        assertEquals(ProcessingStep.COMPLETE,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        assertFalse(pipelineTaskDataOperations.executionClock(pipelineTask).isRunning());
    }

    @Test
    public void testError() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations.taskErrored(pipelineTask);
        assertTrue(pipelineTaskDataOperations.hasErrored(pipelineTask));
        assertEquals(List.of(pipelineTask), pipelineTaskDataOperations
            .erroredPipelineTasks(pipelineOperationsTestUtils.pipelineInstance()));
        pipelineTaskDataOperations.clearError(pipelineTask);
        assertFalse(pipelineTaskDataOperations.hasErrored(pipelineTask));
        pipelineTaskDataOperations.taskErrored(pipelineTask);
        assertTrue(pipelineTaskDataOperations.hasErrored(pipelineTask));
        pipelineTaskDataOperations.setError(pipelineTask, false);
        assertFalse(pipelineTaskDataOperations.hasErrored(pipelineTask));
    }

    @Test
    public void testHaltRequested() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        assertFalse(pipelineTaskDataOperations.haltRequested(pipelineTask));
        pipelineTaskDataOperations.setHaltRequested(pipelineTask, true);
        assertTrue(pipelineTaskDataOperations.haltRequested(pipelineTask));
        pipelineTaskDataOperations.setHaltRequested(pipelineTask, false);
        assertFalse(pipelineTaskDataOperations.haltRequested(pipelineTask));
    }

    @Test
    public void testSetWorkerInfo() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations.updateWorkerInfo(pipelineTask, "host", 42);

        assertEquals("host:42",
            pipelineTaskDisplayDataOperations.pipelineTaskDisplayData(pipelineTask)
                .getWorkerName());
    }

    @Test
    public void testDistinctSoftwareRevisions() {
        assertEquals(List.of("ziggy software revision 1", "pipeline software revision 1"),
            pipelineTaskDataOperations
                .distinctSoftwareRevisions(pipelineOperationsTestUtils.pipelineInstance()));
        assertEquals(List.of("ziggy software revision 1", "pipeline software revision 1"),
            pipelineTaskDataOperations
                .distinctSoftwareRevisions(pipelineOperationsTestUtils.pipelineInstanceNode()));
    }

    @Test
    public void testPipelineSoftwareRevision() {
        assertEquals("pipeline software revision 1", pipelineTaskDataOperations
            .pipelineSoftwareRevision(pipelineOperationsTestUtils.getPipelineTasks().get(0)));
        assertEquals("pipeline software revision 1", pipelineTaskDataOperations
            .pipelineSoftwareRevision(pipelineOperationsTestUtils.getPipelineTasks().get(1)));
    }

    @Test
    public void testPrepareTasksForManualResubmit() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations
            .prepareTasksForManualResubmit(pipelineOperationsTestUtils.getPipelineTasks());
        assertEquals(0, pipelineTaskDataOperations.autoResubmitCount(pipelineTask));
        assertTrue(pipelineTaskDataOperations.retrying(pipelineTask));
    }

    @Test
    public void testPrepareTaskForAutoResubmit() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations.prepareTaskForAutoResubmit(pipelineTask);
        assertEquals(1, pipelineTaskDataOperations.autoResubmitCount(pipelineTask));
        assertTrue(pipelineTaskDataOperations.hasErrored(pipelineTask));
    }

    @Test
    public void testPrepareTaskForRestart() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.EXECUTING);
        pipelineTaskDataOperations.taskErrored(pipelineTask);
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        assertTrue(pipelineTaskDataOperations.hasErrored(pipelineTask));

        pipelineTaskDataOperations.prepareTaskForRestart(pipelineTask);
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        assertFalse(pipelineTaskDataOperations.hasErrored(pipelineTask));
    }

    @Test
    public void testSubtaskCounts() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations.updateSubtaskCounts(pipelineTask, 1, 2, 3);
        TaskCountsTest.testSubtaskCounts(1, 2, 3,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));

        pipelineTaskDataOperations.updateSubtaskCounts(pipelineTask, -1, -2, -3);
        TaskCountsTest.testSubtaskCounts(1, 2, 3,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
    }

    @Test
    public void testIncrementFailureCount() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        assertEquals(0, pipelineTaskDisplayDataOperations.pipelineTaskDisplayData(pipelineTask)
            .getFailureCount());
        pipelineTaskDataOperations.incrementFailureCount(pipelineTask);
        assertEquals(1, pipelineTaskDisplayDataOperations.pipelineTaskDisplayData(pipelineTask)
            .getFailureCount());
    }

    @Test
    public void testIncrementTaskLogIndex() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        assertEquals("1-1-step1.42-0.log",
            pipelineTaskDataOperations.logFilename(pipelineTask, 42));
        pipelineTaskDataOperations.incrementTaskLogIndex(pipelineTask);
        pipelineTaskDataOperations.incrementTaskLogIndex(pipelineTask);
        assertEquals("1-1-step1.42-2.log",
            pipelineTaskDataOperations.logFilename(pipelineTask, 42));
        assertEquals("1-1-step1.42-24.log",
            pipelineTaskDataOperations.logFilename(pipelineTask, 42, 24));
    }

    @Test
    public void testPipelineTaskMetrics() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);

        List<PipelineTaskMetric> existingPipelineTaskMetrics = pipelineTaskDataOperations
            .pipelineTaskMetrics(pipelineTask);
        assertEquals(1, existingPipelineTaskMetrics.size());
        assertEquals(42, existingPipelineTaskMetrics.get(0).getValue());

        Map<PipelineTask, List<PipelineTaskMetric>> taskMetricsByTask = createPipelineTaskMetrics(
            List.of(pipelineTask));

        existingPipelineTaskMetrics = pipelineTaskDataOperations.pipelineTaskMetrics(pipelineTask);
        assertEquals(3, existingPipelineTaskMetrics.size());
        Map<String, PipelineTaskMetric> metricByCategory = existingPipelineTaskMetrics.stream()
            .collect(Collectors.toMap(PipelineTaskMetric::getCategory, Function.identity()));
        for (PipelineTaskMetric pipelineTaskMetric : taskMetricsByTask.get(pipelineTask)) {
            assertEquals(pipelineTaskMetric,
                metricByCategory.get(pipelineTaskMetric.getCategory()));
        }

        PipelineTaskMetric pipelineTaskMetric = new PipelineTaskMetric("metric4", 400, Units.RATE);
        existingPipelineTaskMetrics.add(pipelineTaskMetric);
        pipelineTaskDataOperations.updatePipelineTaskMetrics(pipelineTask,
            existingPipelineTaskMetrics);
        existingPipelineTaskMetrics = pipelineTaskDataOperations.pipelineTaskMetrics(pipelineTask);
        assertEquals(4, existingPipelineTaskMetrics.size());
        assertTrue(existingPipelineTaskMetrics.contains(pipelineTaskMetric));

        pipelineTaskMetric = existingPipelineTaskMetrics.get(0);
        existingPipelineTaskMetrics.remove(pipelineTaskMetric);
        pipelineTaskDataOperations.updatePipelineTaskMetrics(pipelineTask,
            existingPipelineTaskMetrics);
        existingPipelineTaskMetrics = pipelineTaskDataOperations.pipelineTaskMetrics(pipelineTask);
        assertEquals(3, existingPipelineTaskMetrics.size());
        assertFalse(existingPipelineTaskMetrics.contains(pipelineTaskMetric));
    }

    @Test
    public void testTaskMetricsByTask() {
        Map<PipelineTask, List<PipelineTaskMetric>> expectedTaskMetricsByTask = createPipelineTaskMetrics(
            pipelineOperationsTestUtils.getPipelineTasks());

        Map<PipelineTask, List<PipelineTaskMetric>> taskMetricsByTask = pipelineTaskDataOperations
            .taskMetricsByTask(pipelineOperationsTestUtils.pipelineInstanceNode());
        assertEquals(expectedTaskMetricsByTask, taskMetricsByTask);
    }

    private Map<PipelineTask, List<PipelineTaskMetric>> createPipelineTaskMetrics(
        List<PipelineTask> pipelineTasks) {
        Map<PipelineTask, List<PipelineTaskMetric>> taskMetricsByTask = new HashMap<>();

        int pipelineTaskIndex = 100;
        for (PipelineTask pipelineTask : pipelineTasks) {
            List<PipelineTaskMetric> pipelineTaskMetrics = new ArrayList<>();
            pipelineTaskMetrics
                .add(new PipelineTaskMetric("metric1", pipelineTaskIndex + 1, Units.TIME));
            pipelineTaskMetrics
                .add(new PipelineTaskMetric("metric2", pipelineTaskIndex + 2, Units.BYTES));
            pipelineTaskMetrics
                .add(new PipelineTaskMetric("metric3", pipelineTaskIndex + 3, Units.RATE));
            taskMetricsByTask.put(pipelineTask, pipelineTaskMetrics);
            pipelineTaskDataOperations.updatePipelineTaskMetrics(pipelineTask, pipelineTaskMetrics);
            pipelineTaskIndex += 100;
        }
        return taskMetricsByTask;
    }

    @Test
    public void testTaskExecutionLogs() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);
        List<TaskExecutionLog> taskExecutionLogs = pipelineTaskDataOperations
            .taskExecutionLogs(pipelineTask);
        assertEquals(0, taskExecutionLogs.size());

        pipelineTaskDataOperations.addTaskExecutionLog(pipelineTask, "dummy1", 1, 0L);
        pipelineTaskDataOperations.addTaskExecutionLog(pipelineTask, "dummy2", 2, 0L);
        pipelineTaskDataOperations.addTaskExecutionLog(pipelineTask, "dummy3", 3, 0L);
        taskExecutionLogs = pipelineTaskDataOperations.taskExecutionLogs(pipelineTask);
        assertEquals(3, taskExecutionLogs.size());

        Map<String, TaskExecutionLog> logsByWorkerHost = new HashMap<>();
        for (TaskExecutionLog taskExecutionLog : taskExecutionLogs) {
            logsByWorkerHost.put(taskExecutionLog.getWorkerHost(), taskExecutionLog);
        }
        testTaskExecutionLog(logsByWorkerHost.get("dummy1"), 1);
        testTaskExecutionLog(logsByWorkerHost.get("dummy2"), 2);
        testTaskExecutionLog(logsByWorkerHost.get("dummy3"), 3);
    }

    private void testTaskExecutionLog(TaskExecutionLog taskExecutionLog, int workerThread) {
        assertNotNull(taskExecutionLog);
        assertEquals(workerThread, taskExecutionLog.getWorkerThread());
    }

    @Test
    public void testEmptyRemoteJobs() {
        PipelineTask pipelineTask = spy(new PipelineTask(null, new PipelineInstanceNode(), null));
        doReturn(42L).when(pipelineTask).getId();

        Set<RemoteJob> remoteJobs = pipelineTaskDataOperations.remoteJobs(pipelineTask);
        assertEquals(0, remoteJobs.size());
    }

    @Test
    public void testRemoteJobs() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);

        Set<RemoteJob> expectedJobs = new HashSet<>(
            Set.of(new RemoteJob(3), new RemoteJob(2), new RemoteJob(1)));
        pipelineTaskDataOperations.updateRemoteJobs(pipelineTask, expectedJobs);
        Set<RemoteJob> remoteJobs = pipelineTaskDataOperations.remoteJobs(pipelineTask);
        assertEquals(3, remoteJobs.size());
        assertEquals(expectedJobs, remoteJobs);

        PipelineTaskDisplayData pipelineTaskDisplayData = pipelineTaskDisplayDataOperations
            .pipelineTaskDisplayData(pipelineTask);
        assertEquals(3, pipelineTaskDisplayData.getRemoteJobs().size());
        assertEquals(expectedJobs, pipelineTaskDisplayData.getRemoteJobs());
    }

    @Test
    public void testRemoteJobsSort() {
        PipelineTask pipelineTask = pipelineOperationsTestUtils.getPipelineTasks().get(0);

        Set<RemoteJob> expectedJobs = new HashSet<>(
            Set.of(new RemoteJob(3), new RemoteJob(2), new RemoteJob(1)));
        pipelineTaskDataOperations.updateRemoteJobs(pipelineTask, expectedJobs);
        List<RemoteJob> remoteJobList = new ArrayList<>(
            pipelineTaskDataOperations.remoteJobs(pipelineTask));
        assertEquals(new RemoteJob(1), remoteJobList.get(0));
        assertEquals(new RemoteJob(2), remoteJobList.get(1));
        assertEquals(new RemoteJob(3), remoteJobList.get(2));
    }

    @Test
    public void testAddRemoteJobs() {

        PipelineTask task = pipelineTaskOperationsTest.createPipelineTask();
        List<RemoteJobInformation> remoteJobsInformation = remoteJobsInformation();
        pipelineTaskDataOperations.addRemoteJobs(task, remoteJobsInformation);

        Set<RemoteJob> remoteJobs = pipelineTaskDataOperations.remoteJobs(task);
        assertEquals(3, remoteJobs.size());
        assertTrue(remoteJobs.contains(new RemoteJob(9101154)));
        assertTrue(remoteJobs.contains(new RemoteJob(9102337)));
        assertTrue(remoteJobs.contains(new RemoteJob(6020203)));
        for (RemoteJob job : remoteJobs) {
            assertFalse(job.isFinished());
            assertEquals(0, job.getCostEstimate(), 1e-9);
        }
    }

    @Test
    public void testUpdateJobs() {

        // Create the task with 3 remote jobs
        PipelineTask task = pipelineTaskOperationsTest.createPipelineTask();
        List<RemoteJobInformation> remoteJobsInformation = remoteJobsInformation();
        pipelineTaskDataOperations.addRemoteJobs(task, remoteJobsInformation);

        mockRemoteJobUpdates();

        pipelineTaskDataOperations.updateJobs(task);

        // Check for the expected results
        List<RemoteJob> remoteJobs = new ArrayList<>(pipelineTaskDataOperations.remoteJobs(task));
        RemoteJob job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9101154)));
        assertTrue(job.isFinished());
        assertEquals(10.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9102337)));
        assertFalse(job.isFinished());
        assertEquals(4.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(6020203)));
        assertFalse(job.isFinished());
        assertEquals(0, job.getCostEstimate(), 1e-9);

        // Make sure that the database was also updated
        Set<RemoteJob> databaseRemoteJobs = pipelineTaskDataOperations.remoteJobs(task);
        for (RemoteJob remoteJob : databaseRemoteJobs) {
            RemoteJob otherJob = remoteJobs.get(remoteJobs.indexOf(remoteJob));
            assertEquals(otherJob.isFinished(), remoteJob.isFinished());
            assertEquals(otherJob.getCostEstimate(), remoteJob.getCostEstimate(), 1e-9);
        }
    }

    @Test
    public void testUpdateJobsForPipelineInstance() {

        // Create the task with 3 remote jobs
        PipelineTask task = pipelineTaskOperationsTest.createPipelineTask();
        List<RemoteJobInformation> remoteJobsInformation = remoteJobsInformation();
        pipelineTaskDataOperations.addRemoteJobs(task, remoteJobsInformation);

        mockRemoteJobUpdates();

        pipelineTaskDataOperations.updateJobs(pipelineTaskOperations.pipelineInstance(task));

        // Check for the expected results
        List<RemoteJob> remoteJobs = new ArrayList<>(pipelineTaskDataOperations.remoteJobs(task));
        RemoteJob job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9101154)));
        assertTrue(job.isFinished());
        assertEquals(10.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9102337)));
        assertFalse(job.isFinished());
        assertEquals(4.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(6020203)));
        assertFalse(job.isFinished());
        assertEquals(0, job.getCostEstimate(), 1e-9);
    }

    private List<RemoteJobInformation> remoteJobsInformation() {
        List<RemoteJobInformation> remoteJobsInformation = new ArrayList<>();
        RemoteJobInformation remoteJobInformation = new RemoteJobInformation("pbsLogfile",
            "1-1-tps.0", "hecc");
        remoteJobInformation.setJobId(9101154);
        remoteJobsInformation.add(remoteJobInformation);
        remoteJobInformation = new RemoteJobInformation("pbsLogfile", "1-1-tps.1", "hecc");
        remoteJobInformation.setJobId(9102337);
        remoteJobsInformation.add(remoteJobInformation);
        remoteJobInformation = new RemoteJobInformation("pbsLogfile", "1-1-tps.2", "hecc");
        remoteJobInformation.setJobId(6020203);
        remoteJobsInformation.add(remoteJobInformation);
        return remoteJobsInformation;
    }

    private void mockRemoteJobUpdates() {
        BatchManager<?> batchManager = Mockito.mock(BatchManager.class);
        Mockito.doReturn(batchManager)
            .when(pipelineTaskDataOperations)
            .batchManager(ArgumentMatchers.any(RemoteJob.class));
        RemoteJob job9101154 = new RemoteJob(9101154, "hecc", 1.0);
        RemoteJob job9102337 = new RemoteJob(9102337, "hecc", 0.8);
        RemoteJob job6020203 = new RemoteJob(6020203, "hecc", 0.8);

        Mockito.when(batchManager.isFinished(job9101154)).thenReturn(true);
        Mockito.when(batchManager.isFinished(job9102337)).thenReturn(false);
        Mockito.when(batchManager.isFinished(job6020203)).thenReturn(false);

        Mockito.when(batchManager.getUpdatedCostEstimate(job9101154)).thenReturn(10.0);
        Mockito.when(batchManager.getUpdatedCostEstimate(job9102337)).thenReturn(4.0);
        Mockito.when(batchManager.getUpdatedCostEstimate(job6020203)).thenReturn(0.0);
    }
}
