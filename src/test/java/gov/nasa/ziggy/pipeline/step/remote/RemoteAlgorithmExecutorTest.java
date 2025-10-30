package gov.nasa.ziggy.pipeline.step.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.remote.batch.SupportedBatchSystem;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.messages.MonitorAlgorithmRequest;
import gov.nasa.ziggy.util.PipelineException;

/** Unit tests for {@link RemoteAlgorithmExecutor}. */
public class RemoteAlgorithmExecutorTest {

    private PipelineTaskOperations pipelineTaskOperations = Mockito
        .mock(PipelineTaskOperations.class);
    private PipelineTaskDataOperations pipelineTaskDataOperations = Mockito
        .mock(PipelineTaskDataOperations.class);
    private BatchParameters batchParameters = Mockito.mock(BatchParameters.class);
    private BatchManager<?> batchManager = Mockito.mock(BatchManager.class);
    private int subtaskCount;
    private PipelineTask pipelineTask = Mockito.mock(PipelineTask.class);
    private RemoteAlgorithmExecutorForTest remoteAlgorithmExecutor;
    private AlertService alertService = Mockito.mock(AlertService.class);
    private PipelineNodeExecutionResources executionResources = Mockito
        .mock(PipelineNodeExecutionResources.class);
    private RemoteEnvironment remoteEnvironment = Mockito.mock(RemoteEnvironment.class);
    private SubtaskCounts subtaskCounts = Mockito.mock(SubtaskCounts.class);
    private RemoteJob completeRemoteJob;
    private RemoteJob incompleteRemoteJob;
    private RemoteJobInformation incompleteRemoteJobInformation;

    @Before
    public void setUp() {
        Mockito.when(pipelineTaskOperations.executionResources(pipelineTask))
            .thenReturn(executionResources);
        Mockito.when(executionResources.getRemoteEnvironment()).thenReturn(remoteEnvironment);
        Mockito.when(remoteEnvironment.getBatchSystem()).thenReturn(SupportedBatchSystem.PBS);
        subtaskCount = 100;
        Mockito.when(pipelineTaskDataOperations.subtaskCounts(pipelineTask))
            .thenReturn(subtaskCounts);
        Mockito.when(subtaskCounts.getTotalSubtaskCount()).thenReturn(105);
        Mockito.when(subtaskCounts.getCompletedSubtaskCount()).thenReturn(5);
        remoteAlgorithmExecutor = new RemoteAlgorithmExecutorForTest(pipelineTask);
        completeRemoteJob = new RemoteJob();
        completeRemoteJob.setFinished(true);
        completeRemoteJob.setJobId(1234567L);
        incompleteRemoteJob = new RemoteJob();
        incompleteRemoteJob.setFinished(false);
        incompleteRemoteJob.setJobId(1234568L);
        incompleteRemoteJobInformation = new RemoteJobInformation("test1", "test2", "hecc");
        incompleteRemoteJobInformation.setJobId(1234568L);
    }

    @Test
    public void testSubmitForExecution() {
        RemoteJobInformation remoteJobInformation1 = new RemoteJobInformation("dummy", "1-2-tps.0",
            "hecc");
        RemoteJobInformation remoteJobInformation2 = new RemoteJobInformation("dummy", "1-2-tps.1",
            "hecc");
        RemoteJobInformation remoteJobInformation3 = new RemoteJobInformation("dummy", "1-2-tps.2",
            "hecc");
        remoteJobInformation1.setBatchSubmissionExitCode(0);
        remoteJobInformation2.setBatchSubmissionExitCode(1);
        remoteJobInformation3.setBatchSubmissionExitCode(0);
        Mockito.when(batchManager.submitJobs(pipelineTask, subtaskCount))
            .thenReturn(
                List.of(remoteJobInformation1, remoteJobInformation2, remoteJobInformation3));
        Mockito.when(batchManager.jobIdByName(pipelineTask))
            .thenReturn(Map.of("1-2-tps.0", 1L, "1-2-tps.2", 2L));

        // Perform the submission.
        remoteAlgorithmExecutor.submitForExecution();

        // Verify that an alert was issued.
        Mockito.verify(alertService)
            .generateAndBroadcastAlert("PI (Remote)", pipelineTask, Severity.WARNING,
                "Attempted to submit " + 3 + " jobs but only successfully submitted " + 2);

        // Verify that a MonitorAlgorithmRequest was sent with the appropriate content.
        assertNotNull(remoteAlgorithmExecutor.monitorAlgorithmRequest);
        Map<String, RemoteJobInformation> remoteJobInformationByName = new HashMap<>();
        List<RemoteJobInformation> remoteJobsInformation = remoteAlgorithmExecutor.monitorAlgorithmRequest
            .getRemoteJobsInformation();
        assertEquals(2, remoteJobsInformation.size());
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            remoteJobInformationByName.put(remoteJobInformation.getJobName(), remoteJobInformation);
        }
        assertEquals(1L, remoteJobInformationByName.get("1-2-tps.0").getJobId());
        assertEquals(2L, remoteJobInformationByName.get("1-2-tps.2").getJobId());
    }

    @Test(expected = PipelineException.class)
    public void testSubmitAllSubmissionsFail() {
        Mockito.when(batchManager.submitJobs(pipelineTask, subtaskCount))
            .thenReturn(new ArrayList<>());
        remoteAlgorithmExecutor.submitForExecution();
    }

    @Test
    public void testResumeMonitoringNoRemoteJobs() {
        Mockito.when(pipelineTaskDataOperations.remoteJobs(pipelineTask))
            .thenReturn(new HashSet<>());
        assertFalse(remoteAlgorithmExecutor.resumeMonitoring());
        assertTrue(CollectionUtils.isEmpty(remoteAlgorithmExecutor.getRemoteJobsInformation()));
        assertTrue(
            CollectionUtils.isEmpty(remoteAlgorithmExecutor.getRemoteJobsMarkedAsFinished()));
    }

    @Test
    public void testResumeMonitoringNoIncompleteRemoteJobs() {
        Mockito.when(pipelineTaskDataOperations.remoteJobs(pipelineTask))
            .thenReturn(Set.of(completeRemoteJob));
        assertFalse(remoteAlgorithmExecutor.resumeMonitoring());
        assertTrue(CollectionUtils.isEmpty(remoteAlgorithmExecutor.getRemoteJobsInformation()));
        assertTrue(
            CollectionUtils.isEmpty(remoteAlgorithmExecutor.getRemoteJobsMarkedAsFinished()));
    }

    @Test
    public void testResumeMonitoringJobFinished() {
        Mockito.when(pipelineTaskDataOperations.remoteJobs(pipelineTask))
            .thenReturn(Set.of(incompleteRemoteJob));
        Mockito.when(batchManager.isFinished(incompleteRemoteJob)).thenReturn(true);
        assertFalse(remoteAlgorithmExecutor.resumeMonitoring());
        assertTrue(CollectionUtils.isEmpty(remoteAlgorithmExecutor.getRemoteJobsInformation()));
        assertEquals(incompleteRemoteJob,
            remoteAlgorithmExecutor.getRemoteJobsMarkedAsFinished().get(0));
        assertEquals(1, remoteAlgorithmExecutor.getRemoteJobsMarkedAsFinished().size());
    }

    @Test
    public void testResumeMonitoringJobLongGone() {
        Mockito.when(pipelineTaskDataOperations.remoteJobs(pipelineTask))
            .thenReturn(Set.of(incompleteRemoteJob));
        Mockito.when(batchManager.isFinished(incompleteRemoteJob)).thenReturn(false);
        Mockito.when(batchManager.remoteJobInformation(incompleteRemoteJob)).thenReturn(null);
        assertFalse(remoteAlgorithmExecutor.resumeMonitoring());
        assertTrue(CollectionUtils.isEmpty(remoteAlgorithmExecutor.getRemoteJobsInformation()));
        assertEquals(incompleteRemoteJob,
            remoteAlgorithmExecutor.getRemoteJobsMarkedAsFinished().get(0));
        assertEquals(1, remoteAlgorithmExecutor.getRemoteJobsMarkedAsFinished().size());
    }

    @Test
    public void testResumeMonitoring() {
        Mockito.when(pipelineTaskDataOperations.remoteJobs(pipelineTask))
            .thenReturn(Set.of(completeRemoteJob, incompleteRemoteJob));
        Mockito.when(batchManager.remoteJobInformation(incompleteRemoteJob))
            .thenReturn(incompleteRemoteJobInformation);
        assertTrue(remoteAlgorithmExecutor.resumeMonitoring());
        assertFalse(CollectionUtils.isEmpty(remoteAlgorithmExecutor.getRemoteJobsInformation()));
        RemoteJobInformation remoteJobInformation = remoteAlgorithmExecutor
            .getRemoteJobsInformation()
            .get(0);
        assertEquals("test1", remoteJobInformation.getLogFile());
        assertEquals("test2", remoteJobInformation.getJobName());
        assertEquals(1234568L, remoteJobInformation.getJobId());
        assertTrue(
            CollectionUtils.isEmpty(remoteAlgorithmExecutor.getRemoteJobsMarkedAsFinished()));
    }

    /** Subclass of {@link RemoteAlgorithmExecutor} used in testing. */
    private class RemoteAlgorithmExecutorForTest extends RemoteAlgorithmExecutor {

        private MonitorAlgorithmRequest monitorAlgorithmRequest;
        private List<RemoteJob> remoteJobsMarkedAsFinished = new ArrayList<>();

        public RemoteAlgorithmExecutorForTest(PipelineTask pipelineTask) {
            super(pipelineTask);
        }

        public List<RemoteJob> getRemoteJobsMarkedAsFinished() {
            return remoteJobsMarkedAsFinished;
        }

        @Override
        protected void addToMonitor() {
            monitorAlgorithmRequest = monitorAlgorithmRequest();
        }

        @Override
        protected PipelineTaskOperations pipelineTaskOperations() {
            return pipelineTaskOperations;
        }

        @Override
        protected PipelineTaskDataOperations pipelineTaskDataOperations() {
            return pipelineTaskDataOperations;
        }

        @Override
        protected BatchParameters batchParameters() {
            return batchParameters;
        }

        @Override
        protected BatchManager<?> batchManager() {
            return batchManager;
        }

        @Override
        protected int subtaskCount() {
            return subtaskCount;
        }

        @Override
        protected AlertService alertService() {
            return alertService;
        }

        @Override
        protected Path workingDir() {
            return Paths.get("/path/to/task/dir");
        }

        @Override
        protected void markRemoteJobFinished(RemoteJob remoteJob) {
            remoteJobsMarkedAsFinished.add(remoteJob);
        }
    }
}
