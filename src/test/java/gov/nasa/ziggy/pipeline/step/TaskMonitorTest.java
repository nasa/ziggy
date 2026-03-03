package gov.nasa.ziggy.pipeline.step;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCountsTest;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.AlgorithmStateFiles.SubtaskStateCounts;
import gov.nasa.ziggy.services.alert.Alert;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertLog;
import gov.nasa.ziggy.services.alert.AlertLogOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.messages.AllJobsFinishedMessage;
import gov.nasa.ziggy.services.messages.HaltTasksRequest;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;

/**
 * Unit tests for {@link TestMonitor} class.
 *
 * @author PT
 * @author Bill Wohler
 */
public class TaskMonitorTest {

    private Path taskDir;
    private TaskMonitor taskMonitor;
    private PipelineTask pipelineTask;
    private List<AlgorithmStateFiles> algorithmStateFiles = new ArrayList<>();
    private AlgorithmStateFiles taskAlgorithmStateFiles;
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private PipelineNodeOperations pipelineNodeOperations = new PipelineNodeOperations();
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule pipelineResultsRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        directoryRule, "pipeline-results");

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule).around(pipelineResultsRule);

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() throws IOException, ConfigurationException {

        taskDir = DirectoryProperties.taskDataDir().resolve("10-20-nodename");
        Files.createDirectories(taskDir);
        taskAlgorithmStateFiles = new AlgorithmStateFiles(taskDir.toFile());
        // Create 6 subtask directories
        List<File> subtaskDirectories = new ArrayList<>();
        for (int subtask = 0; subtask < 6; subtask++) {
            Path subtaskDir = taskDir.resolve("st-" + Integer.toString(subtask));
            Files.createDirectories(subtaskDir);
            subtaskDirectories.add(subtaskDir.toFile());
            algorithmStateFiles.add(new AlgorithmStateFiles(subtaskDir.toFile()));
        }

        // Set up the database
        PipelineOperationsTestUtils testUtils = new PipelineOperationsTestUtils();
        testUtils.setUpSingleNodePipeline();
        pipelineTask = testUtils.getPipelineTasks().get(0);
        pipelineTaskDataOperations.updateSubtaskCounts(pipelineTask, 6, -1, -1);
        pipelineTaskDataOperations.updateProcessingStep(pipelineTask, ProcessingStep.QUEUED);

        // And, finally, the TaskMonitor itself.
        taskMonitor = new TaskMonitor(pipelineTask, taskDir.toFile(), 100L);
        taskMonitor = Mockito.spy(taskMonitor);
        Mockito.doReturn(0L).when(taskMonitor).fileSystemCheckIntervalMillis();
        Mockito.doReturn(0).when(taskMonitor).fileSystemChecksCount();
    }

    @Test
    public void testUpdate() {

        taskMonitor.update();
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());

        // Update the task state and see that the database gets updated.
        taskAlgorithmStateFiles.updateCurrentState(AlgorithmStateFiles.AlgorithmState.PROCESSING);
        taskMonitor.update();
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());

        // Update a subtask to PROCESSING, which should do nothing.
        algorithmStateFiles.get(0)
            .updateCurrentState(AlgorithmStateFiles.AlgorithmState.PROCESSING);
        taskMonitor.update();
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());

        // Update a subtask to completed, which will cause the database values to change.
        algorithmStateFiles.get(0).updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        taskMonitor.update();
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 1, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());

        // Set a subtask to failed, which will cause the database values to change.
        algorithmStateFiles.get(1).updateCurrentState(AlgorithmStateFiles.AlgorithmState.FAILED);
        taskMonitor.update();
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 1, 1,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());

        // Get the rest of the subasks to either completed or failed. Note that it's not the
        // TaskMonitor that advances the task state to WAITING_TO_STORE; that's the job of the
        // AlgorithmMonitor, because it needs to disposition the task at the end of execution.
        algorithmStateFiles.get(2).updateCurrentState(AlgorithmStateFiles.AlgorithmState.FAILED);
        algorithmStateFiles.get(3).updateCurrentState(AlgorithmStateFiles.AlgorithmState.FAILED);
        algorithmStateFiles.get(4).updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        algorithmStateFiles.get(5).updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        taskMonitor.update();
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 3, 3,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertTrue(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(1)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(1))
            .publishTaskProcessingCompleteMessage(ArgumentMatchers.any(CountDownLatch.class));
    }

    @Test
    public void testWorkerStatusMessage() {

        taskAlgorithmStateFiles.updateCurrentState(AlgorithmStateFiles.AlgorithmState.PROCESSING);
        algorithmStateFiles.get(0).updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());

        // A final message from the wrong worker isn't interesting.
        PipelineTask someOtherTask = spy(PipelineTask.class);
        when(someOtherTask.getId()).thenReturn(1000L);
        WorkerStatusMessage workerStatusMessage = new WorkerStatusMessage(1, "",
            Long.toString(pipelineTask.getPipelineInstanceId()), someOtherTask, "", "", 0, true);
        taskMonitor.handleWorkerStatusMessage(workerStatusMessage);
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(0)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(0))
            .publishTaskProcessingCompleteMessage(ArgumentMatchers.any(CountDownLatch.class));

        // A non-final message from the right worker isn't interesting, either.
        taskMonitor.handleWorkerStatusMessage(new WorkerStatusMessage(1, "",
            Long.toString(pipelineTask.getPipelineInstanceId()), pipelineTask, "", "", 0, false));
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(0)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(0))
            .publishTaskProcessingCompleteMessage(ArgumentMatchers.any(CountDownLatch.class));

        // A final message from the worker isn't interesting if the task is running remotely.
        PipelineNodeExecutionResources executionResources = pipelineTaskOperations
            .executionResources(pipelineTask);
        executionResources.setRemoteExecutionEnabled(true);
        executionResources = pipelineNodeOperations.merge(executionResources);
        taskMonitor.handleWorkerStatusMessage(new WorkerStatusMessage(1, "",
            Long.toString(pipelineTask.getPipelineInstanceId()), pipelineTask, "", "", 0, true));
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(0)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(0))
            .publishTaskProcessingCompleteMessage(ArgumentMatchers.any(CountDownLatch.class));

        // A final message from the right worker causes a final update.
        executionResources.setRemoteExecutionEnabled(false);
        executionResources = pipelineNodeOperations.merge(executionResources);
        taskMonitor.handleWorkerStatusMessage(new WorkerStatusMessage(1, "",
            Long.toString(pipelineTask.getPipelineInstanceId()), pipelineTask, "", "", 0, true));
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 1, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(1)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(1)).publishTaskProcessingCompleteMessage(null);
    }

    @Test
    public void testAllJobsFinishedMessage() {
        taskAlgorithmStateFiles.updateCurrentState(AlgorithmStateFiles.AlgorithmState.PROCESSING);
        algorithmStateFiles.get(0).updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());

        // A message for the wrong task isn't interesting.
        PipelineTask someOtherTask = spy(PipelineTask.class);
        when(someOtherTask.getId()).thenReturn(1000L);
        AllJobsFinishedMessage allJobsFinishedMessage = new AllJobsFinishedMessage(someOtherTask);
        taskMonitor.handleAllJobsFinishedMessage(allJobsFinishedMessage);
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(0)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(0))
            .publishTaskProcessingCompleteMessage(ArgumentMatchers.any(CountDownLatch.class));

        // A message for the right task causes a final update.
        allJobsFinishedMessage = new AllJobsFinishedMessage(pipelineTask);
        taskMonitor.handleAllJobsFinishedMessage(allJobsFinishedMessage);
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 1, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(1)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(1)).publishTaskProcessingCompleteMessage(null);
    }

    @Test
    public void testHaltTasksRequest() {
        taskAlgorithmStateFiles.updateCurrentState(AlgorithmStateFiles.AlgorithmState.PROCESSING);
        algorithmStateFiles.get(0).updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());

        // A message for the wrong task isn't interesting.
        PipelineTask someOtherTask = spy(PipelineTask.class);
        when(someOtherTask.getId()).thenReturn(1000L);
        HaltTasksRequest request = new HaltTasksRequest(List.of(someOtherTask));
        taskMonitor.handleHaltTasksRequest(request);
        assertEquals(ProcessingStep.QUEUED,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 0, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(0)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(0))
            .publishTaskProcessingCompleteMessage(ArgumentMatchers.any(CountDownLatch.class));

        // A message for the right task causes a final update.
        request = new HaltTasksRequest(List.of(pipelineTask));
        taskMonitor.handleHaltTasksRequest(request);
        assertEquals(ProcessingStep.EXECUTING,
            pipelineTaskDataOperations.processingStep(pipelineTask));
        TaskCountsTest.testSubtaskCounts(6, 1, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.allSubtasksProcessed());
        Mockito.verify(taskMonitor, Mockito.times(1)).shutdown();
        Mockito.verify(taskMonitor, Mockito.times(1)).publishTaskProcessingCompleteMessage(null);
    }

    @Test
    public void testCheckForFinishFile() {
        Mockito.doReturn(10L).when(taskMonitor).fileSystemCheckIntervalMillis();
        Mockito.doReturn(1).when(taskMonitor).fileSystemChecksCount();

        // A garden variety update with no finish file returns false.
        taskMonitor.update();
        assertFalse(taskMonitor.isFinishFileDetected());

        // A final update with no finish file returns false.
        HaltTasksRequest request = new HaltTasksRequest(List.of(pipelineTask));
        taskMonitor.handleHaltTasksRequest(request);
        assertFalse(taskMonitor.isFinishFileDetected());

        // A final update with a finish file returns true.
        taskMonitor.resetMonitoringEnabled();
        TimestampFile.create(taskDir.toFile(), TimestampFile.Event.FINISH);
        taskMonitor.handleHaltTasksRequest(request);
        assertTrue(taskMonitor.isFinishFileDetected());

        // Even when there is a finish file, the garden variety update doesn't
        // even look for it.
        taskMonitor.resetFinishFileDetection();
        taskMonitor.resetMonitoringEnabled();
        taskMonitor.update();
        assertFalse(taskMonitor.isFinishFileDetected());
    }

    @Test
    public void testRecheckSubtaskCounts() {
        Mockito.doReturn(10L).when(taskMonitor).fileSystemCheckIntervalMillis();
        Mockito.doReturn(1).when(taskMonitor).fileSystemChecksCount();

        // Set 5 out of 6 subtasks to complete.
        for (int stateCount = 0; stateCount < algorithmStateFiles.size() - 1; stateCount++) {
            algorithmStateFiles.get(stateCount)
                .updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        }

        // A garden variety update doesn't wait for subtask counts to update.
        taskMonitor.update();
        TaskCountsTest.testSubtaskCounts(6, 5, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.iswaitForSubtaskCountUpdates());
        assertFalse(taskMonitor.isAllSubtasksProcessed());

        // A final update after halting also doesn't wait.
        HaltTasksRequest request = new HaltTasksRequest(List.of(pipelineTask));
        taskMonitor.handleHaltTasksRequest(request);
        taskMonitor.update();
        assertFalse(taskMonitor.iswaitForSubtaskCountUpdates());
        assertFalse(taskMonitor.isAllSubtasksProcessed());

        // All remote jobs can finish and the task monitor also won't wait.
        taskMonitor.resetIncompleteSubtasksPossible();
        taskMonitor.resetMonitoringEnabled();
        AllJobsFinishedMessage allJobsFinishedMessage = new AllJobsFinishedMessage(pipelineTask);
        taskMonitor.handleAllJobsFinishedMessage(allJobsFinishedMessage);
        taskMonitor.update();
        assertFalse(taskMonitor.iswaitForSubtaskCountUpdates());
        assertFalse(taskMonitor.isAllSubtasksProcessed());

        // Set the finish file in place so we don't have to wait for it.
        TimestampFile.create(taskDir.toFile(), TimestampFile.Event.FINISH);

        // A final update with incomplete subtasks should wait.
        taskMonitor.resetIncompleteSubtasksPossible();
        taskMonitor.resetMonitoringEnabled();
        taskMonitor.handleWorkerStatusMessage(new WorkerStatusMessage(1, "",
            Long.toString(pipelineTask.getPipelineInstanceId()), pipelineTask, "", "", 0, true));
        assertTrue(taskMonitor.iswaitForSubtaskCountUpdates());
        TaskCountsTest.testSubtaskCounts(6, 5, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertFalse(taskMonitor.isAllSubtasksProcessed());

        // Set up so that the initial check for all-subtasks-complete returns false,
        // but all the subtasks are actually done. Here we should wait but ultimately the
        // subtask counts should show completion.
        taskMonitor.resetIncompleteSubtasksPossible();
        taskMonitor.resetMonitoringEnabled();
        taskMonitor.resetWaitedForSubtaskCountUpdates();
        Mockito.doReturn(false)
            .when(taskMonitor)
            .allSubtasksProcessed(ArgumentMatchers.any(SubtaskStateCounts.class));
        Mockito.doReturn(true).when(taskMonitor).allSubtasksProcessed();
        algorithmStateFiles.get(algorithmStateFiles.size() - 1)
            .updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        taskMonitor.handleWorkerStatusMessage(new WorkerStatusMessage(1, "",
            Long.toString(pipelineTask.getPipelineInstanceId()), pipelineTask, "", "", 0, true));
        assertTrue(taskMonitor.iswaitForSubtaskCountUpdates());
        TaskCountsTest.testSubtaskCounts(6, 6, 0,
            pipelineTaskDataOperations.subtaskCounts(pipelineTask));
        assertTrue(taskMonitor.isAllSubtasksProcessed());
    }

    @Test
    public void testCheckForMemoryWarnings() throws IOException {

        // First time there should be no alerts generated.
        taskMonitor.update();
        verify(taskMonitor).issueAlertForLowMemoryWarning();
        verify(taskMonitor, times(0)).issueMemoryAlert(ArgumentMatchers.anyString());
        assertTrue(
            CollectionUtils.isEmpty(new AlertLogOperations().alertLogs(List.of(pipelineTask))));

        // Create a memory warning file.
        Files.createFile(
            taskDir.resolve(ComputeNodeMaster.FREE_MEMORY_WARNING_FILE_NAME_PREFIX + "1234567"));
        taskMonitor.update();
        verify(taskMonitor, times(2)).issueAlertForLowMemoryWarning();
        verify(taskMonitor, times(1)).issueMemoryAlert(ArgumentMatchers.anyString());
        verify(taskMonitor, times(1)).issueMemoryAlert("1234567");
        List<AlertLog> alertLogs = new AlertLogOperations().alertLogs(List.of(pipelineTask));
        assertEquals(1, alertLogs.size());
        AlertLog alertLog = alertLogs.get(0);
        Alert alertData = alertLog.getAlertData();
        assertEquals(pipelineTask.getId(), alertData.getSourceTask().getId());
        assertEquals(Severity.WARNING, alertData.getSeverity());
        assertTrue(alertData.getMessage().contains("1234567"));

        // When we do another update, no new alert is generated.
        taskMonitor.update();
        verify(taskMonitor, times(3)).issueAlertForLowMemoryWarning();
        verify(taskMonitor, times(1)).issueMemoryAlert(ArgumentMatchers.anyString());
        verify(taskMonitor, times(1)).issueMemoryAlert("1234567");
        alertLogs = new AlertLogOperations().alertLogs(List.of(pipelineTask));
        assertEquals(1, alertLogs.size());
        assertEquals(alertLog.getId(), alertLogs.get(0).getId());

        // Add another memory warning file.
        Files.createFile(
            taskDir.resolve(ComputeNodeMaster.FREE_MEMORY_WARNING_FILE_NAME_PREFIX + "1234568"));
        taskMonitor.update();
        verify(taskMonitor, times(4)).issueAlertForLowMemoryWarning();
        verify(taskMonitor, times(2)).issueMemoryAlert(ArgumentMatchers.anyString());
        verify(taskMonitor, times(1)).issueMemoryAlert("1234567");
        verify(taskMonitor, times(1)).issueMemoryAlert("1234568");
        alertLogs = new AlertLogOperations().alertLogs(List.of(pipelineTask));
        assertEquals(2, alertLogs.size());
        for (AlertLog newAlertLog : alertLogs) {
            if (newAlertLog.getId() == alertLog.getId()) {
                continue;
            }
            alertData = newAlertLog.getAlertData();
            assertEquals(pipelineTask.getId(), alertData.getSourceTask().getId());
            assertEquals(Severity.WARNING, alertData.getSeverity());
            assertTrue(alertData.getMessage().contains("1234568"));
        }
    }
}
