package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.module.AlgorithmType;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Units;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Performs unit tests for {@link PipelineTaskData} and {@link PipelineTaskDisplayData}.
 *
 * @author Bill Wohler
 */

public class PipelineTaskDataTest {

    private static final long PIPELINE_TASK_ID = 42L;
    private static final long PIPELINE_INSTANCE_ID = 43L;
    private static final long NOW = 1000L;
    private static final Date CREATED = new Date(NOW);
    private static final String MODULE_NAME = "moduleName";
    private static final String BRIEF_STATE = "briefState";
    private static final String ZIGGY_SOFTWARE_REVISION = "ziggyRevision";
    private static final String PIPELINE_SOFTWARE_REVISION = "pipelineRevision";
    private static final ProcessingStep PROCESSING_STEP = ProcessingStep.EXECUTING;
    private static final String WORKER_HOST = "workerHost";
    private static final int WORKER_THREAD = 24;
    private static final String WORKER_NAME = WORKER_HOST + ":" + WORKER_THREAD;
    private static final ExecutionClock EXECUTION_CLOCK = new ExecutionClock();
    private static final int COMPLETED_SUBTASK_COUNT = 2;
    private static final int FAILED_SUBTASK_COUNT = 3;
    private static final int RUNNING_SUBTASK_COUNT = 1;
    private static final int TOTAL_SUBTASK_COUNT = COMPLETED_SUBTASK_COUNT + FAILED_SUBTASK_COUNT
        - RUNNING_SUBTASK_COUNT;
    private static final int FAILURE_COUNT = 4;
    private PipelineTask pipelineTask;
    private PipelineTaskData pipelineTaskData;
    private PipelineTaskDisplayData pipelineTaskDisplayData;
    private Set<RemoteJob> remoteJobs;
    private List<PipelineTaskMetric> pipelineTaskMetrics;

    @Before
    public void setUp() {
        pipelineTask = spy(PipelineTask.class);
        doReturn(PIPELINE_TASK_ID).when(pipelineTask).getId();
        doReturn(PIPELINE_INSTANCE_ID).when(pipelineTask).getPipelineInstanceId();
        doReturn(MODULE_NAME).when(pipelineTask).getModuleName();
        doReturn(new UnitOfWork(BRIEF_STATE)).when(pipelineTask).getUnitOfWork();
        doReturn(CREATED).when(pipelineTask).getCreated();

        pipelineTaskData = new PipelineTaskData(pipelineTask);
        pipelineTaskData.setZiggySoftwareRevision(ZIGGY_SOFTWARE_REVISION);
        pipelineTaskData.setPipelineSoftwareRevision(PIPELINE_SOFTWARE_REVISION);
        pipelineTaskData.setProcessingStep(PROCESSING_STEP);
        pipelineTaskData.setWorkerHost(WORKER_HOST);
        pipelineTaskData.setWorkerThread(WORKER_THREAD);
        pipelineTaskData.setCompletedSubtaskCount(COMPLETED_SUBTASK_COUNT);
        pipelineTaskData.setFailedSubtaskCount(FAILED_SUBTASK_COUNT);
        pipelineTaskData.setTotalSubtaskCount(TOTAL_SUBTASK_COUNT);
        pipelineTaskData.setFailureCount(FAILURE_COUNT);
        pipelineTaskMetrics = List
            .of(new PipelineTaskMetric(MODULE_NAME, RUNNING_SUBTASK_COUNT, Units.RATE));
        pipelineTaskData.setPipelineTaskMetrics(pipelineTaskMetrics);
        RemoteJob remoteJob = new RemoteJob(1);
        remoteJob.setCostEstimate(42.0);
        remoteJobs = Set.of(remoteJob);
        pipelineTaskData.setRemoteJobs(remoteJobs);

        pipelineTaskDisplayData = new PipelineTaskDisplayData(pipelineTaskData);
    }

    @Test
    public void testCostEstimate() {
        assertEquals(42.0, pipelineTaskDisplayData.costEstimate(), 0.0);
    }

    @Test
    public void testGetPipelineTask() {
        assertEquals(pipelineTask, pipelineTaskDisplayData.getPipelineTask());
    }

    @Test
    public void testGetPipelineTaskId() {
        assertEquals(PIPELINE_TASK_ID, pipelineTaskDisplayData.getPipelineTaskId());
    }

    @Test
    public void testGetPipelineInstanceId() {
        assertEquals(PIPELINE_INSTANCE_ID, pipelineTaskDisplayData.getPipelineInstanceId());
    }

    @Test
    public void testGetCreated() {
        assertEquals(CREATED, pipelineTaskDisplayData.getCreated());
    }

    @Test
    public void testGetModuleName() {
        assertEquals(MODULE_NAME, pipelineTaskDisplayData.getModuleName());
    }

    @Test
    public void testGetBriefState() {
        assertEquals(BRIEF_STATE, pipelineTaskDisplayData.getBriefState());
    }

    @Test
    public void testGetZiggySoftwareRevision() {
        assertEquals(ZIGGY_SOFTWARE_REVISION, pipelineTaskDisplayData.getZiggySoftwareRevision());
    }

    @Test
    public void testGetPipelineSoftwareRevision() {
        assertEquals(PIPELINE_SOFTWARE_REVISION,
            pipelineTaskDisplayData.getPipelineSoftwareRevision());
    }

    @Test
    public void testGetWorkerName() {
        assertEquals(WORKER_HOST, pipelineTaskData.getWorkerHost());
        assertEquals(WORKER_THREAD, pipelineTaskData.getWorkerThread());
        assertEquals(WORKER_NAME, pipelineTaskDisplayData.getWorkerName());
    }

    @Test
    public void testGetExecutionClock() {
        assertEquals(EXECUTION_CLOCK, pipelineTaskDisplayData.getExecutionClock());
    }

    @Test
    public void testGetProcessingStep() {
        assertEquals(PROCESSING_STEP, pipelineTaskDisplayData.getProcessingStep());
    }

    @Test
    public void testGetDisplayProcessingStep() {
        assertEquals(PROCESSING_STEP.toString(),
            pipelineTaskDisplayData.getDisplayProcessingStep());

        pipelineTaskData.setError(true);
        pipelineTaskDisplayData = new PipelineTaskDisplayData(pipelineTaskData);

        assertEquals("ERROR - " + PROCESSING_STEP.toString(),
            pipelineTaskDisplayData.getDisplayProcessingStep());
    }

    @Test
    public void testIsError() {
        assertEquals(false, pipelineTaskDisplayData.isError());

        pipelineTaskData.setHaltRequested(true);
        assertEquals(true, pipelineTaskData.isHaltRequested());

        pipelineTaskData.setRetry(true);
        assertEquals(true, pipelineTaskData.isRetry());
    }

    @Test
    public void testGetTotalSubtaskCount() {
        assertEquals(TOTAL_SUBTASK_COUNT, pipelineTaskDisplayData.getTotalSubtaskCount());
    }

    @Test
    public void testGetCompletedSubtaskCount() {
        assertEquals(COMPLETED_SUBTASK_COUNT, pipelineTaskDisplayData.getCompletedSubtaskCount());
    }

    @Test
    public void testGetFailedSubtaskCount() {
        assertEquals(FAILED_SUBTASK_COUNT, pipelineTaskDisplayData.getFailedSubtaskCount());
    }

    @Test
    public void testGetFailureCount() {
        assertEquals(FAILURE_COUNT, pipelineTaskDisplayData.getFailureCount());

        pipelineTaskData.incrementFailureCount();
        pipelineTaskDisplayData = new PipelineTaskDisplayData(pipelineTaskData);

        assertEquals(FAILURE_COUNT + 1, pipelineTaskDisplayData.getFailureCount());
    }

    @Test
    public void testAutoResubmitCount() {
        pipelineTaskData.setAutoResubmitCount(42);
        assertEquals(42, pipelineTaskData.getAutoResubmitCount());
        pipelineTaskData.incrementAutoResubmitCount();
        assertEquals(43, pipelineTaskData.getAutoResubmitCount());
        pipelineTaskData.resetAutoResubmitCount();
        assertEquals(0, pipelineTaskData.getAutoResubmitCount());
    }

    @Test
    public void testAlgorithmType() {
        pipelineTaskData.setAlgorithmType(AlgorithmType.REMOTE);
        assertEquals(AlgorithmType.REMOTE, pipelineTaskData.getAlgorithmType());
    }

    @Test
    public void testTaskLogIndex() {
        pipelineTaskData.setTaskLogIndex(42);
        assertEquals(42, pipelineTaskData.getTaskLogIndex());
        pipelineTaskData.incrementTaskLogIndex();
        assertEquals(43, pipelineTaskData.getTaskLogIndex());
    }

    @Test
    public void testGetPipelineTaskMetrics() {
        assertEquals(pipelineTaskMetrics, pipelineTaskDisplayData.getPipelineTaskMetrics());
    }

    @Test
    public void testTaskExecutionLogs() {

        List<TaskExecutionLog> taskExecutionLogs = List
            .of(new TaskExecutionLog(WORKER_HOST, WORKER_THREAD));
        pipelineTaskData.setTaskExecutionLogs(taskExecutionLogs);
        assertEquals(taskExecutionLogs, pipelineTaskData.getTaskExecutionLogs());
    }

    @Test
    public void testGetRemoteJobs() {
        assertEquals(remoteJobs, pipelineTaskDisplayData.getRemoteJobs());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testHashCodeEquals() {
        PipelineTaskDisplayData pipelineTaskDisplayData2 = new PipelineTaskDisplayData(
            pipelineTaskData);

        assertEquals(pipelineTaskDisplayData.hashCode(), pipelineTaskDisplayData2.hashCode());
        assertTrue(pipelineTaskDisplayData.equals(pipelineTaskDisplayData));
        assertTrue(pipelineTaskDisplayData.equals(pipelineTaskDisplayData2));

        PipelineTaskData pipelineTaskData2 = new PipelineTaskData(pipelineTask);

        assertEquals(pipelineTaskData.hashCode(), pipelineTaskData2.hashCode());
        assertTrue(pipelineTaskData.equals(pipelineTaskData));
        assertTrue(pipelineTaskData.equals(pipelineTaskData2));

        PipelineTask pipelineTask2 = spy(PipelineTask.class);
        doReturn(PIPELINE_TASK_ID + 1).when(pipelineTask2).getId();
        doReturn(new UnitOfWork(BRIEF_STATE + "foo")).when(pipelineTask2).getUnitOfWork();

        pipelineTaskData2 = new PipelineTaskData(pipelineTask2);
        pipelineTaskDisplayData2 = new PipelineTaskDisplayData(pipelineTaskData2);
        assertNotEquals(pipelineTaskDisplayData.hashCode(), pipelineTaskDisplayData2.hashCode());
        assertFalse(pipelineTaskDisplayData.equals(pipelineTaskDisplayData2));

        assertNotEquals(pipelineTaskData.hashCode(), pipelineTaskData2.hashCode());
        assertFalse(pipelineTaskData.equals(pipelineTaskData2));

        assertFalse(pipelineTaskDisplayData.equals(null));
        assertFalse(pipelineTaskDisplayData.equals("a string"));

        assertFalse(pipelineTaskData.equals(null));
        assertFalse(pipelineTaskData.equals("a string"));
    }

    @Test
    public void testToFullString() {
        assertEquals(
            "PipelineTaskDisplayData: pipelineTaskId=" + PIPELINE_TASK_ID + ", moduleName="
                + MODULE_NAME + ", briefState=" + BRIEF_STATE,
            pipelineTaskDisplayData.toFullString());
    }

    @Test
    public void testToString() {
        assertEquals(Long.toString(PIPELINE_TASK_ID), pipelineTaskDisplayData.toString());
    }
}
