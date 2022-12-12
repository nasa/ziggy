package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Unit tests for {@link ProcessingSummaryOperations}.
 *
 * @author PT
 */
public class ProcessingSummaryOperationsTest {

    List<PipelineTask> tasks = new ArrayList<>();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {

        // Create some instances in the database.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTask pipelineTask1 = new PipelineTask();
            PipelineTask pipelineTask2 = new PipelineTask();
            PipelineInstance pipelineInstance = new PipelineInstance();
            pipelineTask1.setPipelineInstance(pipelineInstance);
            pipelineTask2.setPipelineInstance(pipelineInstance);
            new PipelineInstanceCrud().create(pipelineInstance);
            PipelineTaskCrud crud = new PipelineTaskCrud();
            crud.create(pipelineTask1);
            crud.create(pipelineTask2);
            tasks.add(pipelineTask1);
            tasks.add(pipelineTask2);
            return null;
        });
    }

    @Test
    public void testProcessingSummary() {
        ProcessingSummary summary = new ProcessingSummaryOperations().processingSummary(1L);
        assertEquals(1L, summary.getId());
        assertEquals(0, summary.getTotalSubtaskCount());
        assertEquals(0, summary.getCompletedSubtaskCount());
        assertEquals(0, summary.getFailedSubtaskCount());
        assertEquals(ProcessingState.INITIALIZING, summary.getProcessingState());
    }

    @Test
    public void testUpdateTaskCounts() {
        ProcessingSummaryOperations ops = new ProcessingSummaryOperations();
        ops.updateSubTaskCounts(1L, 10, 1, 2);
        ProcessingSummary summary = ops.processingSummary(1L);
        assertEquals(1L, summary.getId());
        assertEquals(10, summary.getTotalSubtaskCount());
        assertEquals(1, summary.getCompletedSubtaskCount());
        assertEquals(2, summary.getFailedSubtaskCount());
        assertEquals(ProcessingState.INITIALIZING, summary.getProcessingState());

        summary = ops.processingSummary(2L);
        assertEquals(2L, summary.getId());
        assertEquals(0, summary.getTotalSubtaskCount());
        assertEquals(0, summary.getCompletedSubtaskCount());
        assertEquals(0, summary.getFailedSubtaskCount());
        assertEquals(ProcessingState.INITIALIZING, summary.getProcessingState());
    }

    @Test
    public void testUpdateProcessingState() {
        ProcessingSummaryOperations ops = new ProcessingSummaryOperations();
        ops.updateProcessingState(2L, ProcessingState.ALGORITHM_QUEUED);
        ProcessingSummary summary = ops.processingSummary(2L);
        assertEquals(2L, summary.getId());
        assertEquals(0, summary.getTotalSubtaskCount());
        assertEquals(0, summary.getCompletedSubtaskCount());
        assertEquals(0, summary.getFailedSubtaskCount());
        assertEquals(ProcessingState.ALGORITHM_QUEUED, summary.getProcessingState());

        summary = ops.processingSummary(1L);
        assertEquals(1L, summary.getId());
        assertEquals(0, summary.getTotalSubtaskCount());
        assertEquals(0, summary.getCompletedSubtaskCount());
        assertEquals(0, summary.getFailedSubtaskCount());
        assertEquals(ProcessingState.INITIALIZING, summary.getProcessingState());
    }

    @Test
    public void testProcessingSummaries() {
        ProcessingSummaryOperations ops = new ProcessingSummaryOperations();
        ops.updateSubTaskCounts(1L, 10, 1, 2);
        ops.updateProcessingState(2L, ProcessingState.ALGORITHM_QUEUED);
        Map<Long, ProcessingSummary> summaries = ops.processingSummaries(tasks);
        assertEquals(2, summaries.size());
        ProcessingSummary summary = summaries.get(1L);
        assertEquals(1L, summary.getId());
        assertEquals(10, summary.getTotalSubtaskCount());
        assertEquals(1, summary.getCompletedSubtaskCount());
        assertEquals(2, summary.getFailedSubtaskCount());
        assertEquals(ProcessingState.INITIALIZING, summary.getProcessingState());
        summary = summaries.get(2L);
        assertEquals(2L, summary.getId());
        assertEquals(0, summary.getTotalSubtaskCount());
        assertEquals(0, summary.getCompletedSubtaskCount());
        assertEquals(0, summary.getFailedSubtaskCount());
        assertEquals(ProcessingState.ALGORITHM_QUEUED, summary.getProcessingState());
    }

    @Test
    public void testProcessingSummariesForInstance() {
        DatabaseTransactionFactory.performTransaction(() -> {
            ProcessingSummaryOperations ops = new ProcessingSummaryOperations();
            ops.updateSubTaskCounts(1L, 10, 1, 2);
            ops.updateProcessingState(2L, ProcessingState.ALGORITHM_QUEUED);
            Map<Long, ProcessingSummary> summaries = ops.processingSummariesForInstanceInternal(1L);
            assertEquals(2, summaries.size());
            ProcessingSummary summary = summaries.get(1L);
            assertEquals(1L, summary.getId());
            assertEquals(10, summary.getTotalSubtaskCount());
            assertEquals(1, summary.getCompletedSubtaskCount());
            assertEquals(2, summary.getFailedSubtaskCount());
            assertEquals(ProcessingState.INITIALIZING, summary.getProcessingState());
            summary = summaries.get(2L);
            assertEquals(2L, summary.getId());
            assertEquals(0, summary.getTotalSubtaskCount());
            assertEquals(0, summary.getCompletedSubtaskCount());
            assertEquals(0, summary.getFailedSubtaskCount());
            assertEquals(ProcessingState.ALGORITHM_QUEUED, summary.getProcessingState());
            return null;
        });
    }

}
