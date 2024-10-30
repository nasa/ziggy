package gov.nasa.ziggy.util.dispmod;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;

public class TaskSummaryDisplayModelTest {

    @Test
    public void test() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpFivePipelineTaskDisplayData();
        List<PipelineTaskDisplayData> pipelineTasks = pipelineOperationsTestUtils
            .getPipelineTaskDisplayData();
        TaskCounts taskCounts = new TaskCounts(pipelineTasks);
        TaskSummaryDisplayModel model = new TaskSummaryDisplayModel(taskCounts);
        assertEquals(6, model.getColumnCount());
        assertEquals("Module", model.getColumnName(0));
        assertEquals(pipelineTasks.size() + 1, model.getRowCount()); // a row for the total is added

        assertEquals(taskCounts.getModuleCounts().get("module1"), model.getContentAtRow(0));
        assertEquals("module1", model.getValueAt(0, 0));
        assertEquals(1, model.getValueAt(0, 1));
        assertEquals(0, model.getValueAt(0, 2));
        assertEquals(0, model.getValueAt(0, 3));
        assertEquals(0, model.getValueAt(0, 4));
        assertEquals("9/10 (1)", model.getValueAt(0, 5));

        assertEquals("", model.getValueAt(5, 0));
        assertEquals(2, model.getValueAt(5, 1));
        assertEquals(1, model.getValueAt(5, 2));
        assertEquals(1, model.getValueAt(5, 3));
        assertEquals(1, model.getValueAt(5, 4));
        assertEquals("135/150 (15)", model.getValueAt(5, 5));
    }
}
