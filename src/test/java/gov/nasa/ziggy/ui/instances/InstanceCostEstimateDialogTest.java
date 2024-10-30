package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.instances.InstanceCostEstimateDialog.instanceCost;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;

public class InstanceCostEstimateDialogTest {

    @Test
    public void testInstanceCost() {
        List<PipelineTaskDisplayData> pipelineTasks = List.of(createPipelineTask(0.000123));
        assertEquals("0.0001", instanceCost(pipelineTasks));
        pipelineTasks = List.of(createPipelineTask(1.00123));
        assertEquals("1.001", instanceCost(pipelineTasks));
        pipelineTasks = List.of(createPipelineTask(10.0123));
        assertEquals("10.01", instanceCost(pipelineTasks));
        pipelineTasks = List.of(createPipelineTask(100.123));
        assertEquals("100.1", instanceCost(pipelineTasks));
        pipelineTasks = List.of(createPipelineTask(1001.23));
        assertEquals("1001.2", instanceCost(pipelineTasks));
    }

    private PipelineTaskDisplayData createPipelineTask(double costEstimate) {
        PipelineTaskDisplayData pipelineTask = mock(PipelineTaskDisplayData.class);
        Mockito.when(pipelineTask.costEstimate()).thenReturn(costEstimate);
        return pipelineTask;
    }
}
