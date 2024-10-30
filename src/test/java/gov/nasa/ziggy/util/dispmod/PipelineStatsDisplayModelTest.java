package gov.nasa.ziggy.util.dispmod;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.SystemProxy;
import gov.nasa.ziggy.util.dispmod.PipelineStatsDisplayModel.TaskProcessingTimeStats;

public class PipelineStatsDisplayModelTest {

    private static final long START_MILLIS = 1700000000000L;
    private static final long HOUR_MILLIS = 60 * 60 * 1000;

    @Test
    public void test() {
        TaskProcessingTimeStats taskProcessingTimeStats = TaskProcessingTimeStats
            .of(pipelineTasks());
        assertEquals(3, taskProcessingTimeStats.getCount());
        assertEquals(2.0, taskProcessingTimeStats.getMax(), 0.0001);
        assertEquals(1.0, taskProcessingTimeStats.getMean(), 0.0001);
        assertEquals(0.0, taskProcessingTimeStats.getMin(), 0.0001);
        assertEquals(new Date(START_MILLIS), taskProcessingTimeStats.getMinStart());
        // TODO Calculator.net says stddev of 0, 1, 2 is 0.81649658092773, not 1.0
        assertEquals(1.0, taskProcessingTimeStats.getStddev(), 0.0001);
        assertEquals(3.0, taskProcessingTimeStats.getSum(), 0.0001);
    }

    private List<PipelineTaskDisplayData> pipelineTasks() {
        // The first task took two hours and the second task started after the first and took one
        // hour.
        return List.of(
            pipelineTask("module1", new Date(START_MILLIS),
                new Date(START_MILLIS + 2 * HOUR_MILLIS)),
            pipelineTask("module2", new Date(START_MILLIS + 4 * HOUR_MILLIS),
                new Date(START_MILLIS + 5 * HOUR_MILLIS)),
            pipelineTask("module3", new Date(0), new Date(0)));
    }

    private PipelineTaskDisplayData pipelineTask(String moduleName, Date start, Date end) {
        PipelineTask pipelineTask = Mockito.mock(PipelineTask.class);
        doReturn((long) 1).when(pipelineTask).getId();
        doReturn(start).when(pipelineTask).getCreated();
        doReturn(new UnitOfWork(moduleName)).when(pipelineTask).getUnitOfWork();

        PipelineTaskDisplayData pipelineTaskDisplayData = new PipelineTaskDisplayData(
            new PipelineTaskData(pipelineTask));
        SystemProxy.setUserTime(start.getTime());
        pipelineTaskDisplayData.getExecutionClock().start();
        SystemProxy.setUserTime(end.getTime());
        pipelineTaskDisplayData.getExecutionClock().stop();

        return pipelineTaskDisplayData;
    }
}
