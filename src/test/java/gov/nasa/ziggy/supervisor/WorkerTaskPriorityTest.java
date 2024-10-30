package gov.nasa.ziggy.supervisor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.messages.TaskRequest;

/**
 * Tests to ensure that task priority is correctly managed.
 *
 * @author PT
 * @author Bill Wohler
 */
public class WorkerTaskPriorityTest {

    private PipelineTask pipelineTask5;
    private PipelineTask pipelineTask6;
    private PipelineTask pipelineTask7;
    private PipelineTask pipelineTask10;

    @Before
    public void setUp() {
        pipelineTask5 = spy(PipelineTask.class);
        doReturn(5L).when(pipelineTask5).getId();
        pipelineTask6 = spy(PipelineTask.class);
        doReturn(6L).when(pipelineTask6).getId();
        pipelineTask7 = spy(PipelineTask.class);
        doReturn(7L).when(pipelineTask7).getId();
        pipelineTask10 = spy(PipelineTask.class);
        doReturn(10L).when(pipelineTask10).getId();
    }

    @Test
    public void testTaskPrioritization() throws InterruptedException {

        PriorityBlockingQueue<TaskRequest> queue = new PriorityBlockingQueue<>();
        TaskRequest w1 = new TaskRequest(100, 100, 70, pipelineTask5, Priority.LOWEST, false,
            RunMode.STANDARD);
        TaskRequest w2 = new TaskRequest(100, 100, 70, pipelineTask7, Priority.HIGHEST, false,
            RunMode.STANDARD);
        TaskRequest w3 = new TaskRequest(100, 100, 70, pipelineTask6, Priority.LOWEST, false,
            RunMode.STANDARD);
        TaskRequest w4 = new TaskRequest(100, 100, 69, pipelineTask10, Priority.LOWEST, false,
            RunMode.STANDARD);

        // Add the tasks in inverse-task-ID order
        queue.add(w4);
        queue.add(w2);
        queue.add(w3);
        queue.add(w1);

        // Retrieve the tasks and check their ordering

        assertEquals(w4, queue.take());
        assertEquals(w2, queue.take());
        assertEquals(w1, queue.take());
        assertEquals(w3, queue.take());
    }
}
