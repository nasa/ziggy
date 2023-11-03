package gov.nasa.ziggy.supervisor;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.services.messages.TaskRequest;

/**
 * Tests to ensure that task priority is correctly managed.
 *
 * @author PT
 */
public class WorkerTaskPriorityTest {

    @Test
    public void testTaskPrioritization() throws InterruptedException {

        PriorityBlockingQueue<TaskRequest> queue = new PriorityBlockingQueue<>();
        TaskRequest w1 = new TaskRequest(100, 100, 70, 5, Priority.LOWEST, false, RunMode.STANDARD);
        TaskRequest w2 = new TaskRequest(100, 100, 70, 7, Priority.HIGHEST, false,
            RunMode.STANDARD);
        TaskRequest w3 = new TaskRequest(100, 100, 70, 6, Priority.LOWEST, false, RunMode.STANDARD);
        TaskRequest w4 = new TaskRequest(100, 100, 69, 10, Priority.LOWEST, false,
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
