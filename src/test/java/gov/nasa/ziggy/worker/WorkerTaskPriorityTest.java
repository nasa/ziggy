package gov.nasa.ziggy.worker;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;

/**
 * Tests to ensure that task priority is correctly managed.
 *
 * @author PT
 */
public class WorkerTaskPriorityTest {

    @Test
    public void testTaskPrioritization() throws InterruptedException {

        PriorityBlockingQueue<WorkerTaskRequest> queue = new PriorityBlockingQueue<>();
        WorkerTaskRequest w1 = new WorkerTaskRequest(100, 100, 5, 4, false, RunMode.STANDARD);
        WorkerTaskRequest w2 = new WorkerTaskRequest(100, 100, 7, 0, false, RunMode.STANDARD);
        WorkerTaskRequest w3 = new WorkerTaskRequest(100, 100, 6, 4, false, RunMode.STANDARD);

        // Add the tasks in inverse-task-ID order
        queue.add(w2);
        queue.add(w3);
        queue.add(w1);

        // Retrieve the tasks and check their ordering

        assertEquals(w2, queue.take());
        assertEquals(w1, queue.take());
        assertEquals(w3, queue.take());
    }
}
