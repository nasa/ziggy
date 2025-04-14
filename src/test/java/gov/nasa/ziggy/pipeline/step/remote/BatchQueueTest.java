package gov.nasa.ziggy.pipeline.step.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

/** Unit tests for {@link BatchQueue} class. */
public class BatchQueueTest {

    @Test
    public void testAutoSelectableBatchQueues() {
        List<BatchQueue> autoSelectableBatchQueues = BatchQueue
            .autoSelectableBatchQueues(BatchQueueTestUtils.batchQueues());
        assertEquals(3, autoSelectableBatchQueues.size());
        assertEquals("low", autoSelectableBatchQueues.get(0).getName());
        assertEquals("normal", autoSelectableBatchQueues.get(1).getName());
        assertEquals("long", autoSelectableBatchQueues.get(2).getName());
    }

    @Test
    public void testReservedBatchQueueWithQueueName() {
        BatchQueue queue = BatchQueue.reservedBatchQueueWithQueueName(
            BatchQueueTestUtils.batchQueueByName().get("reserved"), "R123456");
        assertEquals("R123456", queue.getName());
        assertEquals("the most reserved", queue.getDescription());
        assertEquals(Float.MAX_VALUE, queue.getMaxWallTimeHours(), 1e-6);
        assertEquals(Integer.MAX_VALUE, queue.getMaxNodes());
        assertFalse(queue.isAutoSelectable());
        assertTrue(queue.isReserved());
    }
}
