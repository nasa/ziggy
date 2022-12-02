package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author Todd Klaus
 */
public class WorkerMemoryManagerTest {
    private static final int AVAILABLE_MEGA_BYTES = 1000;
    private static final int ALLOCATION_SIZE = 600;

    @Test
    public void testSemaphore() {
        WorkerMemoryManager semaphore = new WorkerMemoryManager(AVAILABLE_MEGA_BYTES);

        // availableMemory == 1000
        assertEquals("availableMemory", semaphore.availableMemoryMegaBytes(), AVAILABLE_MEGA_BYTES);

        assertTrue("first acquire", semaphore.tryAcquireMegaBytes(ALLOCATION_SIZE));

        // availableMemory == 400
        assertEquals("availableMemory", semaphore.availableMemoryMegaBytes(),
            AVAILABLE_MEGA_BYTES - ALLOCATION_SIZE);

        assertFalse("second acquire", semaphore.tryAcquireMegaBytes(ALLOCATION_SIZE));

        // availableMemory == 400
        assertEquals("availableMemory", semaphore.availableMemoryMegaBytes(),
            AVAILABLE_MEGA_BYTES - ALLOCATION_SIZE);

        semaphore.releaseMemoryMegaBytes(ALLOCATION_SIZE);

        // availableMemory == 1000
        assertEquals("availableMemory", semaphore.availableMemoryMegaBytes(), AVAILABLE_MEGA_BYTES);

        assertTrue("third acquire", semaphore.tryAcquireMegaBytes(ALLOCATION_SIZE));

        // availableMemory == 400
        assertEquals("availableMemory", semaphore.availableMemoryMegaBytes(),
            AVAILABLE_MEGA_BYTES - ALLOCATION_SIZE);
    }
}
