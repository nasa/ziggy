package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

public class SubtaskAllocatorTest {

    private TaskConfiguration taskConfigurationManager;

    @Before
    public void setup() {
        taskConfigurationManager = mock(TaskConfiguration.class);
    }

    /**
     * Tests the use-case in which there is a single set of subtasks, potentially running in
     * multiple separate jobs.
     */
    @Test
    public void testAllocatorWithSingleSubtaskSet() {
        when(taskConfigurationManager.getSubtaskCount()).thenReturn(6);
        SubtaskAllocator allocator = new SubtaskAllocator(taskConfigurationManager);

        SubtaskAllocation allocation;

        // Get the 6 subtasks
        for (int i = 0; i < 6; i++) {
            allocation = allocator.nextSubtask();
            assertEquals(SubtaskServer.ResponseType.OK, allocation.getStatus());
            assertEquals(i, allocation.getSubtaskIndex());
        }

        // Now we mark subtasks as complete
        allocator.markSubtaskComplete(0);
        allocator.markSubtaskComplete(1);

        // But there are also some that are getting processed by another job
        allocator.markSubtaskLocked(2);
        allocator.markSubtaskLocked(3);

        // which means that we should get subtasks 2 and 3 offered again
        for (int i = 2; i < 4; i++) {
            allocation = allocator.nextSubtask();
            assertEquals(SubtaskServer.ResponseType.OK, allocation.getStatus());
            assertEquals(i, allocation.getSubtaskIndex());
        }

        // mark the subtasks as still locked
        allocator.markSubtaskLocked(2);
        allocator.markSubtaskLocked(3);

        // which means that we should get subtasks 2 and 3 offered again
        for (int i = 2; i < 4; i++) {
            allocation = allocator.nextSubtask();
            assertEquals(SubtaskServer.ResponseType.OK, allocation.getStatus());
            assertEquals(i, allocation.getSubtaskIndex());
        }

        // Now mark subtasks 2 and 3 as completed (they unlocked, and so this job was able to
        // see that they have .COMPLETE in their directories)
        allocator.markSubtaskComplete(2);
        allocator.markSubtaskComplete(3);
        allocator.markSubtaskLocked(4);
        allocation = allocator.nextSubtask();
        assertEquals(SubtaskServer.ResponseType.OK, allocation.getStatus());
        assertEquals(4, allocation.getSubtaskIndex());

        // Mark 4 as complete but leave 5 as processing. This should result in a TRY_AGAIN status.
        allocator.markSubtaskComplete(4);
        allocation = allocator.nextSubtask();
        assertEquals(SubtaskServer.ResponseType.TRY_AGAIN, allocation.getStatus());
        assertEquals(-1, allocation.getSubtaskIndex());

        // Mark 5 as locked. It should be offered to the server again.
        allocator.markSubtaskLocked(5);
        allocation = allocator.nextSubtask();
        assertEquals(SubtaskServer.ResponseType.OK, allocation.getStatus());
        assertEquals(5, allocation.getSubtaskIndex());

        // Finally, mark 5 as complete. Only when all the subtasks are complete should
        // the server be told that no more subtasks are available.
        allocator.markSubtaskComplete(5);
        allocation = allocator.nextSubtask();
        assertEquals(SubtaskServer.ResponseType.NO_MORE, allocation.getStatus());
        assertEquals(-1, allocation.getSubtaskIndex());
    }
}
