package gov.nasa.ziggy.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.times;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.util.Requestor;
import gov.nasa.ziggy.util.SystemProxy;

/**
 * Unit tests for the {@link PipelineWorker} class.
 * <p>
 * At this time, most of the functionality in PipelineWorker is legacy functionality, so there won't
 * be any unit tests added to cover those. What we will do is add unit tests that cover new
 * functionality.
 *
 * @author PT
 */
public class PipelineWorkerTest implements Requestor {

    private PipelineWorker worker = Mockito.spy(new PipelineWorker("dummy", 0));
    private KillTasksRequest request = KillTasksRequest.forTaskIds(this, List.of(1L, 2L, 3L));

    @Before
    public void setUp() {
        SystemProxy.disableExit();
        Mockito.doNothing()
            .when(worker)
            .sendKilledTaskMessage(Mockito.any(KillTasksRequest.class), Mockito.anyLong());
    }

    /**
     * Exercises the {@link PipelineWorker#killTasks(KillTasksRequest)} method when the kill-tasks
     * request doesn't include the task of the worker.
     */
    @Test
    public void testKillRequestOtherTasks() {
        worker.setTaskId(100L);
        worker.killTasks(request);
        Mockito.verify(worker, times(0)).sendKilledTaskMessage(request, 100L);
        Mockito.verify(worker, times(0)).killWorker();
        assertNull(SystemProxy.getLatestExitCode());
    }

    @Test
    public void testKillRequestThisTask() {
        worker.setTaskId(3L);
        worker.killTasks(request);
        Mockito.verify(worker).sendKilledTaskMessage(request, 3L);
        Mockito.verify(worker).killWorker();
        assertEquals(0, SystemProxy.getLatestExitCode().intValue());
    }

    @Override
    public Object requestorIdentifier() {
        return Integer.valueOf(100);
    }
}
