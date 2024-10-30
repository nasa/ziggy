package gov.nasa.ziggy.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.messages.HaltTasksRequest;
import gov.nasa.ziggy.util.SystemProxy;

/**
 * Unit tests for the {@link PipelineWorker} class.
 * <p>
 * At this time, most of the functionality in PipelineWorker is legacy functionality, so there won't
 * be any unit tests added to cover those. What we will do is add unit tests that cover new
 * functionality.
 *
 * @author PT
 * @author Bill Wohler
 */
public class PipelineWorkerTest {

    private PipelineTask pipelineTask1;
    private PipelineTask pipelineTask2;
    private PipelineTask pipelineTask3;
    private PipelineTask pipelineTask100;

    private HaltTasksRequest request;

    @Before
    public void setUp() {
        SystemProxy.disableExit();

        pipelineTask1 = spy(PipelineTask.class);
        pipelineTask2 = spy(PipelineTask.class);
        pipelineTask3 = spy(PipelineTask.class);
        pipelineTask100 = spy(PipelineTask.class);

        List<PipelineTask> pipelineTasks = List.of(pipelineTask1, pipelineTask2, pipelineTask3);
        request = new HaltTasksRequest(pipelineTasks);
    }

    /**
     * Exercises the {@link PipelineWorker#haltTask(HaltTasksRequest)} method when the kill-tasks
     * request doesn't include the task of the worker.
     */
    @Test
    public void testHaltRequestOtherTasks() {
        PipelineWorker worker = createPipelineWorker(pipelineTask100);
        worker.haltTask(request);
        Mockito.verify(worker, times(0)).sendTaskHaltedMessage(request, pipelineTask100);
        Mockito.verify(worker, times(0)).killWorker();
        assertNull(SystemProxy.getLatestExitCode());
    }

    @Test
    public void testHaltRequestThisTask() {
        PipelineWorker worker = createPipelineWorker(pipelineTask3);
        worker.haltTask(request);
        Mockito.verify(worker).sendTaskHaltedMessage(request, pipelineTask3);
        Mockito.verify(worker).killWorker();
        assertEquals(0, SystemProxy.getLatestExitCode().intValue());
    }

    private PipelineWorker createPipelineWorker(PipelineTask pipelineTask) {
        PipelineWorker worker = spy(new PipelineWorker("dummy", pipelineTask, 0));
        Mockito.doNothing()
            .when(worker)
            .sendTaskHaltedMessage(Mockito.any(HaltTasksRequest.class),
                Mockito.any(PipelineTask.class));
        return worker;
    }
}
