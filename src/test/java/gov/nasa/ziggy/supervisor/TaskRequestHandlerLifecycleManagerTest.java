package gov.nasa.ziggy.supervisor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.util.Requestor;
import gov.nasa.ziggy.worker.WorkerResources;

/**
 * Unit tests for {@link TaskRequestHandlerLifecycleManager} class.
 *
 * @author PT
 */
public class TaskRequestHandlerLifecycleManagerTest {

    private InstrumentedTaskRequestHandlerLifecycleManager lifecycleManager;
    private Thread taskRequestLoopThread;

    @Before
    public void setUp() throws InterruptedException {
        DatabaseService.setInstance(Mockito.mock(DatabaseService.class));

        // Use the InstrumentedTaskRequestHandlerLifecycleManager class so we can supply a desired
        // worker count directly.
        lifecycleManager = new InstrumentedTaskRequestHandlerLifecycleManager();
        lifecycleManager.setMaxWorkers(1);
        taskRequestLoopThread = new Thread(() -> {
            lifecycleManager.start();
        });
        taskRequestLoopThread.setDaemon(true);
    }

    @After
    public void tearDown() throws InterruptedException {
        lifecycleManager.shutdown();
        taskRequestLoopThread.interrupt();
        TestEventDetector.detectTestEvent(500L,
            () -> lifecycleManager.getTaskRequestThreadPool() == null);
        lifecycleManager = null;
        DatabaseService.reset();
    }

    /**
     * Tests that the constructor and startup work correctly, to wit:
     * <ol>
     * <li>The executor service and loop thread start correctly.
     * <li>The executor service has the correct number of task request handler threads.
     * <li>The correct pipeline definition node ID is stored.
     * <li>The countdown latch is present and in the correct state.
     * <li>The loop thread is waiting for new task requests.
     * </ol>
     */
    @Test
    public void testStart() {

        List<List<TaskRequestHandler>> allTaskRequestHandlers = lifecycleManager
            .getTaskRequestHandlers();
        taskRequestLoopThread.start();

        // Right out of the box there should be no task request handlers and no executor
        // for handlers.
        assertEquals(0, allTaskRequestHandlers.size());
        assertNull(lifecycleManager.getTaskRequestThreadCountdownLatch());
        assertEquals(0, lifecycleManager.taskRequestSize());
        assertNull(lifecycleManager.getPipelineDefinitionNodeId());

        // Add a task to the queue and wait for it to get handled.
        lifecycleManager.addTaskRequestToQueue(new TaskRequest(1L, 1L, -1L, 1L,
            PipelineInstance.Priority.NORMAL, false, PipelineModule.RunMode.STANDARD));
        TestEventDetector.detectTestEvent(500L, () -> allTaskRequestHandlers.size() > 0);
        // There should be one set of task request handlers constructed, with 1 handler in the set.
        assertEquals(1, allTaskRequestHandlers.size());
        List<TaskRequestHandler> taskRequestHandlers = allTaskRequestHandlers.get(0);
        assertEquals(1, taskRequestHandlers.size());
        TaskRequestHandler taskRequestHandler = taskRequestHandlers.get(0);

        // The handler should have handled 1 task request.

        TestEventDetector.detectTestEvent(500L,
            () -> taskRequestHandler.getTaskRequests().size() > 0);
        Set<TaskRequest> taskRequests = taskRequestHandler.getTaskRequests();
        assertEquals(1, taskRequests.size());
        TaskRequest taskRequest = taskRequests.iterator().next();
        assertEquals(-1L, taskRequest.getPipelineDefinitionNodeId());

        // The manager should have the correct pipeline instance ID.
        assertEquals(-1L, lifecycleManager.getPipelineDefinitionNodeId().longValue());

        // The countdown latch itself should still have a nonzero count.
        CountDownLatch countdownLatch = lifecycleManager.getTaskRequestThreadCountdownLatch();
        assertNotNull(countdownLatch);
        assertEquals(1, countdownLatch.getCount());

        // There should still be a running executor service.
        assertNotNull(lifecycleManager.getTaskRequestThreadPool());
        assertFalse(lifecycleManager.getTaskRequestThreadPool().isShutdown());
    }

    /**
     * Tests that the shutdown system for the manager works correctly, to wit:
     * <ol>
     * <li>The thread pool is shut down and nullified.
     * </ol>
     */
    @Test
    public void testShutdown() {

        taskRequestLoopThread.start();
        lifecycleManager.addTaskRequestToQueue(new TaskRequest(1L, 1L, -1L, 1L,
            PipelineInstance.Priority.NORMAL, false, PipelineModule.RunMode.STANDARD));
        TestEventDetector.detectTestEvent(500L,
            () -> lifecycleManager.getTaskRequestHandlers().size() > 0);
        ExecutorService executorService = lifecycleManager.getTaskRequestThreadPool();
        lifecycleManager.shutdown();

        // The thread pool should be shut down and nullified.
        assertTrue(TestEventDetector.detectTestEvent(500L,
            () -> lifecycleManager.getTaskRequestThreadPool() == null));
        assertTrue(executorService.isShutdown());
    }

    /**
     * Tests the transition from one pipeline definition node ID to another. Specifically:
     * <ol>
     * <li>The original thread pool is shut down and replaced.
     * <li>The new thread pool has the correct number of threads.
     * <li>All the task requests have been picked up by a task request handler.
     * <li>Each task is picked up by the correct task request handler.
     * <li>The lifecycle manager's pipeline definition node ID is updated.
     * <ol>
     */
    @Test
    public void testDefinitionNodeTransition() {

        taskRequestLoopThread.start();
        List<List<TaskRequestHandler>> allTaskRequestHandlers = lifecycleManager
            .getTaskRequestHandlers();

        lifecycleManager.addTaskRequestToQueue(new TaskRequest(1L, 1L, -1L, 1L,
            PipelineInstance.Priority.NORMAL, false, PipelineModule.RunMode.STANDARD));
        TestEventDetector.detectTestEvent(500L, () -> allTaskRequestHandlers.size() > 0);

        ExecutorService threadPool = lifecycleManager.getTaskRequestThreadPool();
        assertTrue(TestEventDetector.detectTestEvent(500L,
            () -> lifecycleManager.getPipelineDefinitionNodeId() == -1L));

        // Create some task requests
        TaskRequest t1 = new TaskRequest(0, 0, -2, 1, Priority.NORMAL, false, RunMode.STANDARD);
        TaskRequest t2 = new TaskRequest(0, 0, -2, 2, Priority.NORMAL, false, RunMode.STANDARD);
        TaskRequest t3 = new TaskRequest(0, 0, -2, 3, Priority.NORMAL, false, RunMode.STANDARD);

        lifecycleManager.setMaxWorkers(2);

        // Pop the task requests into the queue
        lifecycleManager.addTaskRequestToQueue(t1);
        lifecycleManager.addTaskRequestToQueue(t2);
        lifecycleManager.addTaskRequestToQueue(t3);

        // Wait for all the tasks to get pulled into task request dispatchers
        assertTrue(
            TestEventDetector.detectTestEvent(500L, () -> lifecycleManager.taskRequestSize() == 0));

        // The definition node ID should have changed.
        assertTrue(TestEventDetector.detectTestEvent(500L,
            () -> lifecycleManager.getPipelineDefinitionNodeId() != null
                && lifecycleManager.getPipelineDefinitionNodeId() == -2L));

        // The thread pool executor should be different.
        assertTrue(TestEventDetector.detectTestEvent(100L,
            () -> threadPool != lifecycleManager.getTaskRequestThreadPool()));

        // Get the ensemble of TaskRequestHandler instances.
        assertTrue(
            TestEventDetector.detectTestEvent(500L, () -> allTaskRequestHandlers.size() == 2));

        // The second list should have 2 handlers which between them handled 3 task
        // requests, the ones for pipeline definition node ID == -2.
        List<TaskRequestHandler> handlers = allTaskRequestHandlers.get(1);
        assertEquals(2, handlers.size());
        final List<TaskRequestHandler> finalHandlers = new ArrayList<>(handlers);
        assertTrue(
            TestEventDetector.detectTestEvent(500L, () -> taskRequestCount(finalHandlers) == 3));

        Set<TaskRequest> taskRequests = new HashSet<>(handlers.get(0).getTaskRequests());
        taskRequests.addAll(handlers.get(1).getTaskRequests());
        assertTrue(taskRequests.contains(t1));
        assertTrue(taskRequests.contains(t2));
        assertTrue(taskRequests.contains(t3));

        // The first list should have 1 handler which handled 1 task request,
        // the first request.
        handlers = allTaskRequestHandlers.get(0);
        assertEquals(1, handlers.size());
        taskRequests = handlers.get(0).getTaskRequests();
        assertEquals(1, taskRequests.size());
        TaskRequest taskRequest = taskRequests.iterator().next();
        assertEquals(-1L, taskRequest.getPipelineDefinitionNodeId());
    }

    private int taskRequestCount(List<TaskRequestHandler> taskRequestHandlers) {
        int taskRequestCount = 0;
        for (TaskRequestHandler handler : taskRequestHandlers) {
            taskRequestCount += handler.getTaskRequests().size();
        }
        return taskRequestCount;
    }

    /**
     * Tests removal of task requests from the queue:
     * <ol>
     * <li>Task IDs in the request list that aren't in the task request queue are ignored.
     * <li>Only tasks that are common to the task request queue and the task ID list are removed
     * from the queue.
     * </ol>
     */
    @Test
    public void testRemoveRequestsFromQueue() {

        lifecycleManager = Mockito.spy(lifecycleManager);
        // Shut off the task request handler threads so that the tasks don't instantly fly
        // out of the queue.
        assertTrue(
            TestEventDetector.detectTestEvent(500L, () -> lifecycleManager.taskRequestSize() == 0));

        // Put some tasks into the queue.
        lifecycleManager.addTaskRequestToQueue(
            new TaskRequest(0, 0, -2, 1, Priority.NORMAL, false, RunMode.STANDARD));
        lifecycleManager.addTaskRequestToQueue(
            new TaskRequest(0, 0, -2, 2, Priority.NORMAL, false, RunMode.STANDARD));
        lifecycleManager.addTaskRequestToQueue(
            new TaskRequest(0, 0, -2, 3, Priority.NORMAL, false, RunMode.STANDARD));

        // Check that the tasks stayed put
        assertEquals(3, lifecycleManager.taskRequestSize());

        KillTasksRequest request = KillTasksRequest.forTaskIds(lifecycleManager,
            List.of(1L, 3L, 4L));
        lifecycleManager.killQueuedTasksAction(request);
        assertEquals(1, lifecycleManager.taskRequestSize());
        assertEquals(2, lifecycleManager.taskRequestQueuePeek().getTaskId());
        Mockito.verify(lifecycleManager).publishKilledTaskMessage(request, 1L);
        Mockito.verify(lifecycleManager, times(0)).publishKilledTaskMessage(request, 2L);
        Mockito.verify(lifecycleManager).publishKilledTaskMessage(request, 3L);
        Mockito.verify(lifecycleManager, times(0)).publishKilledTaskMessage(request, 4L);
    }

    /**
     * Subclass of {@link TaskRequestHandlerLifecycleManager} that allows the user to set the number
     * of workers to be provided the next time the workers get destroyed and recreated.
     *
     * @author PT
     */
    public static class InstrumentedTaskRequestHandlerLifecycleManager
        extends TaskRequestHandlerLifecycleManager implements Requestor {

        private int maxWorkers;
        private PipelineTaskCrud pipelineTaskCrud;
        private PipelineOperations pipelineOperations;

        public InstrumentedTaskRequestHandlerLifecycleManager() {
            super(true);
            TaskRequestHandlerLifecycleManager.setInstance(this);
            pipelineTaskCrud = Mockito.mock(PipelineTaskCrud.class);
            Mockito.when(pipelineTaskCrud.retrieveAll(ArgumentMatchers.<Long> anyList()))
                .thenReturn(new ArrayList<>());
            pipelineOperations = Mockito.spy(PipelineOperations.class);
        }

        public void setMaxWorkers(int maxWorkers) {
            this.maxWorkers = maxWorkers;
        }

        @Override
        WorkerResources workerResources(TaskRequest taskRequest) {
            return new WorkerResources(maxWorkers, 1);
        }

        @Override
        protected PipelineTaskCrud pipelineTaskCrud() {
            return pipelineTaskCrud;
        }

        @Override
        protected PipelineExecutor pipelineExecutor() {
            return Mockito.mock(PipelineExecutor.class);
        }

        @Override
        protected AlertService alertService() {
            return Mockito.mock(AlertService.class);
        }

        @Override
        protected PipelineOperations pipelineOperations() {
            return pipelineOperations;
        }

        @Override
        public Object requestorIdentifier() {
            return Long.valueOf(0L);
        }
    }
}
