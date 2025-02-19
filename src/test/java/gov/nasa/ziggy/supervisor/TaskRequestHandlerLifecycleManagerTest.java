package gov.nasa.ziggy.supervisor;

import static gov.nasa.ziggy.TestEventDetector.detectTestEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.FlakyTestCategory;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.HaltTasksRequest;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.worker.WorkerResources;

/**
 * Unit tests for {@link TaskRequestHandlerLifecycleManager} class.
 *
 * @author PT
 * @author Bill Wohler
 */
public class TaskRequestHandlerLifecycleManagerTest {

    private InstrumentedTaskRequestHandlerLifecycleManager lifecycleManager;
    private Thread taskRequestLoopThread;

    private PipelineTask pipelineTask1;
    private PipelineTask pipelineTask2;
    private PipelineTask pipelineTask3;
    private PipelineTask pipelineTask4;

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

        pipelineTask1 = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(1L).when(pipelineTask1).getId();
        pipelineTask2 = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(2L).when(pipelineTask2).getId();
        pipelineTask3 = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(3L).when(pipelineTask3).getId();
        pipelineTask4 = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(4L).when(pipelineTask4).getId();
    }

    @After
    public void tearDown() throws InterruptedException {
        lifecycleManager.shutdown();
        taskRequestLoopThread.interrupt();
        assertTrue("pool=" + lifecycleManager.getTaskRequestThreadPool(),
            detectTestEvent(500L, () -> lifecycleManager.getTaskRequestThreadPool() == null));
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

        Queue<List<TaskRequestHandler>> allTaskRequestHandlers = lifecycleManager
            .getTaskRequestHandlers();
        taskRequestLoopThread.start();

        // Right out of the box there should be no task request handlers and no executor
        // for handlers.
        assertEquals(0, allTaskRequestHandlers.size());
        assertNull(lifecycleManager.getTaskRequestThreadCountdownLatch());
        assertEquals(0, lifecycleManager.taskRequestSize());
        assertNull(lifecycleManager.getPipelineDefinitionNodeId());

        // Add a task to the queue and wait for it to get handled.
        lifecycleManager.addTaskRequestToQueue(new TaskRequest(1L, 1L, -1L, pipelineTask1,
            PipelineInstance.Priority.NORMAL, false, PipelineModule.RunMode.STANDARD));
        assertTrue("size=" + allTaskRequestHandlers.size(),
            detectTestEvent(500L, () -> allTaskRequestHandlers.size() > 0));

        // There should be one set of task request handlers constructed, with 1 handler in the set.
        assertEquals(1, allTaskRequestHandlers.size());
        List<TaskRequestHandler> taskRequestHandlers = allTaskRequestHandlers.poll();
        assertEquals(1, taskRequestHandlers.size());
        TaskRequestHandler taskRequestHandler = taskRequestHandlers.get(0);

        // The handler should have handled 1 task request.

        assertTrue("size=" + taskRequestHandler.getTaskRequests().size(),
            detectTestEvent(500L, () -> taskRequestHandler.getTaskRequests().size() > 0));
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
        lifecycleManager.addTaskRequestToQueue(new TaskRequest(1L, 1L, -1L, pipelineTask1,
            PipelineInstance.Priority.NORMAL, false, PipelineModule.RunMode.STANDARD));
        assertTrue("size=" + lifecycleManager.getTaskRequestHandlers().size(),
            detectTestEvent(500L, () -> lifecycleManager.getTaskRequestHandlers().size() > 0));
        ExecutorService executorService = lifecycleManager.getTaskRequestThreadPool();
        lifecycleManager.shutdown();

        // The thread pool should be shut down and nullified.
        assertTrue(
            detectTestEvent(500L, () -> lifecycleManager.getTaskRequestThreadPool() == null));
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
    @Category(FlakyTestCategory.class)
    @Test
    public void testDefinitionNodeTransition() {

        taskRequestLoopThread.start();
        Queue<List<TaskRequestHandler>> allTaskRequestHandlers = lifecycleManager
            .getTaskRequestHandlers();

        lifecycleManager.addTaskRequestToQueue(new TaskRequest(1L, 1L, -1L, pipelineTask1,
            PipelineInstance.Priority.NORMAL, false, PipelineModule.RunMode.STANDARD));
        assertTrue("size=" + allTaskRequestHandlers.size(),
            detectTestEvent(500L, () -> allTaskRequestHandlers.size() > 0));

        ExecutorService threadPool = lifecycleManager.getTaskRequestThreadPool();
        assertTrue(
            detectTestEvent(500L, () -> lifecycleManager.getPipelineDefinitionNodeId() == -1L));

        // Create some task requests
        TaskRequest t1 = new TaskRequest(0, 0, -2, pipelineTask1, Priority.NORMAL, false,
            RunMode.STANDARD);
        TaskRequest t2 = new TaskRequest(0, 0, -2, pipelineTask2, Priority.NORMAL, false,
            RunMode.STANDARD);
        TaskRequest t3 = new TaskRequest(0, 0, -2, pipelineTask3, Priority.NORMAL, false,
            RunMode.STANDARD);

        lifecycleManager.setMaxWorkers(2);

        // Pop the task requests into the queue
        lifecycleManager.addTaskRequestToQueue(t1);
        lifecycleManager.addTaskRequestToQueue(t2);
        lifecycleManager.addTaskRequestToQueue(t3);

        // Wait for all the tasks to get pulled into task request dispatchers
        assertTrue(detectTestEvent(500L, () -> lifecycleManager.taskRequestSize() == 0));

        // The definition node ID should have changed.
        assertTrue(
            detectTestEvent(500L, () -> lifecycleManager.getPipelineDefinitionNodeId() != null
                && lifecycleManager.getPipelineDefinitionNodeId() == -2L));

        // The thread pool executor should be the same.
        assertTrue(
            detectTestEvent(100L, () -> threadPool == lifecycleManager.getTaskRequestThreadPool()));

        // Get the ensemble of TaskRequestHandler instances.
        assertTrue(detectTestEvent(500L, () -> allTaskRequestHandlers.size() == 2));

        // The first list should have 1 handler which handled 1 task request,
        // the first request.
        List<TaskRequestHandler> handlers = allTaskRequestHandlers.poll();
        assertEquals(1, handlers.size());
        Set<TaskRequest> taskRequests = handlers.get(0).getTaskRequests();
        assertEquals(1, taskRequests.size());
        TaskRequest taskRequest = taskRequests.iterator().next();
        assertEquals(-1L, taskRequest.getPipelineDefinitionNodeId());

        // The second list should have 2 handlers which between them handled 3 task
        // requests, the ones for pipeline definition node ID == -2.
        handlers = allTaskRequestHandlers.poll();
        assertEquals(2, handlers.size());
        final List<TaskRequestHandler> finalHandlers = new ArrayList<>(handlers);
        assertTrue(detectTestEvent(500L, () -> taskRequestCount(finalHandlers) == 3));

        taskRequests = new HashSet<>(handlers.get(0).getTaskRequests());
        taskRequests.addAll(handlers.get(1).getTaskRequests());
        assertTrue(taskRequests.contains(t1));
        assertTrue(taskRequests.contains(t2));
        assertTrue(taskRequests.contains(t3));
    }

    private int taskRequestCount(List<TaskRequestHandler> taskRequestHandlers) {
        int taskRequestCount = 0;
        for (TaskRequestHandler handler : taskRequestHandlers) {
            taskRequestCount += handler.getTaskRequests().size();
        }
        return taskRequestCount;
    }

    @Test
    public void testUpdateThreadsForSameModule() {

        taskRequestLoopThread.start();
        Queue<List<TaskRequestHandler>> allTaskRequestHandlers = lifecycleManager
            .getTaskRequestHandlers();

        lifecycleManager.addTaskRequestToQueue(new TaskRequest(1L, 1L, -1L, pipelineTask1,
            PipelineInstance.Priority.NORMAL, false, PipelineModule.RunMode.STANDARD));
        assertTrue("size=" + allTaskRequestHandlers.size(),
            detectTestEvent(50L, () -> allTaskRequestHandlers.size() == 1));

        // Wait for all the tasks to get pulled into task request dispatchers
        assertTrue("size=" + lifecycleManager.taskRequestSize(),
            detectTestEvent(50L, () -> lifecycleManager.taskRequestSize() == 0));

        // Create some task requests
        TaskRequest t1 = new TaskRequest(2L, 1L, -1L, pipelineTask1, Priority.NORMAL, false,
            RunMode.STANDARD);
        TaskRequest t2 = new TaskRequest(2L, 1L, -1L, pipelineTask2, Priority.NORMAL, false,
            RunMode.STANDARD);
        TaskRequest t3 = new TaskRequest(2L, 1L, -1L, pipelineTask3, Priority.NORMAL, false,
            RunMode.STANDARD);

        lifecycleManager.setMaxWorkers(3);
        lifecycleManager.handlePipelineInstanceFinishedMessage(null);

        // Pop the task requests into the queue
        lifecycleManager.addTaskRequestToQueue(t1);
        lifecycleManager.addTaskRequestToQueue(t2);
        lifecycleManager.addTaskRequestToQueue(t3);

        // Wait for all the tasks to get pulled into task request dispatchers
        assertTrue("size=" + lifecycleManager.taskRequestSize(),
            detectTestEvent(50L, () -> lifecycleManager.taskRequestSize() == 0));

        // Get the ensemble of TaskRequestHandler instances.
        assertTrue("size=" + allTaskRequestHandlers.size(),
            detectTestEvent(50L, () -> allTaskRequestHandlers.size() == 2));

        // The second list should have 3 handlers which between them handled 3 task
        // requests, the ones for pipeline definition node ID == -1.
        allTaskRequestHandlers.poll(); // discard
        List<TaskRequestHandler> handlers = allTaskRequestHandlers.poll();
        assertEquals(3, handlers.size());
        assertTrue("count=" + taskRequestCount(handlers),
            detectTestEvent(500L, () -> taskRequestCount(handlers) == 3));
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
        assertTrue(detectTestEvent(500L, () -> lifecycleManager.taskRequestSize() == 0));

        // Put some tasks into the queue.
        TaskRequest taskRequest = new TaskRequest(0, 0, -2, pipelineTask1, Priority.NORMAL, false,
            RunMode.STANDARD);
        lifecycleManager.addTaskRequestToQueue(taskRequest);
        taskRequest = new TaskRequest(0, 0, -2, pipelineTask2, Priority.NORMAL, false,
            RunMode.STANDARD);
        lifecycleManager.addTaskRequestToQueue(taskRequest);
        taskRequest = new TaskRequest(0, 0, -2, pipelineTask3, Priority.NORMAL, false,
            RunMode.STANDARD);
        lifecycleManager.addTaskRequestToQueue(taskRequest);

        // Check that the tasks stayed put
        assertEquals(3, lifecycleManager.taskRequestSize());

        List<PipelineTask> pipelineTasks = List.of(pipelineTask1, pipelineTask3, pipelineTask4);
        HaltTasksRequest request = new HaltTasksRequest(pipelineTasks);
        lifecycleManager.haltQueuedTasksAction(request);
        assertEquals(1, lifecycleManager.taskRequestSize());
        assertEquals(pipelineTask2, lifecycleManager.taskRequestQueuePeek().getPipelineTask());
        Mockito.verify(lifecycleManager).publishTaskHaltedMessage(pipelineTask1);
        Mockito.verify(lifecycleManager, times(0)).publishTaskHaltedMessage(pipelineTask2);
        Mockito.verify(lifecycleManager).publishTaskHaltedMessage(pipelineTask3);
        Mockito.verify(lifecycleManager, times(0)).publishTaskHaltedMessage(pipelineTask4);
    }

    @Test
    public void testHandleQueuedTasksAction() {

        lifecycleManager = Mockito.spy(lifecycleManager);
        // Shut off the task request handler threads so that the tasks don't instantly fly
        // out of the queue.
        assertTrue(detectTestEvent(500L, () -> lifecycleManager.taskRequestSize() == 0));

        // Put some tasks into the queue.
        TaskRequest taskRequest1 = new TaskRequest(0, 0, -2, pipelineTask1, Priority.NORMAL, false,
            RunMode.STANDARD);
        TaskRequest taskRequest2 = new TaskRequest(0, 0, -2, pipelineTask2, Priority.NORMAL, false,
            RunMode.STANDARD);
        Mockito.when(lifecycleManager.pipelineTaskDataOperations().haltRequested(pipelineTask1))
            .thenReturn(false);
        Mockito.when(lifecycleManager.pipelineTaskDataOperations().haltRequested(pipelineTask2))
            .thenReturn(true);
        lifecycleManager.handleTaskRequestAction(taskRequest1);
        lifecycleManager.handleTaskRequestAction(taskRequest2);
        assertEquals(1, lifecycleManager.taskRequestSize());
        assertEquals(pipelineTask1, lifecycleManager.taskRequestQueuePeek().getPipelineTask());
        Mockito.verify(lifecycleManager).publishTaskHaltedMessage(pipelineTask2);
        Mockito.verify(lifecycleManager, times(0)).publishTaskHaltedMessage(pipelineTask1);
    }

    /**
     * Subclass of {@link TaskRequestHandlerLifecycleManager} that allows the user to set the number
     * of workers to be provided the next time the workers get destroyed and recreated.
     *
     * @author PT
     */
    public static class InstrumentedTaskRequestHandlerLifecycleManager
        extends TaskRequestHandlerLifecycleManager {

        private int maxWorkers;
        private PipelineTaskCrud pipelineTaskCrud;
        private PipelineTaskOperations mockedPipelineTaskOperations;
        private PipelineTaskDataOperations mockedPipelineTaskDataOperations;

        public InstrumentedTaskRequestHandlerLifecycleManager() {
            super(true);
            TaskRequestHandlerLifecycleManager.setInstance(this);
            pipelineTaskCrud = Mockito.mock(PipelineTaskCrud.class);
            Mockito.when(pipelineTaskCrud.retrieveAll(ArgumentMatchers.<Long> anyList()))
                .thenReturn(new ArrayList<>());
            mockedPipelineTaskOperations = Mockito.spy(PipelineTaskOperations.class);
            mockedPipelineTaskDataOperations = Mockito.mock(PipelineTaskDataOperations.class);
        }

        public void setMaxWorkers(int maxWorkers) {
            this.maxWorkers = maxWorkers;
        }

        @Override
        WorkerResources workerResources(TaskRequest taskRequest) {
            return new WorkerResources(maxWorkers, 1);
        }

        @Override
        protected AlertService alertService() {
            return Mockito.mock(AlertService.class);
        }

        @Override
        protected PipelineTaskOperations pipelineTaskOperations() {
            return mockedPipelineTaskOperations;
        }

        @Override
        public PipelineTaskDataOperations pipelineTaskDataOperations() {
            return mockedPipelineTaskDataOperations;
        }
    }
}
