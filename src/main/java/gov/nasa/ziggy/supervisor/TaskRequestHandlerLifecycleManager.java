package gov.nasa.ziggy.supervisor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.services.messages.KilledTaskMessage;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.services.messages.WorkerResourcesMessage;
import gov.nasa.ziggy.services.messages.WorkerResourcesRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.worker.WorkerResources;

/**
 * Manages instances of {@link TaskRequestHandler} throughout their lifecycle.
 * <p>
 * The initial state of the pipeline is one in which there are no {@link TaskRequestHandler}s, but
 * there is a method that blocks until a {@link TaskRequest} arrives. At that time the class
 * instantiates a number of {@link TaskRequestHandler}s based on the worker count specified for the
 * pipeline definition node of the task. Those {@link TaskRequestHandler}s continue to run until the
 * tasks for that pipeline definition node have all completed execution. At that time, the
 * {@link TaskRequestHandler}s are shut down and the class resumes waiting for new task request.
 * <p>
 * The {@link TaskRequestHandlerLifecycleManager} is meant to be a singleton instance. One and only
 * one instance should be constructed and stored in {@link PipelineSupervisor}.
 *
 * @author PT
 */
public class TaskRequestHandlerLifecycleManager extends Thread {

    private static final Logger log = LoggerFactory
        .getLogger(TaskRequestHandlerLifecycleManager.class);

    /** Singleton instance. */
    protected static TaskRequestHandlerLifecycleManager instance;

    private CountDownLatch taskRequestThreadCountdownLatch;
    private final PriorityBlockingQueue<TaskRequest> taskRequestQueue = new PriorityBlockingQueue<>();

    // Use a wrapper class so it can be null.
    private Long pipelineDefinitionNodeId;

    private ExecutorService taskRequestThreadPool;

    // The current actual resources available to the current node, including defaults if
    // appropriate.
    private WorkerResources workerResources = new WorkerResources(0, 0);

    // For testing purposes, the TaskRequestDispatcher can optionally store all of the
    // task request handlers it creates, organized by thread pool instance.
    private List<List<TaskRequestHandler>> taskRequestHandlers = new ArrayList<>();
    private final boolean storeTaskRequestHandlers;

    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    private TaskRequestHandlerLifecycleManager() {
        this(false);
    }

    protected TaskRequestHandlerLifecycleManager(boolean storeTaskRequestHandlers) {
        this.storeTaskRequestHandlers = storeTaskRequestHandlers;

        // Subscribe to task request messages.
        ZiggyMessenger.subscribe(TaskRequest.class, message -> {
            taskRequestQueue.put(message);
        });

        // Subscribe to kill tasks messages.
        ZiggyMessenger.subscribe(KillTasksRequest.class, message -> {
            killQueuedTasksAction(message);
        });

        // Subscribe to requests for the current worker resources. This allows the
        // console to find out what's currently running in the event that the console
        // starts up when a pipeline is already executing.
        ZiggyMessenger.subscribe(WorkerResourcesRequest.class, message -> {
            ZiggyMessenger.publish(new WorkerResourcesMessage(null, workerResources));
        });
    }

    /**
     * Constructs, initializes, and starts the singleton instance of
     * {@link TaskRequestHandlerLifecycleManager}.
     */
    public static synchronized void initializeInstance() {
        if (instance != null) {
            return;
        }
        instance = new TaskRequestHandlerLifecycleManager();
        instance.setDaemon(true);
        instance.start();
    }

    /**
     * Performs the deletion of queued tasks. This consists of removing the task requests from the
     * task request queue and setting the error flag of the removed tasks. The IDs of all tasks
     * listed in the {@link KillTasksRequest} are added to the supervisor's list of killed task IDs.
     * This avoids the need to capture the IDs from the worker, the supervisor, and the batch
     * queues.
     * <p>
     * This method is package scoped to facilitate testing.
     */
    void killQueuedTasksAction(KillTasksRequest request) {
        List<Long> taskIds = request.getTaskIds();

        // Locate the tasks that are queued and which are in the kill request.
        log.info("Halting queued tasks: {}", taskIds);
        List<TaskRequest> tasksForDeletion = new ArrayList<>();
        Set<Long> taskIdsForDeletion = new HashSet<>();
        for (TaskRequest taskRequest : taskRequestQueue) {
            if (taskIds.contains(taskRequest.getTaskId())) {
                tasksForDeletion.add(taskRequest);
                taskIdsForDeletion.add(taskRequest.getTaskId());
            }
        }

        // Take the selected tasks out of the queue.
        taskRequestQueue.removeAll(tasksForDeletion);

        // Removing a task from the queue is easy, so we can just assume this was successful
        // and publish the success messages.
        for (TaskRequest taskRequest : tasksForDeletion) {
            publishKilledTaskMessage(request, taskRequest.getTaskId());
        }
    }

    /**
     * Publishes the {@link KilledTaskMessage}.
     */
    // TODO Inline
    // This requires that the ZiggyMessenger.publish() call can be verified by Mockito.
    // The KillTaskMessage.timeSent field makes it difficult.
    void publishKilledTaskMessage(KillTasksRequest request, long taskId) {
        ZiggyMessenger.publish(new KilledTaskMessage(request, taskId));
    }

    protected PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    protected AlertService alertService() {
        return AlertService.getInstance();
    }

    /**
     * Starts the {@link TaskRequestHandlerLifecycleManager}. Specifically, adds a shutdown hook so
     * that when the supervisor is shut down the task request handler threads can be shut down as
     * well, then kicks off the loop that watches for task requests and creates task request handler
     * instances.
     */
    @Override
    public void run() {
        ZiggyShutdownHook.addShutdownHook(() -> {
            shutdown();
        });

        startLifecycle();
    }

    /**
     * Performs the main loop of the class that waits for the {@link TaskRequestWatchdog} to detect
     * a task request, starts the {@link TaskRequestHandler}s, and waits for the
     * {@link TaskRequestHandler}s to exit.
     */
    private void startLifecycle() {

        try {
            while (!Thread.currentThread().isInterrupted()) {

                // Wait for a task request to arrive.
                TaskRequest initialRequest = taskRequestQueue.take();

                // Set the pipeline definition node that all the workers are supposed
                // to be handling.
                log.info("Setting supported pipeline definition node ID to {}",
                    initialRequest.getPipelineDefinitionNodeId());
                pipelineDefinitionNodeId = initialRequest.getPipelineDefinitionNodeId();

                workerResources = workerResources(initialRequest);
                int workerCount = workerResources.getMaxWorkerCount();
                int heapSizeMb = workerResources.getHeapSizeMb();

                ZiggyMessenger.publish(new WorkerResourcesMessage(null, workerResources));

                // Put the task back onto the queue.
                taskRequestQueue.put(initialRequest);

                // Construct the TaskRequestHandler threads and the countdown latch.
                taskRequestThreadCountdownLatch = new CountDownLatch(workerCount);
                if (workerCount < 1) {
                    throw new PipelineException("worker count < 1");
                }
                log.info("Starting {} workers with total heap size {}", workerCount,
                    workerResources.humanReadableHeapSize().toString());
                taskRequestThreadPool = Executors.newFixedThreadPool(workerCount);
                List<TaskRequestHandler> handlers = new ArrayList<>();
                for (int i = 0; i < workerCount; i++) {
                    log.info("Starting worker # {} of {}", i + 1, workerCount);
                    TaskRequestHandler handler = new TaskRequestHandler(i + 1, workerCount,
                        heapSizeMb, taskRequestQueue, pipelineDefinitionNodeId,
                        taskRequestThreadCountdownLatch);
                    taskRequestThreadPool.submit(handler);
                    handlers.add(handler);
                }
                if (storeTaskRequestHandlers) {
                    taskRequestHandlers.add(handlers);
                }

                // Wait for the countdown latch to hit zero, then loop back to
                // process tasks for some other pipeline instance node.
                taskRequestThreadCountdownLatch.await();
            }
        } catch (InterruptedException e) {
            shutdown();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Shuts down the {@link TaskRequestHandler} threads and discards the thread pool. This is used
     * by the shutdown hook and thus must have public scope.
     */
    public void shutdown() {
        taskRequestQueue.clear();
        if (taskRequestThreadPool != null) {
            taskRequestThreadPool.shutdownNow();
        }
        taskRequestThreadPool = null;
    }

    /**
     * Gets the actual resources for the current pipeline definition node, including default values
     * as appropriate.
     */
    WorkerResources workerResources(TaskRequest taskRequest) {
        WorkerResources databaseResources = pipelineTaskOperations()
            .workerResourcesForTask(taskRequest.getTaskId());
        Integer compositeWorkerCount = databaseResources.getMaxWorkerCount() != null
            ? databaseResources.getMaxWorkerCount()
            : PipelineSupervisor.defaultResources().getMaxWorkerCount();
        Integer compositeHeapSizeMb = databaseResources.getHeapSizeMb() != null
            ? databaseResources.getHeapSizeMb()
            : PipelineSupervisor.defaultResources().getHeapSizeMb();
        return new WorkerResources(compositeWorkerCount, compositeHeapSizeMb);
    }

    /** For testing only. */
    static void setInstance(TaskRequestHandlerLifecycleManager instance) {
        TaskRequestHandlerLifecycleManager.instance = instance;
    }

    /** For testing only. */
    List<List<TaskRequestHandler>> getTaskRequestHandlers() {
        return taskRequestHandlers;
    }

    /** For testing only. */
    ExecutorService getTaskRequestThreadPool() {
        return taskRequestThreadPool;
    }

    /** For testing only. */
    CountDownLatch getTaskRequestThreadCountdownLatch() {
        return taskRequestThreadCountdownLatch;
    }

    /**
     * For testing only. Public scope because the {@code AlgorithmMonitorTest} needs to see whether
     * the task requests have been properly managed.
     */
    public int taskRequestSize() {
        return taskRequestQueue.size();
    }

    /** For testing only. */
    TaskRequest taskRequestQueuePeek() {
        return taskRequestQueue.peek();
    }

    /** For testing only. */
    void addTaskRequestToQueue(TaskRequest request) {
        taskRequestQueue.add(request);
    }

    /** For testing only. */
    Long getPipelineDefinitionNodeId() {
        return pipelineDefinitionNodeId;
    }
}
