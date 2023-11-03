package gov.nasa.ziggy.supervisor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.services.messages.WorkerResources;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

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

    /** Singleton instance */
    private static TaskRequestHandlerLifecycleManager instance;

    private CountDownLatch taskRequestThreadCountdownLatch;
    private final PriorityBlockingQueue<TaskRequest> taskRequestQueue = new PriorityBlockingQueue<>();
    private final Set<Long> killedTaskIds = ConcurrentHashMap.newKeySet();

    // Use a boxed instance so it can be null.
    private Long pipelineDefinitionNodeId;

    private ExecutorService taskRequestThreadPool;

    // For testing purposes, the TaskRequestDispatcher can optionally store all of the
    // task request handlers it creates, organized by thread pool instance.
    private List<List<TaskRequestHandler>> taskRequestHandlers = new ArrayList<>();
    private final boolean storeTaskRequestHandlers;

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
            killQueuedTasksAction(message.getTaskIds());
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
     * task request queue and setting the state of all tasks thus removed to ERROR. The IDs of all
     * tasks listed in the {@link KillTasksRequest} are added to the supervisor's list of killed
     * task IDs. This avoids the need to capture the IDs from the worker, the supervisor, and the
     * batch queues.
     * <p>
     * This method is package scoped to facilitate testing.
     */
    void killQueuedTasksAction(List<Long> taskIds) {
        // Add the IDs to the supervisor's list of task IDs for use later when the tasks already
        // in workers are killed
        killedTaskIds.addAll(taskIds);
        log.info("Deleting tasks: {}", taskIds);
        List<TaskRequest> tasksForDeletion = new ArrayList<>();
        Set<Long> taskIdsForDeletion = new HashSet<>();
        for (TaskRequest request : taskRequestQueue) {
            if (taskIds.contains(request.getTaskId())) {
                tasksForDeletion.add(request);
                taskIdsForDeletion.add(request.getTaskId());
            }
        }
        taskRequestQueue.removeAll(tasksForDeletion);

        // For tasks that were waiting to run, i.e. in the submitted state, we need to manually
        // set them to ERRORed in the database and update all counts and instance states.
        if (!taskIdsForDeletion.isEmpty()) {
            DatabaseTransactionFactory.performTransaction(() -> {
                PipelineTaskCrud pipelineTaskCrud = pipelineTaskCrud();
                PipelineOperations pipelineOperations = pipelineOperations();
                List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll(taskIdsForDeletion);
                for (PipelineTask task : tasks) {
                    pipelineOperations.setTaskState(task, PipelineTask.State.ERROR);
                }
                return null;
            });

            // Issue alerts, too!
            for (long taskId : taskIdsForDeletion) {
                alertService().generateAndBroadcastAlert("PI", taskId, AlertService.Severity.ERROR,
                    "Task " + taskId + " deleted from processing queue.");
            }
        }
    }

    /** CRUD class constructor. Broken out to facilitate testing. */
    protected PipelineTaskCrud pipelineTaskCrud() {
        return new PipelineTaskCrud();
    }

    /** Executor class constructor. Broken out to facilitate testing. */
    protected PipelineExecutor pipelineExecutor() {
        return new PipelineExecutor();
    }

    /** PipelineOperations class constructor. Broken out to facilitate testing. */
    protected PipelineOperations pipelineOperations() {
        return new PipelineOperations();
    }

    /** Alert service class accessor. Broken out to facilitate testing. */
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

                WorkerResources workerResources = workerResources(initialRequest);
                int workerCount = workerResources.getMaxWorkerCount();
                int heapSizeMb = workerResources.getHeapSizeMb();

                // Put the task back onto the queue.
                taskRequestQueue.put(initialRequest);

                // Construct the TaskRequestHandler threads and the countdown latch.
                taskRequestThreadCountdownLatch = new CountDownLatch(workerCount);
                if (workerCount < 1) {
                    throw new PipelineException("worker count < 1");
                }
                log.info("Starting {} workers", workerCount);
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

                // Wait for the countdown latch to hit zero.
                taskRequestThreadCountdownLatch.await();

                // Discard the existing thread pool and loop back to the top to wait for
                // a new task request.
                pipelineDefinitionNodeId = null;
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

    WorkerResources workerResources(TaskRequest taskRequest) {
        return (WorkerResources) DatabaseTransactionFactory
            .performTransaction(() -> new PipelineTaskCrud().retrieve(taskRequest.getTaskId())
                .getPipelineDefinitionNode()
                .workerResources());
    }

    public static boolean taskOnKilledTaskList(long taskId) {
        return instance.killedTaskIds.contains(taskId);
    }

    public static void removeTaskFromKilledTaskList(long taskId) {
        instance.killedTaskIds.remove(taskId);
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
     * For testing only. Public scope because the {@link AlgorithmMonitorTest} needs to see whether
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
