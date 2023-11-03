package gov.nasa.ziggy.supervisor;

import static gov.nasa.ziggy.module.AlgorithmExecutor.ZIGGY_PROGRAM;

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.exec.CommandLine;
import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.services.process.ExternalProcessUtils;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.Requestor;
import gov.nasa.ziggy.worker.PipelineWorker;

/**
 * Interacts with the worker task queue to handle task requests. Hands off incoming messages to a
 * {@link PipelineWorker} to execute.
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 * @author PT
 */
public class TaskRequestHandler implements Runnable, Requestor {
    private static final Logger log = LoggerFactory.getLogger(TaskRequestHandler.class);

    private final int workerId;
    private final int workerCount;
    private final int heapSizeMb;
    private final PriorityBlockingQueue<TaskRequest> taskRequestQueue;
    private final UUID uuid = UUID.randomUUID();
    private final long pipelineDefinitionNodeId;
    private final CountDownLatch handlerShutdownCountdownLatch;

    /** For testing only. */
    private final Set<TaskRequest> taskRequests = new TreeSet<>();

    public TaskRequestHandler(int workerId, int workerCount, int heapSizeMb,
        PriorityBlockingQueue<TaskRequest> taskRequestQueue, long pipelineDefinitionNodeId,
        CountDownLatch handlerShutdownCountdownLatch) {
        this.workerId = workerId;
        this.workerCount = workerCount;
        this.heapSizeMb = heapSizeMb;
        this.taskRequestQueue = taskRequestQueue;
        this.pipelineDefinitionNodeId = pipelineDefinitionNodeId;
        this.handlerShutdownCountdownLatch = handlerShutdownCountdownLatch;
    }

    /**
     * Waits for a new task to become available on the supervisor's task queue. When the task
     * becomes available, creates a {@link PipelineWorker} in an {@link ExternalProcess} to process
     * the task.
     * <p>
     * The {@link TaskRequestHandler} instance lasts for as long as a given pipeline module is
     * processing. At the end of processing a module, all the worker threads are destroyed. New
     * threads with new instances of {@link TaskRequestHandler} are constructed when the next
     * pipeline module begins processing.
     */
    @Override
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public void run() {
        log.debug("run() - start");
        try {
            while (!Thread.currentThread().isInterrupted()) {

                // If execution blocks here, it will eventually unblock when either a task request
                // becomes available, or the thread gets interrupted resulting in an
                // InterruptedException.
                TaskRequest taskRequest = taskRequestQueue.take();

                // If the pipeline definition node ID has changed, it means that the pipeline
                // is ready to transition from one definition node to the next. Given that the
                // next node may need a different number of workers, all the TaskRequestHandlers
                // should shut down when they see that this has happened.
                if (taskRequest.getPipelineDefinitionNodeId() != pipelineDefinitionNodeId) {
                    log.info("Transitioning to pipeline definition node "
                        + taskRequest.getPipelineDefinitionNodeId()
                        + " so shutting down task request handler");

                    // Also, in this case, the task has to be put back onto the queue.
                    taskRequestQueue.put(taskRequest);
                    Thread.currentThread().interrupt();
                    continue;
                }
                taskRequests.add(taskRequest);

                // For testing purposes, we can allow negative values for the pipeline definition
                // node id.
                if (taskRequest.getPipelineDefinitionNodeId() < 0) {
                    continue;
                }
                ExternalProcess externalProcess = ExternalProcess
                    .simpleExternalProcess(commandLine(taskRequest));

                // If execution blocks here, it will stay blocked until the worker process exits
                // (successfully or otherwise). However, if the supervisor shuts down, it will send
                // shutdown messages to all the workers, which will cause all the worker external
                // processes to exit. That will bring us back here, where we will find that the
                // thread has been interrupted and we exit the while loop.
                int status = externalProcess.execute();

                // If the external process returned a nonzero status, we need to ensure that
                // the task is correctly marked as errored and that the instance is properly
                // updated.
                boolean readyForTransition = (boolean) DatabaseTransactionFactory
                    .performTransaction(() -> {
                        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
                        PipelineOperations pipelineOperations = new PipelineOperations();
                        PipelineTask task = pipelineTaskCrud.retrieve(taskRequest.getTaskId());

                        // If the external process returned a nonzero status, we need to ensure that
                        // the task is correctly marked as errored and that the instance is properly
                        // updated.
                        if (status != 0) {

                            // If the external process exited with bad status because its task was
                            // deliberately killed, we want to present this information as an alert
                            // and in the log.
                            if (TaskRequestHandlerLifecycleManager
                                .taskOnKilledTaskList(taskRequest.getTaskId())) {
                                log.info("Task " + taskRequest.getTaskId() + " killed by user");
                                AlertService alertService = AlertService.getInstance();
                                alertService.generateAndBroadcastAlert("PI",
                                    taskRequest.getTaskId(), AlertService.Severity.ERROR,
                                    "Task " + taskRequest.getTaskId()
                                        + " killed during local execution.");
                            } else {
                                log.error("Marking task " + taskRequest.getTaskId()
                                    + " failed because PipelineWorker return value == " + status);

                                AlertService alertService = AlertService.getInstance();
                                alertService.generateAndBroadcastAlert("PI",
                                    taskRequest.getTaskId(), AlertService.Severity.ERROR,
                                    "Task marked as errored due to WorkerProcess return value "
                                        + status);
                            }
                            if (task.getState() != PipelineTask.State.ERROR) {
                                pipelineOperations.setTaskState(task, State.ERROR);
                                pipelineTaskCrud.merge(task);
                            }
                        }
                        return task.getState().equals(PipelineTask.State.COMPLETED);
                    });

                // Note that the worker will return when the task reaches p-state Ac, at which point
                // it's still processing; the AlgorithmMonitor then submits it back into the task
                // queue, at which point another worker will get the task to state COMPLETE. If this
                // is the end of the first task (i.e., the one that stopped at Ac), we don't want to
                // try to transition!
                if (readyForTransition) {
                    transitionToNextInstanceNode(taskRequest.getTaskId());
                }
            }
        } catch (InterruptedException e) {

            // If we got here, then the worker threads have been instructed to shut down.
            // Set the interrupt flag so we can exit the while loop.
            Thread.currentThread().interrupt();
        } finally {
            handlerShutdownCountdownLatch.countDown();
        }
    }

    /**
     * Performs the transition from one pipeline module to the next. The process includes the
     * following steps:
     * <ol>
     * <li>Determine whether the user wants execution to halt with the current module, if so go no
     * further.
     * <li>Update task counts for the current module.
     * <li>Update the state of the current pipeline instance.
     * <li>If the
     * {@link PipelineExecutor#transitionToNextInstanceNode(PipelineInstance, PipelineTask, TaskCounts)}
     * method returns tasks, use {@link PipelineExecutor} to submit them to the task queue. These
     * tasks are the tasks for the next pipeline module, if transition to that module is
     * appropriate.
     * </ol>
     */
    private void transitionToNextInstanceNode(long taskId) {
        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
        PipelineTask task = (PipelineTask) DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTask pipelineTask = pipelineTaskCrud.retrieve(taskId);

            // These lines are here to ensure that lazy initialization runs.
            Hibernate.initialize(pipelineTask.getPipelineInstance());
            Hibernate.initialize(pipelineTask.getModuleImplementation());
            return pipelineTask;
        });
        boolean success = task.getState().equals(PipelineTask.State.COMPLETED);
        PipelineModule currentModule = task.getModuleImplementation();
        if (currentModule != null && currentModule.isHaltPipelineOnTaskCompletion()) {
            log.info("currentPipelineModule.isHaltPipelineOnTaskCompletion == true, "
                + "so NOT executing transition logic for task " + taskId);
            return;
        }
        log.info("executing transition logic for task " + taskId);

        TaskCounts taskCountsForCurrentNode = (TaskCounts) DatabaseTransactionFactory
            .performTransaction(
                () -> new PipelineOperations().taskCounts(task.getPipelineInstanceNode()));
        PipelineExecutor pipelineExecutor = new PipelineExecutor();
        PipelineTask mergedTask = task;
        if (success) {
            log.info("Executing transition logic");

            pipelineExecutor.transitionToNextInstanceNode(task.getPipelineInstance(), task,
                taskCountsForCurrentNode);

            mergedTask = (PipelineTask) DatabaseTransactionFactory.performTransaction(() -> {
                task.setTransitionComplete(true);
                return pipelineTaskCrud.merge(task);
            });
        } else {
            log.info(
                "postProcessing: not executing transition logic because of current task failure");
        }
        pipelineExecutor.logUpdatedInstanceState(mergedTask.getPipelineInstance());
    }

    /** For testing only. */
    Set<TaskRequest> getTaskRequests() {
        return taskRequests;
    }

    /**
     * Generates the command line to start an instance of {@link PipelineWorker} to run the given
     * task. The command line includes the pipeline-side library paths, the maximum permitted Java
     * heap size, and information about the task to process.
     */
    private CommandLine commandLine(TaskRequest taskRequest) {

        // The command line starts with the ziggy command.
        CommandLine commandLine = new CommandLine(
            DirectoryProperties.ziggyBinDir().resolve(ZIGGY_PROGRAM).toString());

        // Next come the JVM arguments -- note that we don't need to set the
        // classpath to include both Ziggy and pipelines because the ziggy program
        // handles that automatically.
        commandLine.addArgument(ExternalProcessUtils.log4jConfigString());
        commandLine.addArgument(ExternalProcessUtils.javaLibraryPath());

        int processHeapSizeMb = Math.round((float) heapSizeMb / workerCount);
        commandLine.addArgument("-Xmx" + Integer.toString(processHeapSizeMb) + "M");

        // Now for the worker process fully qualified class name
        commandLine.addArgument("--class=" + PipelineWorker.class.getName());

        // Now for the worker arguments
        commandLine.addArgument(Integer.toString(workerId));
        commandLine.addArgument(Long.toString(taskRequest.getTaskId()));
        commandLine.addArgument(taskRequest.getRunMode().name());
        log.debug("Worker command line: " + commandLine.toString());

        return commandLine;
    }

    @Override
    public Object requestorIdentifier() {
        return uuid;
    }
}
