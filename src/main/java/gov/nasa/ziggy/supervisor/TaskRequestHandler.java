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

import gov.nasa.ziggy.metrics.report.Memdrone;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
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

    /** For testing only. */
    private final Set<TaskRequest> taskRequests = new TreeSet<>();

    private final long pipelineDefinitionNodeId;
    private final CountDownLatch taskRequestThreadCountdownLatch;

    public TaskRequestHandler(int workerId, int workerCount, int heapSizeMb,
        PriorityBlockingQueue<TaskRequest> taskRequestQueue, long pipelineDefinitionNodeId,
        CountDownLatch taskRequestThreadCountdownLatch) {
        this.workerId = workerId;
        this.workerCount = workerCount;
        this.heapSizeMb = heapSizeMb;
        this.taskRequestQueue = taskRequestQueue;
        this.pipelineDefinitionNodeId = pipelineDefinitionNodeId;
        this.taskRequestThreadCountdownLatch = taskRequestThreadCountdownLatch;
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

                // If the pipeline definition node ID has changed, it means that the next tasks
                // in the queue belong to a different {@link PipelineDefinitionNode}. The only way
                // that this can happen is if multiple pipelines are executing simultaneously and
                // one of them has queued up tasks for a new pipeline definition node and the others
                // have not. Given that the node for the new task requests may need a different
                // number of workers, all the TaskRequestHandlers should shut down when they see
                // that this has happened.
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
                if (taskRequest.getPipelineDefinitionNodeId() > 0) {
                    processTaskRequest(taskRequest);
                }
            }
        } catch (InterruptedException e) {

            // If we got here, then the worker threads have been instructed to shut down.
            // Set the interrupt flag so we can exit the while loop.
            Thread.currentThread().interrupt();
        } finally {

            // Before exiting the run() method, tell the lifecycle manager that this
            // task request handler is finished.
            taskRequestThreadCountdownLatch.countDown();
        }
    }

    private void processTaskRequest(TaskRequest taskRequest) {

        // If we previously marked this instance node as transitioned, unmark it now. This
        // is necessary because the combination of node transitioned plus running tasks from
        // the instance node means that the tasks failed and the pipeline transitioned to
        // doing nothing, and we are now rerunning the tasks and want to be able to
        // transition to the next node.
        markInstanceNodeNotTransitioned(taskRequest.getInstanceNodeId());

        ExternalProcess externalProcess = ExternalProcess
            .simpleExternalProcess(commandLine(taskRequest));

        // If execution blocks here, it will stay blocked until the worker process exits
        // (successfully or otherwise). However, if the supervisor shuts down, it will send
        // shutdown messages to all the workers, which will cause all the worker external
        // processes to exit. That will bring us back here, where we will find that the
        // thread has been interrupted and we exit the while loop.
        int status = externalProcess.execute();

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            PipelineOperations pipelineOperations = new PipelineOperations();
            PipelineTask task = pipelineTaskCrud.retrieve(taskRequest.getTaskId());

            // If the external process returned a nonzero status, we need to ensure that
            // the task is correctly marked as errored and that the instance is properly
            // updated.
            if (status != 0) {

                // We only need to send alerts and alter task states if the task bombed

                log.error("Marking task " + taskRequest.getTaskId()
                    + " failed because PipelineWorker return value == " + status);

                AlertService alertService = AlertService.getInstance();
                alertService.generateAndBroadcastAlert("PI", taskRequest.getTaskId(),
                    AlertService.Severity.ERROR,
                    "Task marked as errored due to WorkerProcess return value " + status);

                if (task.getState() != PipelineTask.State.ERROR) {
                    pipelineOperations.setTaskState(task, State.ERROR);
                    task = pipelineTaskCrud.merge(task);
                }
            }
            return null;
        });

        // Finalize the task's memdrone activities.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTask task = new PipelineTaskCrud().retrieve(taskRequest.getTaskId());
            log.info("task {}: isRetried(): {}", task.getId(), task.isRetry());

            Memdrone memdrone = new Memdrone(task.getModuleName(),
                task.getPipelineInstance().getId());
            if (Memdrone.memdroneEnabled()) {
                memdrone.createStatsCache();
                memdrone.createPidMapCache();
            }
            return null;
        });

        transitionToNextInstanceNode(taskRequest.getInstanceNodeId());
    }

    /**
     * Checks whether the current instance node is complete and, if so, initiates the transition to
     * the next instance node. Synchronized to ensure that each transition is executed once and only
     * once.
     */
    private static synchronized void transitionToNextInstanceNode(long instanceNodeId) {

        // Retrieve the instance node, make sure some fields are populated, generate task counts,
        // perform some logging.
        PipelineExecutor pipelineExecutor = new PipelineExecutor();
        PipelineInstanceNodeInformation nodeInformation = (PipelineInstanceNodeInformation) DatabaseTransactionFactory
            .performTransaction(() -> {

                PipelineInstanceNode instanceNode = new PipelineInstanceNodeCrud()
                    .retrieve(instanceNodeId);
                Hibernate.initialize(instanceNode.getPipelineInstance());
                Hibernate.initialize(instanceNode.getPipelineDefinitionNode());
                Hibernate.initialize(instanceNode.getPipelineDefinitionNode().getNextNodes());
                TaskCounts taskCounts = new PipelineOperations().taskCounts(instanceNode);
                pipelineExecutor.logUpdatedInstanceState(instanceNode.getPipelineInstance());
                return new PipelineInstanceNodeInformation(instanceNode, taskCounts);
            });

        // If some other task has already kicked off the transition, don't send it again.
        if (nodeInformation.getPipelineInstanceNode().isTransitionComplete()) {
            return;
        }

        // If the node is done executing then we need to initiate the transition as long as no
        // tasks have failed.
        PipelineInstanceNode pipelineInstanceNode = nodeInformation.getPipelineInstanceNode();
        TaskCounts taskCounts = nodeInformation.getTaskCounts();
        if (taskCounts.isInstanceNodeExecutionComplete()) {
            log.info("Node {} execution complete",
                pipelineInstanceNode.getPipelineDefinitionNode().getModuleName());
            DatabaseTransactionFactory.performTransaction(() -> {
                new PipelineInstanceNodeCrud().markTransitionComplete(instanceNodeId);
                return null;
            });

            log.info("Task counts: {}", taskCounts.log());
            if (taskCounts.isInstanceNodeComplete()) {
                pipelineExecutor.transitionToNextInstanceNode(pipelineInstanceNode, taskCounts);
            } else {
                log.error("Halting pipeline execution due to errors in node {}",
                    pipelineInstanceNode.getPipelineDefinitionNode().getModuleName());
            }

            // Log the pipeline instance state.
        } else {
            log.info("Instance node {} not complete", instanceNodeId);
        }
    }

    private static synchronized void markInstanceNodeNotTransitioned(long pipelineInstanceNodeId) {
        DatabaseTransactionFactory.performTransaction(() -> {
            new PipelineInstanceNodeCrud().markTransitionIncomplete(pipelineInstanceNodeId);
            return null;
        });
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

    /**
     * Container class for a {@link PipelineInstanceNode} and its associated {@link TaskCounts}.
     * Used to transport both out of a database transaction.
     *
     * @author PT
     */
    public static class PipelineInstanceNodeInformation {

        private final PipelineInstanceNode pipelineInstanceNode;
        private final TaskCounts taskCounts;

        public PipelineInstanceNodeInformation(PipelineInstanceNode pipelineInstanceNode,
            TaskCounts taskCounts) {
            this.pipelineInstanceNode = pipelineInstanceNode;
            this.taskCounts = taskCounts;
        }

        public PipelineInstanceNode getPipelineInstanceNode() {
            return pipelineInstanceNode;
        }

        public TaskCounts getTaskCounts() {
            return taskCounts;
        }
    }
}
