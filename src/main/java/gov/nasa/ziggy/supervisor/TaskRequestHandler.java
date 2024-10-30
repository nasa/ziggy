package gov.nasa.ziggy.supervisor;

import static gov.nasa.ziggy.module.AlgorithmExecutor.ZIGGY_PROGRAM;

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.exec.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.Memdrone;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.logging.TaskLog;
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
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();

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
        log.debug("Starting");
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
                    log.info(
                        "Transitioning to pipeline definition node {} so shutting down task request handler",
                        taskRequest.getPipelineDefinitionNodeId());

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
        log.info("Start processing taskRequest={}", taskRequest);
        markInstanceNodeNotTransitioned(taskRequest.getInstanceNodeId());

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
        PipelineTask pipelineTask = taskRequest.getPipelineTask();
        if (status != 0) {
            log.error("Marking task {} failed because PipelineWorker return value is {}",
                pipelineTask, status);

            AlertService alertService = AlertService.getInstance();
            alertService.generateAndBroadcastAlert("PI", pipelineTask, Severity.ERROR,
                "Task marked as errored due to WorkerProcess return value " + status);
            pipelineTaskDataOperations().taskErrored(pipelineTask);
        }

        // Finalize the task's memdrone activities.
        boolean taskErrored = pipelineTaskDataOperations().hasErrored(pipelineTask);
        log.info("task={}, retried={}, errored={}", pipelineTask,
            pipelineTaskDataOperations().retrying(pipelineTask), taskErrored);

        Memdrone memdrone = new Memdrone(pipelineTask.getModuleName(),
            pipelineTask.getPipelineInstanceId());
        if (Memdrone.memdroneEnabled()) {
            memdrone.createStatsCache();
            memdrone.createPidMapCache();
        }

        transitionToNextInstanceNode(taskRequest.getInstanceNodeId());
        log.info("Finished processing taskRequest={}", taskRequest);
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
        PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
        PipelineInstanceNodeInformation nodeInformation = pipelineInstanceNodeOperations
            .pipelineInstanceNodeInformation(instanceNodeId);
        pipelineExecutor.logUpdatedInstanceState(
            pipelineInstanceNodeOperations.pipelineInstance(instanceNodeId));

        // If some other task has already kicked off the transition, don't send it again.
        if (nodeInformation.getPipelineInstanceNode().isTransitionComplete()) {
            return;
        }

        // If the node is done executing then we need to initiate the transition as long as no
        // tasks have failed.
        PipelineInstanceNode pipelineInstanceNode = nodeInformation.getPipelineInstanceNode();
        TaskCounts taskCounts = nodeInformation.getTaskCounts();
        if (taskCounts.isPipelineTasksExecutionComplete()) {
            log.info("Node {} execution complete", pipelineInstanceNode.getModuleName());
            new PipelineInstanceNodeOperations().markInstanceNodeTransitionComplete(instanceNodeId);

            log.info("{}", taskCounts);
            if (taskCounts.isPipelineTasksComplete()) {
                pipelineExecutor.transitionToNextInstanceNode(pipelineInstanceNode);
            } else {
                log.error("Halting pipeline execution due to errors in node {}",
                    pipelineInstanceNode.getModuleName());
            }

            // Log the pipeline instance state.
        } else {
            log.info("Instance node {} not complete", instanceNodeId);
        }
    }

    private static synchronized void markInstanceNodeNotTransitioned(long pipelineInstanceNodeId) {
        new PipelineInstanceNodeOperations()
            .markInstanceNodeTransitionIncomplete(pipelineInstanceNodeId);
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
        PipelineTask pipelineTask = taskRequest.getPipelineTask();
        commandLine.addArgument(TaskLog.ziggyLogFileSystemProperty(pipelineTask));
        // Now for the worker process fully qualified class name
        commandLine.addArgument("--class=" + PipelineWorker.class.getName());

        // Now for the worker arguments
        commandLine.addArgument(Integer.toString(workerId));
        commandLine.addArgument(pipelineTask.toString());
        commandLine.addArgument(taskRequest.getRunMode().name());
        log.debug("commandLine={}", commandLine.toString());

        return commandLine;
    }

    @Override
    public Object requestorIdentifier() {
        return uuid;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    PipelineInstanceNodeOperations pipelineInstanceOperations() {
        return pipelineInstanceNodeOperations;
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
