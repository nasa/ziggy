package gov.nasa.ziggy.worker;

import static gov.nasa.ziggy.services.database.DatabaseTransactionFactory.performTransaction;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.CounterMetric;
import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.IntervalMetricKey;
import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.module.ModuleFatalProcessingException;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.WorkerMemoryManager;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskExecutionLog;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.DatabaseTransaction;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;
import gov.nasa.ziggy.services.process.ProcessInfo;
import gov.nasa.ziggy.services.process.StatusMessage;
import gov.nasa.ziggy.services.process.StatusReporter;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.util.ZiggyVersion;

/**
 * Coordinates processing of inbound worker task request messages. Manages the database and
 * messaging transaction context, invokes the module, then invokes the transition logic.
 * <p>
 * This class is not MT-safe, and is intended to be called only by a single, dedicated worker thread
 * ({@link WorkerTaskRequestHandler}
 *
 * @author Todd Klaus
 */
public class WorkerTaskRequestDispatcher implements StatusReporter {
    private static final Logger log = LoggerFactory.getLogger(WorkerTaskRequestDispatcher.class);

    // This Map contains the ID of every task being processed by a WorkerTaskRequestDispatcher
    // instance, and whether that task is slated for deletion. This allows outside threads to
    // tell WorkerTaskRequestDispatcher that a task needs to be deleted, and then allows both
    // WorkerTaskRequestDispatcher and the PipelineModule instances that this is the case.
    private static Map<Long, Boolean> taskDeletionStatus = new ConcurrentHashMap<>();

    public static final String PIPELINE_MODULE_COMMIT_METRIC = "pipeline.module.commitTime";
    public static final int MAX_TASK_RETRIEVE_RETRIES = 1;
    public static final long WAIT_BETWEEN_RETRIES_MILLIS = 100;

    private final ProcessInfo processInfo;
    private final int threadNumber;
    private TaskLog taskLog;
    private WorkerMemoryManager memoryManager = null;
    private String lastErrorMessage = "";
    private boolean taskDone;
    private WorkerThreadContext threadContext = new WorkerThreadContext();
    private long taskId;

    /**
     * @param processInfo
     * @param threadNum
     * @param memoryManager
     */
    public WorkerTaskRequestDispatcher(ProcessInfo processInfo, int threadNum,
        WorkerMemoryManager memoryManager) {
        this.processInfo = processInfo;
        threadNumber = threadNum;
        this.memoryManager = memoryManager;

        ZiggyConfiguration.getInstance();
    }

    /**
     * Process incoming worker task requests
     *
     * @throws PipelineException
     */
    public void processMessage(WorkerTaskRequest workerRequest) {
        threadContext.setRequest(workerRequest);
        taskId = workerRequest.getTaskId();
        taskLog = initializeTaskLog(workerRequest.getInstanceId(), workerRequest.getTaskId());

        IntervalMetricKey key = IntervalMetric.start();

        boolean taskDone = false;

        try {
            taskLog.startLogging();

            threadContext.setState(WorkerThreadContext.ThreadState.PROCESSING);
            threadContext.setProcessingStartTimeMillis(System.currentTimeMillis());

            /*
             * Insert a random delay in order to stagger the start times across all workers. Useful
             * for reducing the surge on the database and filestore when a new pipeline starts
             */
            RandomDelay.randomWait();

            if (isTaskDeleted(taskId)) {
                log.info("Task deleted, exiting prior to pre-processing step");
                cleanup();
                return;
            }
            log.info("Executing pre-processing for taskId = " + workerRequest.getTaskId() + "...");

            boolean doTransitionOnly = preProcessing(workerRequest.isDoTransitionOnly());

            log.info("DONE executing pre-processing for taskId = " + workerRequest.getTaskId());

            if (!doTransitionOnly) {
                try {
                    Metric.enableThreadMetrics();

                    if (isTaskDeleted(taskId)) {
                        log.info("Task deleted, exiting prior to processTask step");
                        cleanup();
                        return;
                    }

                    /* Invoke the module */
                    log.info(
                        "Executing processTask for taskId = " + workerRequest.getTaskId() + "...");
                    taskDone = processTask();

                    if (isTaskDeleted(taskId)) {
                        log.info("Task deleted, exiting after processTask step");
                        cleanup();
                        return;
                    }

                    if (taskDone) {
                        log.info(
                            "DONE executing processTask for taskId = " + workerRequest.getTaskId());
                        CounterMetric.increment("pipeline.module.execSuccessCount");
                    } else {
                        log.info(
                            "DONE executing processTask for current step (more steps remain) for taskId = "
                                + workerRequest.getTaskId());
                    }
                } finally {
                    threadContext.setThreadMetrics(Metric.getThreadMetrics());
                    Metric.disableThreadMetrics();
                }
            }

            /* Update the task status */

            if (isTaskDeleted(taskId)) {
                log.info("Task deleted, exiting prior to post-processing step");
                cleanup();
                return;
            }
            log.info("Executing post-processing for taskId = " + workerRequest.getTaskId() + "...");
            postProcessing(taskDone, true);

            if (taskDone) {
                /* run the transition logic */
                if (isTaskDeleted(taskId)) {
                    log.info("Task deleted, exiting prior to doTransition step");
                    cleanup();
                    return;
                }
                doTransition(true);
                log.info(
                    "DONE executing post-processing for taskId = " + workerRequest.getTaskId());
            }

        } catch (Throwable t) {
            lastErrorMessage = t.getMessage();

            log.error("processMessage(): caught exception processing worker task request for "
                + contextString(threadContext), t);

            CounterMetric.increment("pipeline.module.execFailCount");

            if (threadContext.getPipelineTask() != null) {
                // could be null if it wasn't found in the db
                try {
                    postProcessing(true, false);
                } catch (Throwable t2) {
                    log.error("Failed in postProcessing for: " + contextString(threadContext), t2);
                }
                try {
                    doTransition(false);
                } catch (Throwable t2) {
                    log.error("Failed in doTransition for: " + contextString(threadContext), t2);
                }
            }

        } finally {
            cleanup();
            IntervalMetric.stop("pipeline.module.processMessage", key);

            // make sure any active transaction is cleaned up
            DatabaseService databaseService = DatabaseService.getInstance();
            databaseService.rollbackTransactionIfActive();
        }
    }

    private TaskLog initializeTaskLog(long instanceId, long taskId) {
        PipelineTaskCrud crud = new PipelineTaskCrud();
        PipelineTask pipelineTask = (PipelineTask) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineTask task = null;
                int iTry = 0;
                while (task == null && iTry < MAX_TASK_RETRIEVE_RETRIES) {
                    task = crud.retrieve(taskId);
                    if (task == null) {
                        Thread.sleep(WAIT_BETWEEN_RETRIES_MILLIS);
                    }
                }
                if (task == null) {
                    throw new ModuleFatalProcessingException(
                        "No pipeline task found for ID=" + taskId);
                }
                Hibernate.initialize(task.getSummaryMetrics());
                Hibernate.initialize(task.getExecLog());
                Hibernate.initialize(task.getProducerTaskIds());
                return task;
            });

        TaskLog taskLog = new TaskLog(threadNumber, pipelineTask);
        threadContext.setTaskLog(taskLog);

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTask task = crud.retrieve(taskId);
            task.incrementTaskLogIndex();
            crud.update(task);
            return null;
        });

        return taskLog;
    }

    /**
     * Update the PipelineTask in the db to reflect the fact that this worker is now processing the
     * task. This is done with a local transaction (outside of the distributed transaction) and
     * committed immediately so that the console will show updated status right away
     *
     * @return boolean Indicates whether only the transition logic needs to be run
     * @throws PipelineException
     */
    private boolean preProcessing(boolean doTransitionOnlyOverride) throws Exception {

        boolean doTransitionOnly = (boolean) DatabaseTransactionFactory.performTransaction(() -> {
            PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
            PipelineInstance pipelineInstance = pipelineInstanceCrud
                .retrieve(threadContext.getRequest().getInstanceId());

            if (pipelineInstance == null) {
                throw new PipelineException("No PipelineInstance found for id="
                    + threadContext.getRequest().getInstanceId());
            }

            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            PipelineTask pipelineTask = pipelineTaskCrud
                .retrieve(threadContext.getRequest().getTaskId());

            if (pipelineTask == null) {
                throw new PipelineException(
                    "No PipelineTask found for id=" + threadContext.getRequest().getTaskId());
            }

            TaskExecutionLog execLog = new TaskExecutionLog(processInfo.getHost(), threadNumber);
            execLog.setStartProcessingTime(new Date(threadContext.getProcessingStartTimeMillis()));
            execLog.setInitialState(pipelineTask.getState());
            execLog.setInitialProcessingState(pipelineTask.getProcessingState());

            pipelineTask.getExecLog().add(execLog);

            if (pipelineTask.getState() == PipelineTask.State.ERROR) {
                PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();

                pipelineInstanceNodeCrud
                    .decrementFailedTaskCount(threadContext.getRequest().getInstanceNodeId());
            }

            PipelineModuleDefinition moduleDefinition = pipelineTask.getPipelineInstanceNode()
                .getPipelineModuleDefinition();
            threadContext.setMinMemoryMegaBytes(moduleDefinition.getMinMemoryMegaBytes());

            /*
             * If the user requested that only the transition logic be re-run, or if the transition
             * logic previously failed, then we only need to re-run the transition logic
             */
            boolean transitionOnly = doTransitionOnlyOverride
                || pipelineTask.getState() == PipelineTask.State.COMPLETED
                || pipelineTask.getState() == PipelineTask.State.PARTIAL;

            if (!transitionOnly) {
                pipelineTask.setState(PipelineTask.State.PROCESSING);
                pipelineTask.setWorkerHost(processInfo.getHost());
                pipelineTask.setWorkerThread(threadNumber);
                pipelineTask.setTransitionComplete(false);
                pipelineTask.setSoftwareRevision(ZiggyVersion.getSoftwareVersion());
                pipelineTask.startExecutionClock();
            }
            return transitionOnly;
        });

        return doTransitionOnly;
    }

    /**
     * Invoke PipelineModule.processTask() and execute the transition logic (if applicable)
     *
     * @throws Throwable
     * @returns boolean If true, task is done (no more steps)
     */
    private boolean processTask() throws Throwable {
        IntervalMetricKey key = null;
        String moduleExecMetricPrefix = null;

        try {
            /*
             * Make sure enough memory is available for this task before starting the transaction
             */
            if (memoryManager != null) {
                memoryManager.acquireMemoryMegaBytes(threadContext.getMinMemoryMegaBytes());
            }

            performTransaction(() -> {
                PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
                PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

                WorkerTaskRequest currentRequest = threadContext.getRequest();
                threadContext.setPipelineInstance(
                    pipelineInstanceCrud.retrieve(currentRequest.getInstanceId()));
                threadContext
                    .setPipelineTask(pipelineTaskCrud.retrieve(currentRequest.getTaskId()));
                Hibernate.initialize(threadContext.getPipelineTask()
                    .getPipelineInstance()
                    .getPipelineParameterSets());
                Hibernate.initialize(threadContext.getPipelineTask()
                    .getPipelineInstanceNode()
                    .getModuleParameterSets());
                Hibernate.initialize(threadContext.getPipelineTask()
                    .getPipelineDefinitionNode()
                    .getInputDataFileTypes());
                Hibernate.initialize(threadContext.getPipelineTask()
                    .getPipelineDefinitionNode()
                    .getOutputDataFileTypes());
                Hibernate.initialize(
                    threadContext.getPipelineTask().getPipelineDefinitionNode().getModelTypes());
                return null;
            });
            PipelineModuleDefinition moduleDefinition = threadContext.getPipelineTask()
                .getPipelineInstanceNode()
                .getPipelineModuleDefinition();
            String moduleName = moduleDefinition.getName().getName();
            threadContext.setModule(moduleName);
            threadContext
                .setModuleUow(threadContext.getPipelineTask().uowTaskInstance().briefState());
            moduleExecMetricPrefix = "pipeline.module." + threadContext.getModule();

            log.info("processing:" + contextString(threadContext));

            PipelineModule currentPipelineModule = threadContext.getPipelineTask()
                .getModuleImplementation(threadContext.getRequest().getRunMode());

            threadContext.setPipelineModule(currentPipelineModule);

            String moduleSimpleName = threadContext.getPipelineModule().getClass().getSimpleName();

            log.info("Calling " + moduleSimpleName + ".processTask()");

            key = IntervalMetric.start();
            long startTime = System.currentTimeMillis();

            try {
                // Hand off control to the PipelineModule implementation
                if (currentPipelineModule.processTaskRequiresDatabaseTransaction()) {
                    performTransaction(() -> {
                        setTaskDone(currentPipelineModule.processTask());
                        return null;
                    });
                } else {
                    setTaskDone(currentPipelineModule.processTask());
                }
            } catch (Throwable e) {
                log.error("Unexpected exception processing task", e);
                throw e;
            } finally {
                IntervalMetric.stop(moduleExecMetricPrefix + ".processTask", key);
                threadContext.setModuleExecTime(System.currentTimeMillis() - startTime);
            }

            log.info(moduleSimpleName + ".process() completed");

            // send any queued messages
            currentPipelineModule.flushWorkerMessages();

            if (isTaskDone()) {
                ProcessingSummaryOperations attrOps = new ProcessingSummaryOperations();
                attrOps.updateProcessingState(threadContext.getPipelineTask().getId(),
                    ProcessingState.COMPLETE);
            }

            return isTaskDone();
        } catch (Throwable t) {
            log.error("Failed in PipelineModule.processTask() for: " + contextString(threadContext),
                t);
            CounterMetric.increment("pipeline.module.execFailCount");
            throw t;
        } finally {
            if (memoryManager != null) {
                memoryManager.releaseMemoryMegaBytes(threadContext.getMinMemoryMegaBytes());
            }

            if (moduleExecMetricPrefix != null) {
                CounterMetric.increment(moduleExecMetricPrefix + ".execCount");
            }
        }
    }

    /**
     * Update the PipelineTask in the db with the results of the processing
     *
     * @param summaryMetrics
     * @throws Exception
     */
    private void postProcessing(boolean done, boolean success) throws Throwable {

        performTransaction(() -> {
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            long taskId = threadContext.getRequest().getTaskId();
            PipelineTask pipelineTask = pipelineTaskCrud.retrieve(taskId);

            if (pipelineTask == null) {
                throw new PipelineException("No PipelineTask found for id=" + taskId);
            }

            long processingEndTimeMillis = System.currentTimeMillis();
            long totalProcessingTimeMillis = processingEndTimeMillis
                - threadContext.getProcessingStartTimeMillis();

            log.info("Total processing time for this step (minutes): "
                + totalProcessingTimeMillis / 1000.0 / 60.0);

            Date endProcessingTime = new Date(processingEndTimeMillis);

            // Update summary metrics
            threadContext.getPipelineModule()
                .updateMetrics(pipelineTask, threadContext.getThreadMetrics(),
                    totalProcessingTimeMillis);

            if (done) {
                pipelineTask.stopExecutionClock();
                if (success) {
                    if (threadContext.getPipelineModule().isPartialSuccess()) {
                        pipelineTask.setState(PipelineTask.State.PARTIAL);
                    } else {
                        pipelineTask.setState(PipelineTask.State.COMPLETED);
                    }
                } else {
                    pipelineTask.setState(PipelineTask.State.ERROR);
                    pipelineTask.incrementFailureCount();

                    try {
                        AlertService alertService = AlertService.getInstance();
                        alertService.generateAlert("PI(" + threadContext.getModule() + ")",
                            pipelineTask.getId(), AlertService.Severity.INFRASTRUCTURE,
                            lastErrorMessage);
                    } catch (Throwable t) {
                        log.warn("Failed to generate alert for message: " + lastErrorMessage);
                    }
                }
            }

            List<TaskExecutionLog> execLog = pipelineTask.getExecLog();
            log.info("execLog = " + execLog.size());

            if (execLog != null && !execLog.isEmpty()) {
                TaskExecutionLog currentExecLog = execLog.get(execLog.size() - 1);
                currentExecLog.setEndProcessingTime(endProcessingTime);
                currentExecLog.setFinalState(pipelineTask.getState());
                currentExecLog.setFinalProcessingState(pipelineTask.getProcessingState());
            } else {
                log.warn("stepLog is missing or empty for taskId: " + taskId);
            }
            return null;
        });

    }

    private void doTransition(boolean success) throws Throwable {

        /*
         * Start a separate transaction to run the transition logic. Do this in a separate
         * transaction so that if it fails, the next worker to get this task only has to re-try the
         * transition logic rather than the whole unit of work (because we set the pipeline task
         * state to COMPLETED in postProcessing)
         */

        performTransaction(new DatabaseTransaction<Void>() {

            String moduleExecMetricPrefix = "pipeline.module." + threadContext.getModule();
            IntervalMetricKey key = IntervalMetric.start();

            @Override
            public void finallyBlock() {
                IntervalMetric.stop(moduleExecMetricPrefix + ".doTransition", key);
            }

            @Override
            public Void transaction() {
                WorkerTaskRequest currentRequest = threadContext.getRequest();
                LinkedList<PipelineTask> createdTasks = new LinkedList<>();

                /*
                 * update pipelineInstance state based on the current task counts of all of the
                 * PipelineInstanceNodes
                 */

                PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
                PipelineInstance pipelineInstance = pipelineInstanceCrud
                    .retrieve(currentRequest.getInstanceId());

                if (pipelineInstance == null) {
                    throw new PipelineException(
                        "No PipelineInstance found for id=" + currentRequest.getInstanceId());
                }

                PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
                PipelineTask pipelineTask = pipelineTaskCrud.retrieve(currentRequest.getTaskId());

                if (pipelineTask == null) {
                    throw new PipelineException(
                        "No PipelineTask found for id=" + currentRequest.getTaskId());
                }

                PipelineExecutor pipelineExecutor = new PipelineExecutor();
                PipelineModule currentModule = threadContext.getPipelineModule();

                if (currentModule != null && currentModule.isHaltPipelineOnTaskCompletion()) {
                    log.info(
                        "currentPipelineModule.isHaltPipelineOnTaskCompletion == true, so NOT executing transition logic for "
                            + contextString(threadContext));
                } else {
                    log.info("executing transition logic for " + contextString(threadContext));

                    // obtains lock on PI_PIPELINE_INST_NODE (select for update)
                    TaskCounts taskCountsForCurrentNode = pipelineExecutor
                        .updateTaskCountsForCurrentNode(pipelineTask, success);

                    if (success) {
                        log.info("Executing transition logic");

                        createdTasks = pipelineExecutor.doTransition(pipelineInstance, pipelineTask,
                            taskCountsForCurrentNode);
                        pipelineTask.setTransitionComplete(true);
                    } else {
                        log.info(
                            "postProcessing: not executing transition logic because of current task failure");
                    }
                }

                log.info("updating instance state for " + contextString(threadContext));
                pipelineExecutor.updateInstanceState(pipelineInstance);

                for (PipelineTask task : createdTasks) {
                    pipelineExecutor.sendWorkerMessageForTask(task);
                }
                return null;
            }
        });
    }

    private void setTaskDone(boolean taskDone) {
        this.taskDone = taskDone;
    }

    private boolean isTaskDone() {
        return taskDone;
    }

    public static boolean isTaskDeleted(long taskId) {
        return taskDeletionStatus.containsKey(taskId) ? taskDeletionStatus.get(taskId) : false;
    }

    public static boolean deleteTask(long taskId) {
        boolean taskPresent = taskDeletionStatus.containsKey(taskId);
        if (taskPresent) {
            taskDeletionStatus.put(taskId, true);
        }
        return taskPresent;
    }

    /**
     * @param task
     * @return
     */
    private String contextString(WorkerThreadContext context) {
        WorkerTaskRequest request = context.getRequest();
        return "IID=" + request.getInstanceId() + ", TID=" + request.getTaskId() + ", M="
            + context.getModule() + ", UOW=" + context.getModuleUow();
    }

    /**
     * Performs 2 actions that need to be taken before the {@link WorkerTaskRequestDispatcher} is
     * ready to process a new task: ending any current task logging, and deleting the
     * {@link WorkerThreadContext}.
     */
    private void cleanup() {
        if (ZiggyShutdownHook.shutdownInProgress()) {
            return;
        }
        taskDeletionStatus.remove(taskId);
        threadContext = new WorkerThreadContext();
        if (taskLog != null) {
            taskLog.endLogging();
            taskLog = null;
        }
    }

    @Override
    public synchronized StatusMessage reportCurrentStatus() {
        PipelineInstance currentPipelineInstance = threadContext.getPipelineInstance();
        PipelineTask currentPipelineTask = threadContext.getPipelineTask();
        String currentPipelineInstanceId = currentPipelineInstance == null ? "-"
            : "" + currentPipelineInstance.getId();
        String currentPipelineTaskId = currentPipelineTask == null ? "-"
            : "" + currentPipelineTask.getId();

        WorkerStatusMessage message = new WorkerStatusMessage(threadNumber,
            threadContext.getState().toString(), currentPipelineInstanceId, currentPipelineTaskId,
            threadContext.getModule(), threadContext.getModuleUow(),
            threadContext.getProcessingStartTimeMillis());
        return message;
    }

}
