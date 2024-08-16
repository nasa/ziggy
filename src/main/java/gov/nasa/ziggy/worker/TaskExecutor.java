package gov.nasa.ziggy.worker;

import static gov.nasa.ziggy.services.process.AbstractPipelineProcess.getProcessInfo;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.CounterMetric;
import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.IntervalMetricKey;
import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskExecutionLog;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.process.StatusMessage;
import gov.nasa.ziggy.services.process.StatusReporter;
import gov.nasa.ziggy.supervisor.TaskContext;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Coordinates processing of inbound worker task request messages. Manages the database and
 * messaging transaction context, invokes the module, then invokes the transition logic.
 *
 * @author Todd Klaus
 * @author PT
 */
public class TaskExecutor implements StatusReporter {
    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    public static final String PIPELINE_MODULE_COMMIT_METRIC = "pipeline.module.commitTime";
    public static final int MAX_TASK_RETRIEVE_RETRIES = 1;
    public static final long WAIT_BETWEEN_RETRIES_MILLIS = 100;

    /**
     * Instance of {@link PipelineTask} processed by this instance of {@link TaskExecutor}. By
     * placing it in a static field, we make it available to other classes that run under the same
     * {@link PipelineWorker}. As there is only 1 task per {@link PipelineWorker}, there is no
     * danger that multiple pipeline tasks will compete for use of this field.
     */
    private static PipelineTask workerTask;

    private final int workerNumber;
    private String lastErrorMessage = "";
    private boolean taskDone;
    private TaskContext taskContext = new TaskContext();
    private long taskId;
    private RunMode runMode;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    public TaskExecutor(int workerNumber, long taskId, RunMode runMode) {
        this.taskId = taskId;
        this.workerNumber = workerNumber;
        this.runMode = runMode;

        taskContext.setTask(pipelineTaskOperations().pipelineTask(taskId));
    }

    /**
     * Master method that invokes all steps needed to pre-process, process, and post-process the
     * task.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void executeTask() {
        IntervalMetricKey key = IntervalMetric.start();
        try {
            executeTaskInternal();
        } catch (Exception e) {
            // Here we catch Exception because it's what's thrown by PipelineModule.processTask().
            // See the documentation for that class and method for an explanation. Once the
            // exception gets here we handle it so that the worker doesn't terminate until after
            // all the foregoing catch and finally actions are complete.
            lastErrorMessage = e.getMessage();

            log.error("processMessage(): caught exception processing worker task request for "
                + contextString(taskContext), e);

            CounterMetric.increment("pipeline.module.execFailCount");

            postProcessingAfterException();
        } finally {
            IntervalMetric.stop("pipeline.module.processMessage", key);

            // make sure any active transaction is cleaned up
            DatabaseService databaseService = DatabaseService.getInstance();
            databaseService.rollbackTransactionIfActive();
        }
    }

    private void executeTaskInternal() throws Exception {

        taskContext.setState(TaskContext.TaskState.PROCESSING);
        taskContext.setProcessingStartTimeMillis(System.currentTimeMillis());
        ZiggyMessenger.publish(statusMessage(false));

        log.info("Executing pre-processing for taskId={}", taskId);
        preProcessing();
        log.info("Executing pre-processing for taskId={}...done", taskId);

        try {
            Metric.enableThreadMetrics();

            /* Invoke the module */
            log.info("Executing processTask for taskId={}", taskId);
            taskDone = processTask();

            if (taskDone) {
                log.info("Executing processTask for taskId={}...done", taskId);
                CounterMetric.increment("pipeline.module.execSuccessCount");
            } else {
                log.info(
                    "Executing processTask for taskId={}...current step done (more steps remain)",
                    taskId);
            }
        } finally {
            taskContext.setTaskMetrics(Metric.getThreadMetrics());
            Metric.disableThreadMetrics();
        }

        /* Update the task status */
        log.info("Executing post-processing for taskId={}", taskId);
        postProcessing(taskDone, true);
        log.info("Executing post-processing for taskId={}...done", taskId);
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void postProcessingAfterException() {
        if (taskContext.getPipelineTaskId() != null) {
            // could be null if it wasn't found in the db
            try {
                postProcessing(true, false);
            } catch (Exception e) {
                log.error("Failed in postProcessing for: " + contextString(taskContext), e);
            }
        }
    }

    /**
     * Update the PipelineTask in the db to reflect the fact that this worker is now processing the
     * task. This is done with a local transaction (outside of the distributed transaction) and
     * committed immediately so that the console will show updated status right away
     */
    private boolean preProcessing() {

        PipelineTask pipelineTask = pipelineTaskOperations().addTaskExecutionLog(taskId,
            workerNumber, taskContext.getProcessingStartTimeMillis());

        // If the user requested that only the transition logic be re-run, or if the transition
        // logic previously failed, then we only need to re-run the transition logic
        boolean transitionOnly = pipelineTask.getProcessingStep() == ProcessingStep.COMPLETE;

        if (transitionOnly) {
            pipelineTaskOperations().merge(pipelineTask);
        } else {
            pipelineTask.setWorkerHost(getProcessInfo().getHost());
            pipelineTask.setWorkerThread(workerNumber);
            pipelineTask.setSoftwareRevision(
                ZiggyConfiguration.getInstance().getString(PropertyName.ZIGGY_VERSION.property()));

            // If this is the first time we've called the module, set the processing step to the
            // first step that the module defines; otherwise, leave it alone and let the module pick
            // up where it left off.
            if (pipelineTask.getProcessingStep().isInfrastructureStep()) {
                ProcessingStep processingStep = pipelineTaskOperations()
                    .moduleImplementation(pipelineTask, runMode)
                    .processingSteps()
                    .get(0);
                pipelineTaskOperations().updateProcessingStep(pipelineTask, processingStep);
            }
        }
        return transitionOnly;
    }

    /**
     * Invoke PipelineModule.processTask() and execute the transition logic (if applicable)
     *
     * @returns true if task is done (no more steps)
     */
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    private boolean processTask() throws Exception {
        String moduleExecMetricPrefix = null;

        try {
            return processTaskInternal();
        } catch (Exception e) {
            log.error("Failed in PipelineModule.processTask() for: " + contextString(taskContext));
            CounterMetric.increment("pipeline.module.execFailCount");
            throw e;
        } finally {

            if (moduleExecMetricPrefix != null) {
                CounterMetric.increment(moduleExecMetricPrefix + ".execCount");
            }
        }
    }

    private boolean processTaskInternal() throws Exception {

        // Here's initialization using a builder pattern.
        PipelineTask pipelineTask = pipelineTaskOperations().pipelineTask(taskId);

        workerTask = pipelineTask;
        String moduleExecMetricPrefix = "pipeline.module." + taskContext.getModule();

        log.info("Processing {}", contextString(taskContext));

        PipelineModule currentPipelineModule = pipelineTaskOperations()
            .moduleImplementation(pipelineTask, runMode);
        taskContext.setPipelineModule(currentPipelineModule);

        String moduleSimpleName = taskContext.getPipelineModule().getClass().getSimpleName();

        log.info("Calling {}.processTask()", moduleSimpleName);
        pipelineModuleProcessTask(currentPipelineModule, moduleExecMetricPrefix);
        log.info("Calling {}.processTask()...done", moduleSimpleName);

        if (isTaskDone()) {
            pipelineTaskOperations().updateProcessingStep(taskId, ProcessingStep.COMPLETE);
        }

        return isTaskDone();
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void pipelineModuleProcessTask(PipelineModule currentPipelineModule,
        String moduleExecMetricPrefix) throws Exception {

        IntervalMetricKey key = IntervalMetric.start();
        long startTime = System.currentTimeMillis();

        try {
            // Hand off control to the PipelineModule implementation
            setTaskDone(currentPipelineModule.processTask());
        } finally {
            IntervalMetric.stop(moduleExecMetricPrefix + ".processTask", key);
            taskContext.setModuleExecTime(System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Update the PipelineTask in the db with the results of the processing
     */
    private void postProcessing(boolean done, boolean success) {

        PipelineTask pipelineTask = pipelineTaskOperations().pipelineTask(taskId);

        long processingEndTimeMillis = System.currentTimeMillis();
        long totalProcessingTimeMillis = processingEndTimeMillis
            - taskContext.getProcessingStartTimeMillis();

        log.info("Total processing time for this step (minutes): {}",
            totalProcessingTimeMillis / 1000.0 / 60.0);

        Date endProcessingTime = new Date(processingEndTimeMillis);

        // Update summary metrics.
        taskContext.getPipelineModule()
            .updateMetrics(pipelineTask, taskContext.getTaskMetrics(), totalProcessingTimeMillis);

        List<TaskExecutionLog> execLog = pipelineTaskOperations().execLogs(pipelineTask);
        log.debug("execLog size={}", execLog.size());

        if (execLog != null && !execLog.isEmpty()) {
            TaskExecutionLog currentExecLog = execLog.get(execLog.size() - 1);
            currentExecLog.setEndProcessingTime(endProcessingTime);
            currentExecLog.setFinalProcessingStep(pipelineTask.getProcessingStep());
        } else {
            log.warn("Task execution log is missing or empty for taskId={}", taskId);
        }

        if (!done) {
            return;
        }
        if (success) {
            pipelineTaskOperations().updateProcessingStep(pipelineTask.getId(),
                ProcessingStep.COMPLETE);
        } else {
            pipelineTask.incrementFailureCount();
            pipelineTask = pipelineTaskOperations().taskErrored(pipelineTask);

            AlertService alertService = AlertService.getInstance();
            alertService.generateAlert("PI(" + taskContext.getModule() + ")", pipelineTask.getId(),
                AlertService.Severity.INFRASTRUCTURE, lastErrorMessage);
        }
    }

    private void setTaskDone(boolean taskDone) {
        this.taskDone = taskDone;
    }

    private boolean isTaskDone() {
        return taskDone;
    }

    private String contextString(TaskContext context) {
        return "IID=" + context.getPipelineInstanceId() + ", TID=" + taskId + ", M="
            + context.getModule() + ", UOW=" + context.getModuleUow();
    }

    @Override
    public StatusMessage reportCurrentStatus() {
        return statusMessage(false);
    }

    public StatusMessage statusMessage(boolean lastMessage) {
        Long pipelineInstanceId = taskContext.getPipelineInstanceId();
        Long pipelineTaskId = taskContext.getPipelineTaskId();
        String currentPipelineInstanceId = pipelineInstanceId == null ? "-"
            : "" + pipelineInstanceId.toString();
        String currentPipelineTaskId = pipelineTaskId == null ? "-"
            : "" + pipelineTaskId.toString();

        WorkerStatusMessage message = new WorkerStatusMessage(workerNumber,
            taskContext.getState().toString(), currentPipelineInstanceId, currentPipelineTaskId,
            taskContext.getModule(), taskContext.getModuleUow(),
            taskContext.getProcessingStartTimeMillis(), lastMessage);
        message.setSourceProcess(getProcessInfo());
        return message;
    }

    public static PipelineTask getWorkerTask() {
        return workerTask;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }
}
