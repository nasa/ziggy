package gov.nasa.ziggy.worker;

import static gov.nasa.ziggy.services.process.AbstractPipelineProcess.getProcessInfo;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

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
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.UpdateProcessingStepMessage;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.process.AbstractPipelineProcess;
import gov.nasa.ziggy.services.process.StatusMessage;
import gov.nasa.ziggy.services.process.StatusReporter;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.BuildInfo;

/**
 * Coordinates processing of inbound worker task request messages. Manages the database and
 * messaging transaction context, invokes the module, then invokes the transition logic.
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
public class TaskExecutor implements StatusReporter {

    private enum TaskState {
        IDLE, PROCESSING
    }

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
    private TaskState state = TaskState.IDLE;
    private boolean taskDone;
    private PipelineTask pipelineTask;
    private PipelineModule currentPipelineModule;
    private RunMode runMode;
    private long processingStartTimeMillis;
    private Map<String, Metric> taskMetrics;
    private String lastErrorMessage = "";
    private CountDownLatch outgoingMessageLatch;

    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    public TaskExecutor(int workerNumber, PipelineTask pipelineTask, RunMode runMode) {
        this.pipelineTask = pipelineTask;
        this.workerNumber = workerNumber;
        this.runMode = runMode;
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

            log.error("Caught exception processing worker task request for {}",
                pipelineTask.toFullString(), e);

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

        state = TaskState.PROCESSING;
        processingStartTimeMillis = System.currentTimeMillis();
        ZiggyMessenger.publish(statusMessage(false));

        log.info("Executing pre-processing for task {}", pipelineTask);
        preProcessing();
        log.info("Executing pre-processing for task {}...done", pipelineTask);

        try {
            Metric.enableThreadMetrics();

            /* Invoke the module */
            log.info("Executing processTask for task {}", pipelineTask);
            taskDone = processTask();

            if (taskDone) {
                log.info("Executing processTask for task {}...done", pipelineTask);
                CounterMetric.increment("pipeline.module.execSuccessCount");
            } else {
                log.info(
                    "Executing processTask for task {}...current step done (more steps remain)",
                    pipelineTask);
            }
        } finally {
            taskMetrics = Metric.getThreadMetrics();
            Metric.disableThreadMetrics();
        }

        /* Update the task status */
        log.info("Executing post-processing for task {}", pipelineTask);
        postProcessing(taskDone, true);
        log.info("Executing post-processing for task {}...done", pipelineTask);
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void postProcessingAfterException() {
        if (pipelineTask.getId() != null) {
            // could be null if it wasn't found in the db
            try {
                postProcessing(true, false);
            } catch (Exception e) {
                log.error("Failed in postProcessing for {}", pipelineTask.toFullString(), e);
            }
        }
    }

    /**
     * Update the PipelineTask in the db to reflect the fact that this worker is now processing the
     * task.
     */
    private boolean preProcessing() {
        pipelineTaskDataOperations().addTaskExecutionLog(pipelineTask,
            AbstractPipelineProcess.getProcessInfo().getHost(), workerNumber,
            processingStartTimeMillis);
        ProcessingStep processingStep = pipelineTaskDataOperations().processingStep(pipelineTask);

        // If the user requested that only the transition logic be re-run, or if the transition
        // logic previously failed, then we only need to re-run the transition logic
        boolean transitionOnly = processingStep == ProcessingStep.COMPLETE;

        if (!transitionOnly) {
            pipelineTaskDataOperations().updateWorkerInfo(pipelineTask, getProcessInfo().getHost(),
                workerNumber);
            pipelineTaskDataOperations().updateZiggySoftwareRevision(pipelineTask,
                BuildInfo.ziggyVersion());
            pipelineTaskDataOperations().updatePipelineSoftwareRevision(pipelineTask,
                BuildInfo.pipelineVersion());

            // If this is the first time we've called the module, set the processing step to the
            // first step that the module defines; otherwise, leave it alone and let the module pick
            // up where it left off.
            if (processingStep.isInfrastructureStep()) {
                ProcessingStep firstProcessingStep = pipelineTaskOperations()
                    .moduleImplementation(pipelineTask, runMode)
                    .processingSteps()
                    .get(0);
                outgoingMessageLatch = new CountDownLatch(1);
                ZiggyMessenger.publish(
                    new UpdateProcessingStepMessage(pipelineTask, firstProcessingStep),
                    outgoingMessageLatch);
                try {
                    outgoingMessageLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
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
            log.error("Failed to process {}", pipelineTask.toFullString());
            CounterMetric.increment("pipeline.module.execFailCount");
            throw e;
        } finally {

            if (moduleExecMetricPrefix != null) {
                CounterMetric.increment(moduleExecMetricPrefix + ".execCount");
            }
        }
    }

    private boolean processTaskInternal() throws Exception {

        workerTask = pipelineTask;
        log.info("Processing {}", pipelineTask.toFullString());

        currentPipelineModule = pipelineTaskOperations().moduleImplementation(pipelineTask,
            runMode);

        String moduleSimpleName = currentPipelineModule.getClass().getSimpleName();

        log.info("Calling {}.processTask()", moduleSimpleName);
        pipelineModuleProcessTask(currentPipelineModule,
            "pipeline.module." + pipelineTask.getModuleName());
        log.info("Calling {}.processTask()...done", moduleSimpleName);

        if (isTaskDone()) {
            outgoingMessageLatch = new CountDownLatch(1);
            ZiggyMessenger.publish(
                new UpdateProcessingStepMessage(pipelineTask, ProcessingStep.COMPLETE),
                outgoingMessageLatch);
            try {
                outgoingMessageLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return isTaskDone();
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void pipelineModuleProcessTask(PipelineModule currentPipelineModule,
        String moduleExecMetricPrefix) throws Exception {

        IntervalMetricKey key = IntervalMetric.start();
        try {
            // Hand off control to the PipelineModule implementation.
            setTaskDone(currentPipelineModule.processTask());
        } finally {
            IntervalMetric.stop(moduleExecMetricPrefix + ".processTask", key);
        }
    }

    /**
     * Update the PipelineTask in the db with the results of the processing
     */
    private void postProcessing(boolean done, boolean success) {

        long processingEndTimeMillis = System.currentTimeMillis();
        long totalProcessingTimeMillis = processingEndTimeMillis - processingStartTimeMillis;

        log.info("Processing for this step took {} minutes",
            totalProcessingTimeMillis / 1000.0 / 60.0);

        Date endProcessingTime = new Date(processingEndTimeMillis);

        // Update summary metrics.
        currentPipelineModule.updateMetrics(pipelineTask, taskMetrics, totalProcessingTimeMillis);

        pipelineTaskDataOperations().updateLastTaskExecutionLog(pipelineTask, endProcessingTime);

        if (!done) {
            return;
        }
        if (success) {
            outgoingMessageLatch = new CountDownLatch(1);
            ZiggyMessenger.publish(
                new UpdateProcessingStepMessage(pipelineTask, ProcessingStep.COMPLETE),
                outgoingMessageLatch);
            try {
                outgoingMessageLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            pipelineTaskDataOperations().incrementFailureCount(pipelineTask);
            pipelineTaskDataOperations().taskErrored(pipelineTask);

            AlertService alertService = AlertService.getInstance();
            alertService.generateAlert("PI(" + pipelineTask.getModuleName() + ")", pipelineTask,
                Severity.INFRASTRUCTURE, lastErrorMessage);
        }
    }

    private void setTaskDone(boolean taskDone) {
        this.taskDone = taskDone;
    }

    private boolean isTaskDone() {
        return taskDone;
    }

    @Override
    public StatusMessage reportCurrentStatus() {
        return statusMessage(false);
    }

    public StatusMessage statusMessage(boolean lastMessage) {
        Long pipelineInstanceId = pipelineTask.getPipelineInstanceId();
        String currentPipelineInstanceId = pipelineInstanceId == null ? "-"
            : "" + pipelineInstanceId.toString();

        WorkerStatusMessage message = new WorkerStatusMessage(workerNumber, state.toString(),
            currentPipelineInstanceId, pipelineTask, pipelineTask.getModuleName(),
            pipelineTask.getUnitOfWork().briefState(), processingStartTimeMillis, lastMessage);
        message.setSourceProcess(getProcessInfo());
        return message;
    }

    public static PipelineTask getWorkerTask() {
        return workerTask;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }
}
