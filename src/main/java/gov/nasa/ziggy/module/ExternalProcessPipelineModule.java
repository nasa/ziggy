package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.module.PipelineCategories.LOCAL_CATEGORIES;
import static gov.nasa.ziggy.module.PipelineCategories.LOCAL_CATEGORY_UNITS;
import static gov.nasa.ziggy.module.PipelineCategories.REMOTE_CATEGORIES;
import static gov.nasa.ziggy.module.PipelineCategories.REMOTE_CATEGORY_UNITS;
import static gov.nasa.ziggy.module.PipelineMetrics.CREATE_INPUTS_METRIC;
import static gov.nasa.ziggy.module.PipelineMetrics.LOCAL_METRICS;
import static gov.nasa.ziggy.module.PipelineMetrics.PENDING_RECEIVE_METRIC;
import static gov.nasa.ziggy.module.PipelineMetrics.PLEIADES_QUEUE_METRIC;
import static gov.nasa.ziggy.module.PipelineMetrics.PLEIADES_WALL_METRIC;
import static gov.nasa.ziggy.module.PipelineMetrics.REMOTE_METRICS;
import static gov.nasa.ziggy.module.PipelineMetrics.REMOTE_WORKER_WAIT_METRIC;
import static gov.nasa.ziggy.module.PipelineMetrics.STORE_OUTPUTS_METRIC;
import static gov.nasa.ziggy.services.database.DatabaseTransactionFactory.performTransaction;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.metrics.ValueMetric;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.module.remote.TimestampFile;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.ProcessingStatePipelineModule;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Pipeline modules that executes an external process, either directly via a command line run in a
 * shell on the system that hosts the worker process, or indirectly via submission to a job queue.
 * The methods in this class support marshaling, execution, persisting, and restart of failed tasks.
 *
 * @author PT
 */
public class ExternalProcessPipelineModule extends PipelineModule
    implements ProcessingStatePipelineModule {

    private static final Logger log = LoggerFactory.getLogger(ExternalProcessPipelineModule.class);

    /**
     * List of valid processing states
     */
    static final List<ProcessingState> PROCESSING_STATES = ImmutableList.of(
        ProcessingState.INITIALIZING, ProcessingState.MARSHALING,
        ProcessingState.ALGORITHM_SUBMITTING, ProcessingState.ALGORITHM_QUEUED,
        ProcessingState.ALGORITHM_EXECUTING, ProcessingState.ALGORITHM_COMPLETE,
        ProcessingState.STORING, ProcessingState.COMPLETE);

    // Instance members
    private AlgorithmLifecycle algorithmManager;
    private long instanceId;
    private TaskConfigurationManager taskConfigurationManager;
    private String haltStep = "C";
    private PipelineInputs pipelineInputs;
    private PipelineOutputs pipelineOutputs;

    protected boolean processingSuccessful;
    protected boolean doneLooping;

    /**
     * Copy datastore files needed as inputs to the specified working directory.
     */
    protected void copyDatastoreFilesToTaskDirectory(
        TaskConfigurationManager taskConfigurationManager, PipelineTask pipelineTask,
        File taskWorkingDirectory) {
        pipelineInputs.copyDatastoreFilesToTaskDirectory(taskConfigurationManager, pipelineTask,
            taskWorkingDirectory.toPath());
        processingSummaryOperations().updateSubTaskCounts(pipelineTask.getId(),
            taskConfigurationManager.getSubtaskCount(), 0, 0);
    }

    /**
     * Process and store the algorithm result(s), and delete the temporary copies of datastore files
     * in the task directory; update input files in the database with new consumers, if necessary.
     */
    protected void persistResultsAndDeleteTempFiles(PipelineTask pipelineTask,
        ProcessingFailureSummary failureSummary) {
        pipelineInputs.deleteTempInputsFromTaskDirectory(pipelineTask, getTaskDir().toPath());
        pipelineOutputs.copyTaskDirectoryResultsToDatastore(
            pipelineInputs.datastorePathLocator(pipelineTask), pipelineTask, getTaskDir().toPath());
        pipelineOutputs.updateInputFileConsumers(pipelineInputs, pipelineTask,
            getTaskDir().toPath());
    }

    // Constructor
    public ExternalProcessPipelineModule(PipelineTask pipelineTask, RunMode runMode) {
        super(pipelineTask, runMode);
        instanceId = pipelineTask.getPipelineInstance().getId();
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        haltStep = config.getString(PropertyName.PIPELINE_HALT.property(), "C");

        PipelineModuleDefinition pipelineModuleDefinition = pipelineTask.getPipelineInstanceNode()
            .getPipelineModuleDefinition();
        ClassWrapper<PipelineInputs> inputsClass = pipelineModuleDefinition.getInputsClass();
        pipelineInputs = inputsClass.newInstance();
        ClassWrapper<PipelineOutputs> outputsClass = pipelineModuleDefinition.getOutputsClass();
        pipelineOutputs = outputsClass.newInstance();
    }

    /**
     * Indicates that this class should execute {@link processTask} outside of a database
     * transaction.
     */
    @Override
    public boolean processTaskRequiresDatabaseTransaction() {
        return false;
    }

    /**
     * Main processing method accessible to callers.
     */
    @Override
    public boolean processTask() {

        runMode.run(this);
        return processingSuccessful;
    }

    @Override
    public List<RunMode> restartModes() {
        return Arrays.asList(RunMode.RESTART_FROM_BEGINNING, RunMode.RESUBMIT,
            RunMode.RESUME_CURRENT_STEP, RunMode.RESUME_MONITORING);
    }

    // Concrete, non-override methods

    protected File getTaskDir() {
        return algorithmManager().getTaskDir(false);
    }

    /**
     * Performs necessary processing for the INITIALIZING processing state. In this case, all that
     * means is advancing to the MARSHALING processing state.
     */
    @Override
    public void initializingTaskAction() {
        checkHaltRequest(ProcessingState.INITIALIZING);
        incrementProcessingState();
        processingSuccessful = false;
    }

    public void checkHaltRequest(ProcessingState state) {
        String stateShortName = state.shortName();
        if (haltStep.equals(stateShortName)) {
            throw new PipelineException("Halting processing at end of step " + state.toString()
                + " due to configuration request for halt after step " + haltStep);
        }
    }

    /**
     * Performs inputs marshaling for MARSHALING processing state, also clear all existing producer
     * task IDs and update the PipelineTask instance after new producer task IDs are set. Updates
     * the processing state to the ALGORITHM_SUBMITTING.
     */
    @Override
    public void marshalingTaskAction() {

        log.info("Processing step: MARSHALING");
        boolean successful;
        File taskDir = algorithmManager().getTaskDir(true);
        performTransaction(() -> {
            IntervalMetric.measure(CREATE_INPUTS_METRIC, () -> {

                // Note: here we retrieve a copy of the pipeline task from the database and
                // update its producer task IDs. The existing copy of the task in this object
                // is also replaced with the updated task.
                pipelineTask = pipelineTaskCrud().retrieve(taskId());
                pipelineTask.clearProducerTaskIds();
                copyDatastoreFilesToTaskDirectory(taskConfigurationManager(), pipelineTask,
                    taskDir);
            });
            taskConfigurationManager().validate();
            return null;
        });

        if (!taskConfigurationManager().isEmpty()) {
            taskConfigurationManager().persist(taskDir);

            checkHaltRequest(ProcessingState.MARSHALING);
            // Set the next state, whatever it might be
            incrementProcessingState();

            // if there are sub-task inputs, then we can go on to the next step...
            successful = true;
        } else {

            // If there are no sub-task inputs, we should stop processing.
            successful = false;
            checkHaltRequest(ProcessingState.MARSHALING);
        }
        log.info("Processing step MARSHALING complete");
        doneLooping = !successful;
        processingSuccessful = doneLooping;
    }

    /**
     * Perform the necessary processing for state ALGORITHM_SUBMITTING. This is just calling the
     * algorithm execution method in the algorithm lifecycle object. The processing state is not
     * advanced by this method, it will be advanced by the PBS submission infrastructure if
     * successful.
     */
    @Override
    public void submittingTaskAction() {

        log.info("Processing step: SUBMITTING");
        TaskConfigurationManager tcm = null;
        if (runMode.equals(RunMode.STANDARD)) {
            tcm = taskConfigurationManager();
        }
        algorithmManager().executeAlgorithm(tcm);
        checkHaltRequest(ProcessingState.ALGORITHM_SUBMITTING);
        doneLooping = true;
        processingSuccessful = false;
    }

    /**
     * Perform the necessary processing for state ALGORITHM_QUEUED. The only way that this method is
     * ever called is if task processing somehow failed while in the queued state. In this case, the
     * desired action is to resubmit the task.
     */
    @Override
    public void queuedTaskAction() {

        log.info("Resubmitting algorithm to remote system");
        algorithmManager().executeAlgorithm(null);
        checkHaltRequest(ProcessingState.ALGORITHM_QUEUED);
        doneLooping = true;
        processingSuccessful = false;
    }

    /**
     * Perform the necessary processing for state ALGORITHM_EXECUTING. The only way that this method
     * is ever called is if task processing somehow failed during algorithm execution. In this case,
     * the desired action is to resubmit the task.
     */
    @Override
    public void executingTaskAction() {

        log.info("Resubmitting algorithm to remote system");
        algorithmManager().executeAlgorithm(null);
        checkHaltRequest(ProcessingState.ALGORITHM_EXECUTING);
        doneLooping = true;
        processingSuccessful = false;
    }

    /**
     * Perform the necessary processing for state ALGORITHM_COMPLETE. For all algorithm types, this
     * simply advances the processing state to the next one after complete.
     */
    @Override
    public void algorithmCompleteTaskAction() {

        checkHaltRequest(ProcessingState.ALGORITHM_COMPLETE);
        incrementProcessingState();
        processingSuccessful = false;
    }

    /**
     * Perform the necessary processing for state STORING: specifically, store results in the
     * database and/or datastore; update wall time metrics for remote tasks; advance the processing
     * state to COMPLETE.
     */
    @Override
    public void storingTaskAction() {

        log.info("Processing step: STORING");
        if (algorithmManager().isRemote()) {
            long startTransferTime = System.currentTimeMillis();

            // add metrics for "RemoteWorker", "PleiadesQueue", "Matlab",
            // "PendingReceive"
            long remoteWorkerTime = timestampFileElapsedTimeMillis(TimestampFile.Event.ARRIVE_PFE,
                TimestampFile.Event.QUEUED_PBS);
            long pleiadesQueueTime = timestampFileElapsedTimeMillis(TimestampFile.Event.QUEUED_PBS,
                TimestampFile.Event.PBS_JOB_START);
            long pleiadesWallTime = timestampFileElapsedTimeMillis(
                TimestampFile.Event.PBS_JOB_START, TimestampFile.Event.PBS_JOB_FINISH);
            long pendingReceiveTime = startTransferTime
                - timestampFileTimestamp(TimestampFile.Event.PBS_JOB_FINISH);

            log.info("remoteWorkerTime = " + remoteWorkerTime);
            log.info("pleiadesQueueTime = " + pleiadesQueueTime);
            log.info("pleiadesWallTime = " + pleiadesWallTime);
            log.info("pendingReceiveTime = " + pendingReceiveTime);

            valueMetricAddValue(REMOTE_WORKER_WAIT_METRIC, remoteWorkerTime);
            valueMetricAddValue(PLEIADES_QUEUE_METRIC, pleiadesQueueTime);
            valueMetricAddValue(PLEIADES_WALL_METRIC, pleiadesWallTime);
            valueMetricAddValue(PENDING_RECEIVE_METRIC, pendingReceiveTime);
        }
        ProcessingFailureSummary failureSummary = processingFailureSummary();
        boolean abandonPersisting = false;
        if (!failureSummary.isAllTasksSucceeded() && !failureSummary.isAllTasksFailed()) {
            log.info("Sub-task failures occurred. List of sub-task failures follows:");
            for (String failedSubTask : failureSummary.getFailedSubTaskDirs()) {
                log.info("    " + failedSubTask);
            }
            ImmutableConfiguration config = ZiggyConfiguration.getInstance();
            boolean allowPartialTasks = config
                .getBoolean(PropertyName.ALLOW_PARTIAL_TASKS.property(), true);
            abandonPersisting = !allowPartialTasks;
        }
        if (failureSummary.isAllTasksFailed()) {
            log.info("All sub-tasks failed in processing, abandoning storage of results");
            abandonPersisting = true;
        }
        if (abandonPersisting) {
            throw new PipelineException("Unable to persist due to sub-task failures");
        }

        performTransaction(() -> {
            IntervalMetric.measure(STORE_OUTPUTS_METRIC, () -> {
                // process outputs
                persistResultsAndDeleteTempFiles(pipelineTask, failureSummary);
                return null;
            });
            return null;
        });

        log.info("Checking for input files that produced no output");

        // Get the names of the input files
        Set<Path> inputPaths = pipelineInputs().findDatastoreFilesForInputs(pipelineTask);
        Set<String> inputFiles = inputPaths.stream()
            .map(Path::toString)
            .collect(Collectors.toSet());

        // Get the names of the files successfully consumed by this task
        @SuppressWarnings("unchecked")
        Set<String> consumedFiles = (Set<String>) performTransaction(
            () -> datastoreProducerConsumerCrud().retrieveFilesConsumedByTask(taskId()));

        // Remove the latter set from the former
        inputFiles.removeAll(consumedFiles);

        // If anything is left, write to the log and generate an alert
        if (inputFiles.isEmpty()) {
            log.info("All input files produced output");
        } else {
            log.warn(inputFiles.size() + " input files produced no output");
            for (String inputFile : inputFiles) {
                log.warn("Input file " + inputFile + " produced no output");
            }
            AlertService.getInstance()
                .generateAndBroadcastAlert("Algorithm", taskId(), AlertService.Severity.WARNING,
                    inputFiles.size() + " input files produced no output, see log for details");
        }

        // Finally, update status
        performTransaction(() -> {
            checkHaltRequest(ProcessingState.STORING);
            incrementProcessingState();
            return null;
        });
        doneLooping = true;
        processingSuccessful = true;
    }

    @Override
    public void processingCompleteTaskAction() {
        doneLooping = true;
        processingSuccessful = true;
    }

    /**
     * Master processing loop for tasks. Each task is advanced through all of its defined states,
     * and at each state appropriate actions are taken. The loop exits if any of the following
     * occur:
     * <ol>
     * <li>Marshaling returns unsuccessful status (i.e., no task directories created)
     * <li>Submitting is successful -- this happens for remote tasks, since the stepping from
     * submitted to queued to executing to completed happens in the remote system, not in this loop
     * <li>re-submission of a remote task that failed in Queued or Executing states is successful
     * <li>Storing of outputs is successful
     * <li>The {@link Thread} is interrupted, indicating that the current task is to be deleted.
     * <li>Task is completed.
     * </ol>
     */
    @Override
    public void processingMainLoop() {

        // initialize the loop variable and the return variable
        doneLooping = false;
        processingSuccessful = false;

        while (!doneLooping) {

            // Perform the current action (including advancing to the next
            // processing state, if appropriate).
            getProcessingState().taskAction(this);
        }
    }

    @Override
    protected void restartFromBeginning() {
        processingSummaryOperations().updateProcessingState(taskId(), ProcessingState.INITIALIZING);
        processingMainLoop();
    }

    @Override
    protected void resumeCurrentStep() {
        processingMainLoop();
    }

    @Override
    protected void resubmit() {
        processingSummaryOperations().updateProcessingState(taskId(),
            ProcessingState.ALGORITHM_SUBMITTING);
        processingMainLoop();
    }

    @Override
    protected void resumeMonitoring() {
        algorithmManager().getExecutor().resumeMonitoring();
    }

    @Override
    protected void runStandard() {
        processingMainLoop();
    }

    @Override
    public void updateMetrics(PipelineTask pipelineTask, Map<String, Metric> threadMetrics,
        long overallExecTimeMillis) {

        if (!pipelineTask.equals(this.pipelineTask)) {
            throw new PipelineException("processTask called with incorrect pipeline task");
        }

        List<PipelineTaskMetrics> summaryMetrics = pipelineTask.getSummaryMetrics();

        log.debug("Thread Metrics:");
        for (String threadMetricName : threadMetrics.keySet()) {
            log.debug("TM: " + threadMetricName + ": "
                + threadMetrics.get(threadMetricName).getLogString());
        }

        // cross-reference existing summary metrics by category
        Map<String, PipelineTaskMetrics> summaryMetricsByCategory = new HashMap<>();
        for (PipelineTaskMetrics summaryMetric : summaryMetrics) {
            summaryMetricsByCategory.put(summaryMetric.getCategory(), summaryMetric);
        }

        String[] categories;
        String[] metrics;
        Units[] units;

        if (algorithmManager.isRemote()) {
            categories = REMOTE_CATEGORIES;
            metrics = REMOTE_METRICS;
            units = REMOTE_CATEGORY_UNITS;
        } else {
            categories = LOCAL_CATEGORIES;
            metrics = LOCAL_METRICS;
            units = LOCAL_CATEGORY_UNITS;
        }

        for (int i = 0; i < categories.length; i++) {
            String category = categories[i];
            String metricName = metrics[i];
            Units unit = units[i];

            long totalTime = 0;

            Metric metric = threadMetrics.get(metricName);
            if (metric instanceof ValueMetric) {
                ValueMetric iMetric = (ValueMetric) metric;
                totalTime = iMetric.getSum();
            } else {
                log.info("Module did not provide metric with name = " + metricName);
            }

            log.info("TaskID: " + pipelineTask.getId() + ", category: " + category + ", time(ms): "
                + totalTime);

            PipelineTaskMetrics m = summaryMetricsByCategory.get(category);
            if (m == null) {
                m = new PipelineTaskMetrics(category, totalTime, unit);
                summaryMetrics.add(m);
            }

            // don't overwrite the existing value if no value was recorded for
            // this category
            // on this invocation
            if (totalTime > 0) {
                m.setValue(totalTime);
            }
        }
        pipelineTask.setSummaryMetrics(summaryMetrics);
    }

    /**
     * Determines the correct array of processing states and converts to a list.
     */
    @Override
    public List<ProcessingState> processingStates() {

        return PROCESSING_STATES;
    }

    // Methods that construct instances of classes used by the methods above.
    // This technique is used to allow the methods to be overridden by a
    // test class (so that mocked objects are returned). Methods are
    // package-private so that non-test instances of ExternalProcessPipelineModule
    // cannot override them.

    PipelineTaskCrud pipelineTaskCrud() {
        return new PipelineTaskCrud();
    }

    AlgorithmExecutor executor() {
        return algorithmManager().getExecutor();
    }

    StateFile generateStateFile() {
        TaskConfigurationManager tcm = taskConfigurationManager();
        PbsParameters pbsParameters = executor().generatePbsParameters(
            pipelineTask.getParameters(RemoteParameters.class), tcm.numSubTasks());
        return StateFile.generateStateFile(pipelineTask, pbsParameters, tcm.numSubTasks());
    }

    long timestampFileElapsedTimeMillis(TimestampFile.Event startEvent,
        TimestampFile.Event finishEvent) {
        return TimestampFile.elapsedTimeMillis(getTaskDir(), startEvent, finishEvent);
    }

    long timestampFileTimestamp(TimestampFile.Event event) {
        return TimestampFile.timestamp(getTaskDir(), event);
    }

    ValueMetric valueMetricAddValue(String name, long value) {
        return ValueMetric.addValue(name, value);
    }

    ProcessingFailureSummary processingFailureSummary() {
        return new ProcessingFailureSummary(pipelineTask.getModuleName(), getTaskDir());
    }

    public AlgorithmLifecycle algorithmManager() {
        if (algorithmManager == null) {
            algorithmManager = new AlgorithmLifecycleManager(pipelineTask);
        }
        return algorithmManager;
    }

    TaskConfigurationManager taskConfigurationManager() {
        if (taskConfigurationManager == null) {
            taskConfigurationManager = new TaskConfigurationManager(getTaskDir());
            taskConfigurationManager.setInputsClass(pipelineInputs.getClass());
            taskConfigurationManager.setOutputsClass(pipelineOutputs.getClass());
        }
        return taskConfigurationManager;
    }

    public long instanceId() {
        return instanceId;
    }

    @Override
    public String getModuleName() {
        return pipelineTask.getModuleName();
    }

    /** For testing only. */
    PipelineInputs pipelineInputs() {
        return pipelineInputs;
    }

    /** For testing only. */
    PipelineOutputs pipelineOutputs() {
        return pipelineOutputs;
    }

    /** For testing only. */
    boolean isProcessingSuccessful() {
        return processingSuccessful;
    }

    /** For testing only. */
    DatastoreProducerConsumerCrud datastoreProducerConsumerCrud() {
        return new DatastoreProducerConsumerCrud();
    }

    /** For testing only. */
    boolean getDoneLooping() {
        return doneLooping;
    }

    /** For testing only. */
    void setDoneLooping(boolean doneLooping) {
        this.doneLooping = doneLooping;
    }

    @Override
    public long pipelineTaskId() {
        return taskId();
    }
}
