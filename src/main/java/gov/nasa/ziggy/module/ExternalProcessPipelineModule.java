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

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager.InputFiles;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerOperations;
import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.metrics.ValueMetric;
import gov.nasa.ziggy.module.remote.TimestampFile;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Pipeline modules that executes an external process, either directly via a command line run in a
 * shell on the system that hosts the worker process, or indirectly via submission to a job queue.
 * The methods in this class support marshaling, execution, persisting, and restart of failed tasks.
 *
 * @author PT
 */
public class ExternalProcessPipelineModule extends PipelineModule {

    private static final Logger log = LoggerFactory.getLogger(ExternalProcessPipelineModule.class);

    /**
     * List of valid processing steps.
     */
    static final List<ProcessingStep> PROCESSING_STEPS = List.of(ProcessingStep.MARSHALING,
        ProcessingStep.SUBMITTING, ProcessingStep.QUEUED, ProcessingStep.EXECUTING,
        ProcessingStep.WAITING_TO_STORE, ProcessingStep.STORING);

    // Instance members
    private AlgorithmLifecycle algorithmManager;
    private long instanceId;
    private TaskConfiguration taskConfiguration;
    private PipelineInputs pipelineInputs;
    private PipelineOutputs pipelineOutputs;

    protected boolean processingSuccessful;
    protected boolean doneLooping;
    private DatastoreProducerConsumerOperations datastoreProducerConsumerOperations = new DatastoreProducerConsumerOperations();

    public ExternalProcessPipelineModule(PipelineTask pipelineTask, RunMode runMode) {
        super(pipelineTask, runMode);
        instanceId = pipelineTask.getPipelineInstanceId();
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
        return List.of(RunMode.RESTART_FROM_BEGINNING, RunMode.RESUBMIT,
            RunMode.RESUME_CURRENT_STEP, RunMode.RESUME_MONITORING);
    }

    protected File getTaskDir() {
        return algorithmManager().getTaskDir(false);
    }

    public void checkHaltRequest(ProcessingStep step) {
        if (step == haltStep()) {
            throw new PipelineException("Halting processing at end of step " + step
                + " due to setting of " + PropertyName.PIPELINE_HALT.property());
        }
    }

    private ProcessingStep haltStep() {
        return ProcessingStep.valueOf(ZiggyConfiguration.getInstance()
            .getString(PropertyName.PIPELINE_HALT.property(), ProcessingStep.COMPLETE.toString()));
    }

    /**
     * Performs inputs marshaling for MARSHALING processing step, also clear all existing producer
     * task IDs and update the PipelineTask instance after new producer task IDs are set. Updates
     * the processing step to SUBMITTING.
     */
    @Override
    public void marshalingTaskAction() {

        log.info("Processing step {}", ProcessingStep.MARSHALING);
        boolean successful;
        File taskDir = algorithmManager().getTaskDir(true);
        IntervalMetric.measure(CREATE_INPUTS_METRIC, () -> {

            // Note: here we retrieve a copy of the pipeline task from the database and
            // update its producer task IDs. The existing copy of the task in this object
            // is also replaced with the updated task.
//            pipelineTask = datastoreProducerConsumerOperations()
//                .clearProducerTaskIds(pipelineTask().getId());
            copyFilesToTaskDirectory(taskConfiguration(), taskDir);
        });

        if (taskConfiguration().getSubtaskCount() != 0) {
            taskConfiguration().serialize(taskDir);

            checkHaltRequest(ProcessingStep.MARSHALING);

            // Set the next step, whatever it might be.
            incrementProcessingStep();

            // If there are sub-task inputs, then we can go on to the next step.
            successful = true;
        } else {

            // If there are no sub-task inputs, we should stop processing.
            successful = false;
            checkHaltRequest(ProcessingStep.MARSHALING);
        }
        doneLooping = !successful;
        processingSuccessful = doneLooping;
        log.info("Processing step {}...done", ProcessingStep.MARSHALING);
    }

    /**
     * Copy datastore files needed as inputs to the specified working directory.
     */
    public void copyFilesToTaskDirectory(TaskConfiguration taskConfiguration,
        File taskWorkingDirectory) {
        pipelineInputs().copyDatastoreFilesToTaskDirectory(taskConfiguration,
            taskWorkingDirectory.toPath());
        pipelineTaskOperations().updateSubtaskCounts(pipelineTask().getId(),
            taskConfiguration.getSubtaskCount(), 0, 0);
    }

    /**
     * Perform the necessary processing for step SUBMITTING. This is just calling the algorithm
     * execution method in the algorithm lifecycle object. The processing state is not advanced by
     * this method, it will be advanced by the PBS submission infrastructure if successful.
     */
    @Override
    public void submittingTaskAction() {

        log.info("Processing step {}", ProcessingStep.SUBMITTING);
        TaskConfiguration taskConfiguration = null;
        if (runMode.equals(RunMode.STANDARD)) {
            taskConfiguration = taskConfiguration();
        }
        algorithmManager().executeAlgorithm(taskConfiguration);
        checkHaltRequest(ProcessingStep.SUBMITTING);
        doneLooping = true;
        processingSuccessful = false;
        log.info("Processing step {}...done", ProcessingStep.SUBMITTING);
    }

    /**
     * Perform the necessary processing for step QUEUED. The only way that this method is ever
     * called is if task processing somehow failed while in the queued step. In this case, the
     * desired action is to resubmit the task.
     */
    @Override
    public void queuedTaskAction() {

        log.info("Resubmitting {} algorithm to remote system", ProcessingStep.QUEUED);
        algorithmManager().executeAlgorithm(null);
        checkHaltRequest(ProcessingStep.QUEUED);
        doneLooping = true;
        processingSuccessful = false;
        log.info("Resubmitting {} algorithm to remote system...done", ProcessingStep.QUEUED);
    }

    /**
     * Perform the necessary processing for step EXECUTING. The only way that this method is ever
     * called is if task processing somehow failed during algorithm execution. In this case, the
     * desired action is to resubmit the task.
     */
    @Override
    public void executingTaskAction() {

        log.info("Resubmitting {} algorithm to remote system", ProcessingStep.EXECUTING);
        algorithmManager().executeAlgorithm(null);
        checkHaltRequest(ProcessingStep.EXECUTING);
        doneLooping = true;
        processingSuccessful = false;
        log.info("Resubmitting {} algorithm to remote system", ProcessingStep.EXECUTING);
    }

    /**
     * Perform the necessary processing for step WAITING_TO_STORE. For all algorithm types, this
     * simply advances the processing step to the next one after complete.
     */
    @Override
    public void waitingToStoreTaskAction() {

        checkHaltRequest(ProcessingStep.WAITING_TO_STORE);
        incrementProcessingStep();
        processingSuccessful = false;
    }

    /**
     * Perform the necessary processing for step STORING: specifically, store results in the
     * database and/or datastore; update wall time metrics for remote tasks. The infrastructure will
     * pick things up from here.
     */
    @Override
    public void storingTaskAction() {

        log.info("Processing step {}", ProcessingStep.STORING);
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

        IntervalMetric.measure(STORE_OUTPUTS_METRIC, () -> {
            // process outputs
            persistResultsAndUpdateConsumers();
            return null;
        });

        // TODO This message seems orphaned
        log.info("Checking for input files that produced no output");

        // Finally, update status
        checkHaltRequest(ProcessingStep.STORING);
        doneLooping = true;
        processingSuccessful = true;
        log.info("Processing step {}...done", ProcessingStep.STORING);
    }

    /** Process and store the algorithm outputs and update producer-consumer database table. */
    void persistResultsAndUpdateConsumers() {
        Set<Path> outputFiles = pipelineOutputs().copyTaskFilesToDatastore();

        log.info("Creating producer information for output files");
        datastoreProducerConsumerOperations().createOrUpdateProducer(pipelineTask(),
            datastorePathsToRelative(outputFiles));
        log.info("Creating producer information for output files...done");

        log.info("Updating consumer information for input files");
        InputFiles inputFiles = datastoreFileManager().inputFilesByOutputStatus();
        datastoreProducerConsumerOperations().addConsumer(pipelineTask(),
            datastorePathsToNames(inputFiles.getFilesWithOutputs()));
        datastoreProducerConsumerOperations().addNonProducingConsumer(pipelineTask(),
            datastorePathsToNames(inputFiles.getFilesWithoutOutputs()));
        log.info("Updating consumer information for input files...done");

        if (inputFiles.getFilesWithoutOutputs().isEmpty()) {
            log.info("All input files produced output");
        } else {
            log.warn("{} input files produced no output",
                inputFiles.getFilesWithoutOutputs().size());
            for (Path inputFile : inputFiles.getFilesWithoutOutputs()) {
                log.warn("Input file {} produced no output", inputFile.toString());
            }
            AlertService.getInstance()
                .generateAndBroadcastAlert("Algorithm", taskId(), AlertService.Severity.WARNING,
                    inputFiles.getFilesWithoutOutputs()
                        + " input files produced no output, see log for details");
        }
    }

    Set<Path> datastorePathsToRelative(Set<Path> datastorePaths) {
        return datastorePaths.stream()
            .map(s -> DirectoryProperties.datastoreRootDir().toAbsolutePath().relativize(s))
            .collect(Collectors.toSet());
    }

    Set<String> datastorePathsToNames(Set<Path> datastorePaths) {
        return datastorePaths.stream()
            .map(s -> DirectoryProperties.datastoreRootDir().toAbsolutePath().relativize(s))
            .map(Path::toString)
            .collect(Collectors.toSet());
    }

    /**
     * Master processing loop for tasks. Each task is advanced through all of its defined steps, and
     * at each step appropriate actions are taken. The loop exits if any of the following occur:
     * <ol>
     * <li>Marshaling returns unsuccessful status (i.e., no task directories created)
     * <li>Submitting is successful -- this happens for remote tasks, since the stepping from
     * submitted to queued to executing to completed happens in the remote system, not in this loop
     * <li>re-submission of a remote task that failed in Queued or Executing steps is successful
     * <li>Storing of outputs is successful
     * <li>The {@link Thread} is interrupted, indicating that the current task is to be deleted.
     * <li>Task is completed.
     * </ol>
     */
    void processingMainLoop() {

        // initialize the loop variable and the return variable
        doneLooping = false;
        processingSuccessful = false;

        while (!doneLooping) {

            // Perform the current action (including advancing to the next
            // processing step, if appropriate).
            ProcessingStep processingStep = currentProcessingStep();
            processingStep.taskAction(this);
        }
    }

    @Override
    protected void restartFromBeginning() {
        pipelineTaskOperations().updateProcessingStep(taskId(), processingSteps().get(0));
        processingMainLoop();
    }

    @Override
    protected void resumeCurrentStep() {
        processingMainLoop();
    }

    @Override
    protected void resubmit() {
        pipelineTaskOperations().updateProcessingStep(taskId(), ProcessingStep.SUBMITTING);
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

        if (!pipelineTask.equals(pipelineTask())) {
            throw new PipelineException("processTask called with incorrect pipeline task");
        }

        List<PipelineTaskMetrics> summaryMetrics = pipelineTaskOperations()
            .summaryMetrics(pipelineTask);

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

            log.info("TaskID={}, category={}, time(ms)={}", pipelineTask.getId(), category,
                totalTime);

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
        pipelineTaskOperations().merge(pipelineTask);
    }

    /**
     * Returns the valid processing steps for this pipeline module.
     */
    @Override
    public List<ProcessingStep> processingSteps() {
        return PROCESSING_STEPS;
    }

    // Methods that construct instances of classes used by the methods above.
    // This technique is used to allow the methods to be overridden by a
    // test class (so that mocked objects are returned). Methods are
    // package-private so that non-test instances of ExternalProcessPipelineModule
    // cannot override them.

    AlgorithmExecutor executor() {
        return algorithmManager().getExecutor();
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
        return new ProcessingFailureSummary(pipelineTask().getModuleName(), getTaskDir());
    }

    public AlgorithmLifecycle algorithmManager() {
        if (algorithmManager == null) {
            algorithmManager = new AlgorithmLifecycleManager(pipelineTask());
        }
        return algorithmManager;
    }

    TaskConfiguration taskConfiguration() {
        if (taskConfiguration == null) {
            taskConfiguration = new TaskConfiguration(getTaskDir());
            taskConfiguration.setInputsClass(pipelineInputs().getClass());
            taskConfiguration.setOutputsClass(pipelineOutputs().getClass());
        }
        return taskConfiguration;
    }

    public long instanceId() {
        return instanceId;
    }

    @Override
    public String getModuleName() {
        return pipelineTask().getModuleName();
    }

    PipelineModuleDefinition pipelineModuleDefinition() {
        return pipelineTaskOperations().pipelineModuleDefinition(pipelineTask());
    }

    PipelineInputs pipelineInputs() {
        if (pipelineInputs == null) {
            pipelineInputs = PipelineInputsOutputsUtils.newPipelineInputs(
                pipelineModuleDefinition().getInputsClass(), pipelineTask(),
                taskDirManager().taskDir());
        }
        return pipelineInputs;
    }

    PipelineOutputs pipelineOutputs() {
        if (pipelineOutputs == null) {
            pipelineOutputs = PipelineInputsOutputsUtils.newPipelineOutputs(
                pipelineModuleDefinition().getOutputsClass(), pipelineTask(),
                taskDirManager().taskDir());
        }
        return pipelineOutputs;
    }

    DatastoreProducerConsumerOperations datastoreProducerConsumerOperations() {
        return datastoreProducerConsumerOperations;
    }

    // This simplifies the mocking needed for this class.
    PipelineTask pipelineTask() {
        return pipelineTask;
    }

    /** For testing only. */
    boolean isProcessingSuccessful() {
        return processingSuccessful;
    }

    DatastoreFileManager datastoreFileManager() {
        return new DatastoreFileManager(pipelineTask(), getTaskDir().toPath());
    }

    TaskDirectoryManager taskDirManager() {
        return new TaskDirectoryManager(pipelineTask());
    }

    /** For testing only. */
    boolean getDoneLooping() {
        return doneLooping;
    }

    /** For testing only. */
    void setDoneLooping(boolean doneLooping) {
        this.doneLooping = doneLooping;
    }
}
