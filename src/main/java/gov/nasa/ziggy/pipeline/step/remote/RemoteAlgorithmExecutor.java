package gov.nasa.ziggy.pipeline.step.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.step.AlgorithmExecutor;
import gov.nasa.ziggy.pipeline.step.AlgorithmMonitor;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.messages.MonitorAlgorithmRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.PipelineException;

/**
 * Subclass of {@link AlgorithmExecutor} that performs execution and submission activities for tasks
 * that run on a remote system (HPC or cloud environment).
 *
 * @author PT
 */
public class RemoteAlgorithmExecutor extends AlgorithmExecutor {

    private static final Logger log = LoggerFactory.getLogger(RemoteAlgorithmExecutor.class);

    private PipelineNodeExecutionResources executionResources;
    private BatchManager<?> batchManager;
    private BatchParameters batchParameters;
    private int subtaskCount = -1;
    private List<RemoteJobInformation> remoteJobsInformation;

    public RemoteAlgorithmExecutor(PipelineTask pipelineTask) {
        super(pipelineTask);
    }

    /** Sends the {@link MonitorAlgorithmRequest} for all successful remote jobs. */
    @Override
    protected void addToMonitor() {
        ZiggyMessenger.publish(monitorAlgorithmRequest());
    }

    protected MonitorAlgorithmRequest monitorAlgorithmRequest() {
        return new MonitorAlgorithmRequest(pipelineTask, workingDir(), remoteJobsInformation);
    }

    /**
     * Submits the task for remote execution.
     * <p>
     * Submission of remote jobs is performed by the
     * {@link BatchManager#submitJobs(PipelineTask, int)} method, which returns a collection of
     * {@link RemoteJobInformation} instances, one per job. The remote job information instances are
     * used to populate the {@link RemoteJob}s in the database, and are sent to the
     * {@link AlgorithmMonitor}.
     */
    @Override
    protected void submitForExecution() {
        batchParameters().computeParameterValues(executionResources(), subtaskCount());
        log.info("Launching jobs for task {}", pipelineTask.taskBaseName());
        int numNodes = batchParameters().nodeCount();
        log.info("Requested {} nodes for jobs", numNodes);
        log.info("Launching job, name={}, coresPerNode={}, numNodes={}",
            pipelineTask.taskBaseName(), batchParameters().activeCores(), numNodes);

        List<RemoteJobInformation> allRemoteJobsInformation = batchManager()
            .submitJobs(pipelineTask, subtaskCount());
        remoteJobsInformation = allRemoteJobsInformation.stream()
            .filter(s -> s.getBatchSubmissionExitCode() == 0)
            .collect(Collectors.toList());

        // Throw an exception if no jobs at all got into the queue.
        if (remoteJobsInformation.size() == 0) {
            alertService().generateAndBroadcastAlert("PI (Remote)", pipelineTask, Severity.ERROR,
                "No remote jobs submitted");
            throw new PipelineException("No remote jobs created for task " + pipelineTask);
        }

        // If some jobs went into the queue but not others, that's just a warning.
        if (remoteJobsInformation.size() < allRemoteJobsInformation.size()) {
            log.warn("{} jobs submitted but only {} successfully queued",
                allRemoteJobsInformation.size(), remoteJobsInformation.size());
            alertService().generateAndBroadcastAlert("PI (Remote)", pipelineTask, Severity.WARNING,
                "Attempted to submit " + allRemoteJobsInformation.size()
                    + " jobs but only successfully submitted " + remoteJobsInformation.size());
        }

        // Assign the job IDs to the remote job information objects.
        Map<String, Long> jobIdByName = batchManager().jobIdByName(pipelineTask);
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            remoteJobInformation.setJobId(jobIdByName.get(remoteJobInformation.getJobName()));
        }

        // Update the remote jobs in the database.
        pipelineTaskDataOperations().addRemoteJobs(pipelineTask, remoteJobsInformation);

        addToMonitor();

        log.info("Updating processing step -> {}", ProcessingStep.QUEUED);

        pipelineTaskDataOperations().updateSubtaskCounts(pipelineTask, -1, 0, 0);
        pipelineTaskDataOperations().updateProcessingStep(pipelineTask, ProcessingStep.QUEUED);
    }

    /**
     * Resumes monitoring of the task in the {@link AlgorithmMonitor}.
     * <p>
     * The {@link RemoteJob} instances for the given {@link PipelineTask} are retrieved and new
     * {@link RemoteJobInformation} instances are created for any remote jobs that are incomplete.
     * If any RemoteJobInformation instances are created, the task is sent to the AlgorithmMonitor.
     * If no RemoteJobInformation instances are created, it indicates that there are no incomplete
     * remote jobs for the task, hence no monitoring resumption is attempted.
     *
     * @return true if there were jobs to resume, false otherwise
     */
    public boolean resumeMonitoring() {
        remoteJobsInformation = new ArrayList<>();
        Set<RemoteJob> remoteJobs = pipelineTaskDataOperations().remoteJobs(pipelineTask);
        for (RemoteJob remoteJob : remoteJobs) {
            if (remoteJob.isFinished()) {
                continue;
            }
            RemoteJobInformation remoteJobInformation = batchManager()
                .remoteJobInformation(remoteJob);
            if (remoteJobInformation == null) {
                continue;
            }
            remoteJobsInformation.add(remoteJobInformation);
        }
        if (CollectionUtils.isEmpty(remoteJobsInformation)) {
            log.info("No running remote jobs detected, monitoring will not resume");
            return false;
        }
        log.info("{} running remote jobs detected, monitoring will resume",
            remoteJobsInformation.size());
        addToMonitor();
        return true;
    }

    @Override
    protected String activeCores() {
        return Integer.toString(batchParameters().activeCores());
    }

    @Override
    protected String wallTime() {
        return Integer.toString((int) (batchParameters().requestedWallTimeHours() * 3600));
    }

    protected BatchParameters batchParameters() {
        if (batchParameters == null) {
            batchParameters = executionResources().getRemoteEnvironment()
                .getBatchSystem()
                .batchParameters();
            batchParameters.computeParameterValues(executionResources(), subtaskCount());
        }
        return batchParameters;
    }

    protected BatchManager<?> batchManager() {
        if (batchManager == null) {
            batchManager = executionResources().getRemoteEnvironment()
                .getBatchSystem()
                .batchManager();
        }
        return batchManager;
    }

    protected int subtaskCount() {
        if (subtaskCount < 0) {
            SubtaskCounts subtaskCounts = pipelineTaskDataOperations().subtaskCounts(pipelineTask);
            subtaskCount = subtaskCounts.getTotalSubtaskCount()
                - subtaskCounts.getCompletedSubtaskCount();
        }
        return subtaskCount;
    }

    protected PipelineNodeExecutionResources executionResources() {
        if (executionResources == null) {
            executionResources = pipelineTaskOperations().executionResources(pipelineTask);
        }
        return executionResources;
    }

    protected AlertService alertService() {
        return AlertService.getInstance();
    }

    public List<RemoteJobInformation> getRemoteJobsInformation() {
        return remoteJobsInformation;
    }
}
