package gov.nasa.ziggy.module.remote;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmExecutor;
import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.AlgorithmType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.messages.MonitorAlgorithmRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.TimeFormatter;

public abstract class RemoteExecutor extends AlgorithmExecutor {

    private static final Logger log = LoggerFactory.getLogger(RemoteExecutor.class);

    private List<RemoteJobInformation> remoteJobsInformation;
    private QstatParser qstatParser = new QstatParser();

    protected RemoteExecutor(PipelineTask pipelineTask) {
        super(pipelineTask);
    }

    /**
     * A default method to submit tasks to PBS that can be used by all of the currently-supported
     * remote clusters.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    protected void submitToPbsInternal(PipelineTask pipelineTask) {

        try {
            log.info("Launching jobs for task {}", pipelineTask.taskBaseName());

            int numNodes = getPbsParameters().getRequestedNodeCount();
            log.info("Requested {} nodes for jobs", numNodes);

            log.info("Launching job, name={}, coresPerNode={}, numNodes={}",
                pipelineTask.taskBaseName(), getPbsParameters().getActiveCoresPerNode(), numNodes);

            File pbsLogDirFile = DirectoryProperties.pbsLogDir().toFile();
            pbsLogDirFile.mkdirs();
            String pbsLogDir = pbsLogDirFile.getCanonicalPath();

            Qsub.Builder b = new Qsub.Builder().queueName(getPbsParameters().getQueueName())
                .wallTime(getPbsParameters().getRequestedWallTime())
                .numNodes(numNodes)
                .coresPerNode(getPbsParameters().getMinCoresPerNode())
                .gigsPerNode(getPbsParameters().getMinGigsPerNode())
                .model(getPbsParameters().getArchitecture().getNodeName())
                .groupName(getPbsParameters().getRemoteGroup())
                .scriptPath(new File(DirectoryProperties.ziggyBinDir().toFile(), ZIGGY_PROGRAM)
                    .getCanonicalPath())
                .ziggyProgram(NODE_MASTER_NAME)
                .pbsLogDir(pbsLogDir)
                .pipelineTask(pipelineTask)
                .taskDir(workingDir().toString());
            b.cluster(RemoteQueueDescriptor.fromQueueName(getPbsParameters().getQueueName())
                .getRemoteCluster());
            Qsub qsub = b.build();

            log.info("Submitting multiple jobs with 1 node per job for this task");
            Map<RemoteJobInformation, Integer> jobStatusByJobInformation = qsub.submitJobsForTask();
            int attemptedJobSubmissions = jobStatusByJobInformation.size();
            List<RemoteJobInformation> successfullySubmittedJobsInformation = jobStatusByJobInformation
                .entrySet()
                .stream()
                .filter(s -> s.getValue().intValue() == 0)
                .map(Entry::getKey)
                .collect(Collectors.toList());
            if (successfullySubmittedJobsInformation.size() < jobStatusByJobInformation.size()) {
                log.warn("{} jobs submitted but only {} successfully queued",
                    attemptedJobSubmissions, successfullySubmittedJobsInformation.size());
                AlertService.getInstance()
                    .generateAndBroadcastAlert("PI (Remote)", pipelineTask, Severity.WARNING,
                        "Attempted to submit " + attemptedJobSubmissions
                            + " jobs but only successfully submitted "
                            + successfullySubmittedJobsInformation.size());
            }
            if (successfullySubmittedJobsInformation.size() == 0) {
                AlertService.getInstance()
                    .generateAndBroadcastAlert("PI (Remote)", pipelineTask, Severity.ERROR,
                        "No remote jobs submitted");

                throw new PipelineException("No remote jobs created for task " + pipelineTask);
            }
            remoteJobsInformation = successfullySubmittedJobsInformation;

            // Add the job IDs to the RemoteJobInformation instances.
            qstatParser().populateJobIds(pipelineTask, successfullySubmittedJobsInformation);

            // Update the remote jobs in the database.
            pipelineTaskDataOperations().addRemoteJobs(pipelineTask,
                successfullySubmittedJobsInformation);

            addToMonitor();

            // Update the task log index.
            pipelineTaskDataOperations().updateAlgorithmType(pipelineTask, AlgorithmType.REMOTE);

            log.info("Updating processing step -> {}", ProcessingStep.QUEUED);

            pipelineTaskDataOperations().updateSubtaskCounts(pipelineTask, -1, 0, 0);
            pipelineTaskDataOperations().updateProcessingStep(pipelineTask, ProcessingStep.QUEUED);
        } catch (IOException e) {
            throw new UncheckedIOException("Submit to PBS failed due to IOException", e);
        }
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
            RemoteJobInformation remoteJobInformation = qstatParser()
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
    protected void addToMonitor() {
        ZiggyMessenger.publish(
            new MonitorAlgorithmRequest(pipelineTask, workingDir(), remoteJobsInformation));
    }

    @Override
    public String activeCores() {
        return Integer.toString(getPbsParameters().getActiveCoresPerNode());
    }

    @Override
    public String wallTime() {
        return Integer.toString((int) TimeFormatter
            .timeStringHhMmSsToTimeInSeconds(getPbsParameters().getRequestedWallTime()));
    }

    @Override
    protected Path algorithmLogDir() {
        return DirectoryProperties.algorithmLogsDir();
    }

    @Override
    protected Path taskDataDir() {
        return DirectoryProperties.taskDataDir();
    }

    @Override
    public AlgorithmType algorithmType() {
        return AlgorithmType.REMOTE;
    }

    QstatParser qstatParser() {
        return qstatParser;
    }

    List<RemoteJobInformation> getRemoteJobsInformation() {
        return remoteJobsInformation;
    }
}
