package gov.nasa.ziggy.module.remote;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmExecutor;
import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.messages.MonitorAlgorithmRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

public abstract class RemoteExecutor extends AlgorithmExecutor {

    private static final Logger log = LoggerFactory.getLogger(RemoteExecutor.class);

    protected RemoteExecutor(PipelineTask pipelineTask) {
        super(pipelineTask);
    }

    /**
     * A default method to submit tasks to PBS that can be used by all of the currently-supported
     * remote clusters.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    protected void submitToPbsInternal(StateFile initialState, PipelineTask pipelineTask,
        Path algorithmLogDir, Path taskDir) {

        try {
            getStateFile().setPbsSubmitTimeMillis(System.currentTimeMillis());
            getStateFile().persist();
            addToMonitor(getStateFile());

            log.info("Launching jobs for state file: " + getStateFile());

            int coresPerNode = getStateFile().getActiveCoresPerNode();

            int numNodes = getStateFile().getRequestedNodeCount();
            log.info("Number of nodes requested for jobs: " + numNodes);

            log.info("Launching job, name=" + pipelineTask.taskBaseName() + ", coresPerNode="
                + coresPerNode + ", numNodes=" + numNodes);

            File pbsLogDirFile = DirectoryProperties.pbsLogDir().toFile();
            pbsLogDirFile.mkdirs();
            String pbsLogDir = pbsLogDirFile.getCanonicalPath();

            Qsub.Builder b = new Qsub.Builder().queueName(getStateFile().getQueueName())
                .wallTime(getStateFile().getRequestedWallTime())
                .numNodes(numNodes)
                .coresPerNode(getStateFile().getMinCoresPerNode())
                .gigsPerNode(getStateFile().getMinGigsPerNode())
                .model(getStateFile().getRemoteNodeArchitecture())
                .groupName(getStateFile().getRemoteGroup())
                .scriptPath(new File(DirectoryProperties.ziggyBinDir().toFile(), ZIGGY_PROGRAM)
                    .getCanonicalPath())
                .ziggyProgram(NODE_MASTER_NAME)
                .pbsLogDir(pbsLogDir)
                .pipelineTask(pipelineTask)
                .taskDir(workingDir().toString());
            b.cluster(RemoteQueueDescriptor.fromQueueName(getStateFile().getQueueName())
                .getRemoteCluster());
            Qsub qsub = b.build();

            log.info("Submitting multiple jobs with 1 node per job for this task");
            int[] returnCodes = qsub.submitMultipleJobsForTask();
            int goodJobsCount = 0;
            for (int returnCode : returnCodes) {
                if (returnCode == 0) {
                    goodJobsCount++;
                }
            }
            if (goodJobsCount < returnCodes.length) {
                log.warn(returnCodes.length + " jobs submitted but only " + goodJobsCount
                    + " successfully queued");
                AlertService.getInstance()
                    .generateAndBroadcastAlert("PI (Remote)", getStateFile().getPipelineTaskId(),
                        AlertService.Severity.WARNING, "Attempted to submit " + returnCodes.length
                            + " jobs but only successfully submitted " + goodJobsCount);
            }

            // Update the task log index.
            pipelineTaskOperations().setRemoteExecution(pipelineTask.getId());

            // Update the remote jobs in the database
            pipelineTaskOperations().createRemoteJobsFromQstat(pipelineTask.getId());

            log.info("Updating processing step -> " + ProcessingStep.QUEUED);

            pipelineTaskOperations().updateProcessingStep(pipelineTask.getId(),
                ProcessingStep.QUEUED);
            pipelineTaskOperations().updateSubtaskCounts(pipelineTask.getId(),
                initialState.getNumTotal(), initialState.getNumComplete(),
                initialState.getNumFailed());
        } catch (IOException e) {
            throw new UncheckedIOException("Submit to PBS failed due to IOException", e);
        }
    }

    @Override
    protected void addToMonitor(StateFile stateFile) {
        ZiggyMessenger.publish(new MonitorAlgorithmRequest(stateFile, algorithmType()));
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
}
