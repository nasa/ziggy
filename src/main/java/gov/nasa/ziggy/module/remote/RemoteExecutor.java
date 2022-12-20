package gov.nasa.ziggy.module.remote;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmExecutor;
import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

public abstract class RemoteExecutor extends AlgorithmExecutor {

    private static final Logger log = LoggerFactory.getLogger(RemoteExecutor.class);

    protected RemoteExecutor(PipelineTask pipelineTask) {
        super(pipelineTask);
    }

    /**
     * A default method to submit tasks to PBS that can be used by all of the currently-supported
     * remote clusters.
     *
     * @throws IOException
     * @throws ConfigurationException
     */
    protected void submitToPbsInternal(StateFile initialState, PipelineTask pipelineTask,
        Path algorithmLogDir, Path taskDir) throws IOException, ConfigurationException {

        getStateFile().setPbsSubmitTimeMillis(System.currentTimeMillis());
        getStateFile().persist();

        JobSubmissionPaths jobSubmissionPaths = new JobSubmissionPaths();

        log.info("Launching jobs for state file: " + getStateFile());

        int coresPerNode = getStateFile().getActiveCoresPerNode();

        int numNodes = getStateFile().getRequestedNodeCount();
        log.info("Number of nodes requested for jobs: " + numNodes);

        log.info("Launching job, name=" + pipelineTask.taskBaseName() + ", coresPerNode="
            + coresPerNode + ", numNodes=" + numNodes);

        File pbsLogDirFile = DirectoryProperties.pbsLogDir().toFile();
        pbsLogDirFile.mkdirs();
        String pbsLogDir = pbsLogDirFile.getCanonicalPath();

        String[] scriptArgs = { jobSubmissionPaths.getWorkingDirPath(),
            jobSubmissionPaths.getHomeDirPath(), jobSubmissionPaths.getStateFilePath(),
            jobSubmissionPaths.getPipelineConfigPath() };

        Qsub.Builder b = new Qsub.Builder().queueName(getStateFile().getQueueName())
            .wallTime(getStateFile().getRequestedWallTime())
            .numNodes(numNodes)
            .coresPerNode(getStateFile().getMinCoresPerNode())
            .gigsPerNode(getStateFile().getMinGigsPerNode())
            .model(getStateFile().getRemoteNodeArchitecture())
            .groupName(getStateFile().getRemoteGroup())
            .scriptPath(
                new File(jobSubmissionPaths.getBinPath(), EXECUTABLE_NAME).getCanonicalPath())
            .runjavaProgram(NODE_MASTER_NAME)
            .pbsLogDir(pbsLogDir)
            .nasLogDir(algorithmLogDir.toString())
            .pipelineTask(pipelineTask)
            .scriptArgs(scriptArgs);
        b.cluster(RemoteQueueDescriptor.fromName(getStateFile().getQueueName()).getRemoteCluster());
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
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            PipelineTask task = pipelineTaskCrud.retrieve(pipelineTask.getId());
            task.incrementTaskLogIndex();
            pipelineTaskCrud.update(task);
            return null;
        });

        // Update the remote jobs in the database
        new PipelineTaskOperations().createRemoteJobsFromQstat(pipelineTask.getId());

        // update processing state
        log.info("Updating processing state -> " + ProcessingState.ALGORITHM_QUEUED);

        ProcessingSummaryOperations attrOps = new ProcessingSummaryOperations();

        attrOps.updateProcessingState(pipelineTask.getId(), ProcessingState.ALGORITHM_QUEUED);
        attrOps.updateSubTaskCounts(pipelineTask.getId(), initialState.getNumTotal(),
            initialState.getNumComplete(), initialState.getNumFailed());

    }

    @Override
    protected void addToMonitor(StateFile stateFile) {
        AlgorithmMonitor.startRemoteMonitoring(stateFile);
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
    protected void setParameterSetCrud(ParameterSetCrud parameterSetCrud) {
        super.setParameterSetCrud(parameterSetCrud);
    }

    @Override
    protected void setProcessingSummaryOperations(ProcessingSummaryOperations crud) {
        super.setProcessingSummaryOperations(crud);
    }

    @Override
    public AlgorithmType algorithmType() {
        return AlgorithmType.REMOTE;
    }

}
