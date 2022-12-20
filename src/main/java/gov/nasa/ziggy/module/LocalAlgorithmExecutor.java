package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.exec.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Manages a local process in synchronous mode.
 *
 * @author PT
 */
public class LocalAlgorithmExecutor extends AlgorithmExecutor {

    private static final Logger log = LoggerFactory.getLogger(LocalAlgorithmExecutor.class);

    private static final int JOB_INDEX_FOR_LOCAL_EXECUTION = 0;

    public LocalAlgorithmExecutor(PipelineTask pipelineTask) {
        super(pipelineTask);
    }

    @Override
    public PbsParameters generatePbsParameters(RemoteParameters remoteParameters,
        int totalSubtaskCount) {
        return null;
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
    protected void addToMonitor(StateFile stateFile) {
        AlgorithmMonitor.startLocalMonitoring(stateFile);
    }

    @Override
    protected void submitForExecution(StateFile stateFile) throws Exception {

        stateFile.setPbsSubmitTimeMillis(System.currentTimeMillis());
        stateFile.persist();

        JobSubmissionPaths jobSubmissionPaths = new JobSubmissionPaths();

        CommandLine cmdLine = algorithmCommandLine(jobSubmissionPaths);

        // Increment the task log index.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            PipelineTask task = pipelineTaskCrud.retrieve(pipelineTask.getId());
            task.incrementTaskLogIndex();
            pipelineTaskCrud.update(task);
            return null;
        });

        // Start the external process -- note that it will cause execution to block until
        // the algorithm has completed or failed.
        // NB: it may seem strange to use an external process to execute "runjava" and
        // then to use runjava to run the ComputeNodeMaster, given that the ComputeNodeMaster
        // can simply be run from this method directly! This is done to preserve as much as
        // possible the symmetry between local and remote execution.
        ExternalProcess externalProcess = ExternalProcess.allLoggingExternalProcess(cmdLine);
        int exitCode = externalProcess.execute();

        // If the exit code was nonzero, throw an exception here that will mark the pipeline
        // as having failed
        if (exitCode != 0) {
            if (!ZiggyShutdownHook.shutdownInProgress()) {
                throw new PipelineException(
                    "Local processing of task " + pipelineTask.getId() + " failed");
            }
            log.error(
                "Task " + pipelineTask.getId() + " processing incomplete due to worker shutdown");
        }
    }

    /**
     * Generates the "runjava compute-note-master" command line, plus the arguments to same that are
     * constant across local and all remote execution modes (specifically: the task directory, the
     * pipeline home directory, the state file, and the pipeline configuration file).
     */
    protected CommandLine algorithmCommandLine(JobSubmissionPaths jobSubmissionPaths)
        throws IOException {
        CommandLine cmdLine = new CommandLine(
            new File(jobSubmissionPaths.getBinPath(), EXECUTABLE_NAME).getCanonicalPath());
        cmdLine.addArgument(NODE_MASTER_NAME);
        cmdLine.addArgument(jobSubmissionPaths.getWorkingDirPath());
        cmdLine.addArgument(jobSubmissionPaths.getHomeDirPath());
        cmdLine.addArgument(jobSubmissionPaths.getStateFilePath());
        cmdLine.addArgument(jobSubmissionPaths.getPipelineConfigPath());
        cmdLine.addArgument(DirectoryProperties.algorithmLogsDir()
            .resolve(pipelineTask.logFilename(JOB_INDEX_FOR_LOCAL_EXECUTION))
            .toString());
        return cmdLine;
    }

    @Override
    public AlgorithmType algorithmType() {
        return AlgorithmType.LOCAL;
    }

}
