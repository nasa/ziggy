package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.apache.commons.exec.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.messages.MonitorAlgorithmRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
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
    public PbsParameters generatePbsParameters(
        PipelineDefinitionNodeExecutionResources executionResources, int totalSubtaskCount) {
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
        ZiggyMessenger.publish(new MonitorAlgorithmRequest(stateFile, algorithmType()));
    }

    @Override
    protected void submitForExecution(StateFile stateFile) {

        stateFile.setPbsSubmitTimeMillis(System.currentTimeMillis());
        stateFile.persist();
        addToMonitor(stateFile);

        CommandLine cmdLine = algorithmCommandLine();

        // Increment the task log index.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
            PipelineTask task = pipelineTaskCrud.retrieve(pipelineTask.getId());
            task.incrementTaskLogIndex();
            task.setRemoteExecution(false);
            pipelineTaskCrud.merge(task);
            return null;
        });

        // Start the external process -- note that it will cause execution to block until
        // the algorithm has completed or failed.
        // NB: it may seem strange to use an external process to execute the "ziggy" program and
        // then to use the ziggy program to run the ComputeNodeMaster, given that the
        // ComputeNodeMaster can simply be run from this method directly! This is done to preserve
        // as much as possible the symmetry between local and remote execution.
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
     * Generates the "ziggy compute-note-master" command line, plus the arguments to same that are
     * constant across local and all remote execution modes (specifically: the task directory, the
     * pipeline home directory, the state file, and the pipeline configuration file).
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    protected CommandLine algorithmCommandLine() {
        CommandLine cmdLine;
        try {
            cmdLine = new CommandLine(
                new File(DirectoryProperties.ziggyBinDir().toFile(), ZIGGY_PROGRAM)
                    .getCanonicalPath());
            cmdLine.addArgument(NODE_MASTER_NAME);
            cmdLine.addArgument(workingDir().toString());
            cmdLine.addArgument(DirectoryProperties.algorithmLogsDir()
                .resolve(pipelineTask.logFilename(JOB_INDEX_FOR_LOCAL_EXECUTION))
                .toString());
            return cmdLine;
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Error getting path to file " + DirectoryProperties.ziggyBinDir().toString(), e);
        }
    }

    @Override
    public AlgorithmType algorithmType() {
        return AlgorithmType.LOCAL;
    }
}
