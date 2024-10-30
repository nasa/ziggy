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
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.logging.TaskLog;
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

    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();

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
    protected void addToMonitor() {
        ZiggyMessenger.publish(new MonitorAlgorithmRequest(pipelineTask, workingDir()));
    }

    @Override
    protected void submitForExecution() {

        addToMonitor();

        CommandLine cmdLine = algorithmCommandLine();

        pipelineTaskDataOperations().updateAlgorithmType(pipelineTask, AlgorithmType.LOCAL);

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
                throw new PipelineException("Local processing of task " + pipelineTask + " failed");
            }
            log.error("Task {} processing incomplete due to worker shutdown", pipelineTask);
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
            cmdLine.addArgument(TaskLog.algorithmLogFileSystemProperty(pipelineTask));
            cmdLine.addArgument(TaskLog.algorithmNameSystemProperty(pipelineTask));
            cmdLine.addArgument(NODE_MASTER_NAME);
            cmdLine.addArgument(workingDir().toString());
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

    @Override
    protected String activeCores() {
        return "1";
    }

    @Override
    protected String wallTime() {
        return Integer.toString(pipelineModuleDefinitionOperations()
            .pipelineModuleExecutionResources(pipelineModuleDefinitionOperations()
                .pipelineModuleDefinition(pipelineTask.getModuleName()))
            .getExeTimeoutSeconds());
    }

    PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
        return pipelineModuleDefinitionOperations;
    }
}
