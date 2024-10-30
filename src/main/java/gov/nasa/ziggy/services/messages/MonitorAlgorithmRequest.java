package gov.nasa.ziggy.services.messages;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.AlgorithmType;
import gov.nasa.ziggy.module.remote.RemoteJobInformation;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.worker.PipelineWorker;

/**
 * Message sent from the {@link PipelineWorker} to the {@link PipelineSupervisor} to request that a
 * given task be added to either the local or remote {@link AlgorithmMonitor}.
 *
 * @author PT
 * @author Bill Wohler
 */
public class MonitorAlgorithmRequest extends PipelineMessage {

    private static final long serialVersionUID = 20240909L;

    private final PipelineTask pipelineTask;
    private final List<RemoteJobInformation> remoteJobsInformation;
    private final String taskDir;
    private final AlgorithmType algorithmType;

    public MonitorAlgorithmRequest(PipelineTask pipelineTask, Path taskDir) {
        this(pipelineTask, taskDir, AlgorithmType.LOCAL, null);
    }

    public MonitorAlgorithmRequest(PipelineTask pipelineTask, Path taskDir,
        List<RemoteJobInformation> remoteJobInformation) {
        this(pipelineTask, taskDir, AlgorithmType.REMOTE, remoteJobInformation);
    }

    private MonitorAlgorithmRequest(PipelineTask pipelineTask, Path taskDir,
        AlgorithmType algorithmType, List<RemoteJobInformation> remoteJobsInformation) {
        this.pipelineTask = pipelineTask;
        this.remoteJobsInformation = remoteJobsInformation;
        this.taskDir = taskDir.toString();
        this.algorithmType = algorithmType;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    public Path getTaskDir() {
        return Paths.get(taskDir);
    }

    public AlgorithmType getAlgorithmType() {
        return algorithmType;
    }

    public List<RemoteJobInformation> getRemoteJobsInformation() {
        return remoteJobsInformation;
    }
}
