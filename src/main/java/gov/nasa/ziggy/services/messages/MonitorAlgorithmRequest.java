package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.module.AlgorithmExecutor.AlgorithmType;
import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.worker.PipelineWorker;

/**
 * Message sent from the {@link PipelineWorker} to the {@link PipelineSupervisor} to request that a
 * given {@link StateFile} be added to either the local or remote {@link AlgorithmMonitor}.
 *
 * @author PT
 */
public class MonitorAlgorithmRequest extends PipelineMessage {

    private static final long serialVersionUID = 20230511L;

    private final AlgorithmType algorithmType;
    private final StateFile stateFile;

    public MonitorAlgorithmRequest(StateFile stateFile, AlgorithmType algorithmType) {
        this.algorithmType = algorithmType;
        this.stateFile = stateFile;
    }

    public AlgorithmType getAlgorithmType() {
        return algorithmType;
    }

    public StateFile getStateFile() {
        return stateFile;
    }
}
