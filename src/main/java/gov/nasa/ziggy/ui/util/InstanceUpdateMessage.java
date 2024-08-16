package gov.nasa.ziggy.ui.util;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.services.messages.PipelineMessage;

public class InstanceUpdateMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240328L;

    private State instanceState;
    private boolean instancesRemaining;

    public InstanceUpdateMessage(State instanceState, boolean instancesRemaining) {
        this.instanceState = instanceState;
        this.instancesRemaining = instancesRemaining;
    }

    public State getInstanceState() {
        return instanceState;
    }

    public boolean isInstancesRemaining() {
        return instancesRemaining;
    }
}
