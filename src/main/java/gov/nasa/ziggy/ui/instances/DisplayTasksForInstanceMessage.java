package gov.nasa.ziggy.ui.instances;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Message sent from the {@link InstancesPanel} to the {@link TasksPanel} notifying the latter that
 * it may need to update its displays because of a change of the selected instance.
 *
 * @author PT
 */
public class DisplayTasksForInstanceMessage extends PipelineMessage {

    private static final long serialVersionUID = 20230712L;

    private final boolean reselect;
    private final Long pipelineInstanceId;

    public DisplayTasksForInstanceMessage(boolean reselect, Long pipelineInstanceId) {
        this.reselect = reselect;
        this.pipelineInstanceId = pipelineInstanceId;
    }

    public boolean isReselect() {
        return reselect;
    }

    public long getPipelineInstanceId() {
        return pipelineInstanceId;
    }
}
