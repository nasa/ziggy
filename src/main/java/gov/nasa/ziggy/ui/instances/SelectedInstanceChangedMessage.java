package gov.nasa.ziggy.ui.instances;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Message sent when selected instance changes. The currently selected instance is obtained with
 * {@link #getPipelineInstanceId()}.
 *
 * @author PT
 * @author Bill Wohler
 */
public class SelectedInstanceChangedMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240913L;

    private final long pipelineInstanceId;

    public SelectedInstanceChangedMessage(long pipelineInstanceId) {
        this.pipelineInstanceId = pipelineInstanceId;
    }

    public long getPipelineInstanceId() {
        return pipelineInstanceId;
    }
}
