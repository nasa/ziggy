package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.ui.ops.instances.OpsInstancesPanel;

/**
 * Message sent from the WorkerFireTriggerRequestListener to the console to indicate that all
 * pipeline running is done until such time as a fresh trigger is fired. The message has no content,
 * its arrival is all the information the receiver needs.
 *
 * @author PT
 */
public class NoRunningOrQueuedPipelinesMessage extends PipelineMessage {

    /**
     *
     */
    private static final long serialVersionUID = 20210318L;

    /**
     * Handles the receipt of this message by updating the {@link OpsInstancesPanel} to reflect that
     * execution of all pipelines has completed.
     */
    @Override
    public Object handleMessage(MessageHandler handler) {
        handler.handleNoRunningOrQueuedPipelinesMessage();
        return null;
    }

}
