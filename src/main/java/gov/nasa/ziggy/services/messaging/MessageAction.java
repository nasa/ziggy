package gov.nasa.ziggy.services.messaging;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Defines an action that is to be taken when a message is delivered to a subscriber.
 *
 * @author PT
 */
@FunctionalInterface
public interface MessageAction<T extends PipelineMessage> {

    /**
     * Performs the action that is to be taken in response to a message.
     */
    void action(T message);
}
