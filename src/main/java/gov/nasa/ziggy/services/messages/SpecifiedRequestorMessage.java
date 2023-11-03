package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.util.Requestor;

/**
 * A {@link PipelineMessage} with a uniquely specified requestor.
 * <p>
 * Instances of {@link SpecifiedRequestorMessage} are messages that can determine whether a given
 * subscriber is the requestor / intended recipient of the message. In general, the life cycle of
 * {@link SpecifiedRequestorMessage}s are as follows:
 * <ol>
 * <li>An instance of {@link Requestor} publishes an instance of {@link SpecifiedRequestorMessage}.
 * The publisher's {@link Requestor#requestorIdentifier()} method is used by the message to capture
 * the publisher's unique identification.
 * <li>The subscriber that receives the original message constructs a reply for the requestor. This
 * message is also an instance of {@link SpecifiedRequestorMessage}, and it captures the identifier
 * from the original message to use as its own requestor identifier.
 * <li>When the reply message reaches a subscriber, that subscriber can use
 * {@link Requestor#isDestination(Requestor)} to determine whether the subscriber in question is the
 * intended recipient.
 * </ol>
 * <p>
 * Note that, while a {@link SpecifiedRequestorMessage} has an intended recipient, there is no
 * engineering control that prevents other subscribers from subscribing to the message and making
 * use of its content. The intent of the {@link SpecifiedRequestorMessage} is to allow subscribers
 * to protect themselves: specifically, to allow them to avoid taking action in response to a
 * message not intended for them, in the event that taking such an action in that case would cause
 * an error.
 *
 * @author PT
 */
public class SpecifiedRequestorMessage extends PipelineMessage implements Requestor {

    private static final long serialVersionUID = 20230622L;
    private final Object requestorIdentifier;

    public SpecifiedRequestorMessage(Requestor requestor) {
        requestorIdentifier = requestor.requestorIdentifier();
    }

    public SpecifiedRequestorMessage(SpecifiedRequestorMessage message) {
        requestorIdentifier = message.requestorIdentifier;
    }

    @Override
    public Object requestorIdentifier() {
        return requestorIdentifier;
    }

    @Override
    public String toString() {
        return super.toString() + ", requestorIdentifier=" + requestorIdentifier;
    }
}
