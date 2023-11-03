package gov.nasa.ziggy.services.messages;

/**
 * Message sent from the {@link WorkerFireTriggerRequestListener} to the console to indicate that
 * all pipeline running is done until such time as a fresh trigger is fired. The message has no
 * content, its arrival is all the information the receiver needs.
 *
 * @author PT
 */
public class NoRunningOrQueuedPipelinesMessage extends PipelineMessage {

    private static final long serialVersionUID = 20210318L;
}
