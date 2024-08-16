package gov.nasa.ziggy.services.messages;

/**
 * Message sent by the when parameter sets have been modified. This warns subscribers that they may
 * be holding out-of-date information and should decide what if anything they want to do about that.
 *
 * @author PT
 */
public class ParametersChangedMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240530L;
}
