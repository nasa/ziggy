/*
 *
 */
package gov.nasa.ziggy.ui.common;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * TODO: This class....
 *
 * @author Todd Klaus
 */
public interface AsyncListener {
    /**
     * @param reply
     */
    void receive(PipelineMessage reply);

    /**
     *
     *
     */
    void timeoutReached();
}
