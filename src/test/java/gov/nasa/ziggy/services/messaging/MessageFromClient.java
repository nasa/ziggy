package gov.nasa.ziggy.services.messaging;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Super-basic message from the RMI client to the server.
 *
 * @author PT
 */
public class MessageFromClient extends PipelineMessage {

    private static final long serialVersionUID = 20210223L;

    private String payload;

    public MessageFromClient(String payload) {
        this.payload = payload;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        return null;
    }

}
