package gov.nasa.ziggy.services.messaging;

import java.util.HashSet;
import java.util.Set;

import gov.nasa.ziggy.services.messages.PipelineMessage;

public class MessageFromServer extends PipelineMessage {

    private static final long serialVersionUID = 20210223L;

    private Set<MessageFromClient> messagesFromClient;
    private String payload;

    public MessageFromServer(Set<MessageFromClient> messagesFromClient) {
        this.messagesFromClient = messagesFromClient;
        payload = new String();
    }

    public MessageFromServer(String payload) {
        this.payload = payload;
        messagesFromClient = new HashSet<>();
    }

    public Set<MessageFromClient> messagesFromClient() {
        return messagesFromClient;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        return null;
    }

}
