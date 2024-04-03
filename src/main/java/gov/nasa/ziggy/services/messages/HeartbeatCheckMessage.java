package gov.nasa.ziggy.services.messages;

public class HeartbeatCheckMessage extends PipelineMessage {

    private static final long serialVersionUID = 20231126L;

    private final long heartbeatTime;

    public HeartbeatCheckMessage(long heartbeatTime) {
        this.heartbeatTime = heartbeatTime;
    }

    public long getHeartbeatTime() {
        return heartbeatTime;
    }
}
