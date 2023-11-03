package gov.nasa.ziggy.services.messages;

/**
 * Requests that the memdrone shell script be started by the supervisor. This request is needed in
 * one use-case only: the case in which the console also requests that the supervisor restart failed
 * tasks. In all other cases, the supervisor or the remote nodes do not need this message to tell
 * them to start the shell script.
 *
 * @author PT
 */
public class StartMemdroneRequest extends PipelineMessage {
    private static final long serialVersionUID = 20230511L;

    private long instanceId;
    private String moduleName;

    public StartMemdroneRequest(String moduleName, long instanceId) {
        this.instanceId = instanceId;
        this.moduleName = moduleName;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(long instanceId) {
        this.instanceId = instanceId;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }
}
