package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.services.messaging.MessageHandler;

/**
 * Sends to the worker a request to fire a trigger for a given pipeline. The request can include a
 * start and end node, and can include repetition parameters (i.e., the trigger can automatically
 * re-fire after some period of time).
 *
 * @author PT
 */
public class WorkerFireTriggerRequest extends PipelineMessage {

    private static final long serialVersionUID = 20210318L;

    private final String pipelineName;
    private final String instanceName;
    private final String startNodeName;
    private final String endNodeName;
    private final int maxRepeats;
    private final int repeatIntervalSeconds;

    public WorkerFireTriggerRequest(String pipelineName, String instanceName, String startNodeName,
        String endNodeName, int maxRepeats, int repeatIntervalSeconds) {
        this.pipelineName = pipelineName;
        this.instanceName = instanceName;
        this.startNodeName = startNodeName;
        this.endNodeName = endNodeName;
        this.maxRepeats = maxRepeats;
        this.repeatIntervalSeconds = repeatIntervalSeconds;
    }

    public WorkerFireTriggerRequest(String pipelineName, String instanceName,
        PipelineDefinitionNode startNode, PipelineDefinitionNode endNode, int maxRepeats,
        int repeatIntervalSeconds) {
        this.pipelineName = pipelineName;
        this.instanceName = instanceName;
        startNodeName = startNode != null ? startNode.getModuleName().getName() : null;
        endNodeName = endNode != null ? endNode.getModuleName().getName() : null;
        this.maxRepeats = maxRepeats;
        this.repeatIntervalSeconds = repeatIntervalSeconds;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getStartNodeName() {
        return startNodeName;
    }

    public String getEndNodeName() {
        return endNodeName;
    }

    public int getMaxRepeats() {
        return maxRepeats;
    }

    public int getRepeatIntervalSeconds() {
        return repeatIntervalSeconds;
    }

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        messageHandler.handleTriggerRequest(this);
        return null;
    }
}
