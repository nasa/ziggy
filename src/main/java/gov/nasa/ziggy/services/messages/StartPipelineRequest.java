package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;

/**
 * A request to start a given pipeline. The request can include a start and end node, and can
 * include repetition parameters (i.e., the trigger can automatically re-fire after some period of
 * time).
 *
 * @author PT
 */
public class StartPipelineRequest extends PipelineMessage {

    private static final long serialVersionUID = 20230511L;

    private final String pipelineName;
    private final String instanceName;
    private final String startNodeName;
    private final String endNodeName;
    private final int maxRepeats;
    private final int repeatIntervalSeconds;

    public StartPipelineRequest(String pipelineName, String instanceName, String startNodeName,
        String endNodeName, int maxRepeats, int repeatIntervalSeconds) {
        this.pipelineName = pipelineName;
        this.instanceName = instanceName;
        this.startNodeName = startNodeName;
        this.endNodeName = endNodeName;
        this.maxRepeats = maxRepeats;
        this.repeatIntervalSeconds = repeatIntervalSeconds;
    }

    public StartPipelineRequest(String pipelineName, String instanceName,
        PipelineDefinitionNode startNode, PipelineDefinitionNode endNode, int maxRepeats,
        int repeatIntervalSeconds) {
        this.pipelineName = pipelineName;
        this.instanceName = instanceName;
        startNodeName = startNode != null ? startNode.getModuleName() : null;
        endNodeName = endNode != null ? endNode.getModuleName() : null;
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
}
