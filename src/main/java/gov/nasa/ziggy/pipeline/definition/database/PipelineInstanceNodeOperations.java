package gov.nasa.ziggy.pipeline.definition.database;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.services.messages.PipelineInstanceFinishedMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.supervisor.TaskRequestHandler.PipelineInstanceNodeInformation;

/**
 * Operations class for methods primarily concerned with {@link PipelineInstanceNode}s.
 *
 * @author PT
 */
public class PipelineInstanceNodeOperations extends DatabaseOperations {

    private static final Logger log = LoggerFactory.getLogger(PipelineInstanceNodeOperations.class);

    private PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
    private PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
    private PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud = new PipelineDefinitionNodeCrud();
    private PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
    private ParametersOperations parametersOperations = new ParametersOperations();
    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();

    /** returns the next pipeline instance nodes for execution. */
    public List<PipelineInstanceNode> nextPipelineInstanceNodes(long pipelineInstanceNodeId) {
        return performTransaction(() -> {
            List<PipelineInstanceNode> nextInstanceNodes = pipelineInstanceNodeCrud()
                .retrieve(pipelineInstanceNodeId)
                .getNextNodes();
            if (nextInstanceNodes.isEmpty()) {
                return nextInstanceNodes;
            }
            for (PipelineInstanceNode nextInstanceNode : nextInstanceNodes) {
                if (nextInstanceNode != null) {
                    log.info("Launching node {} with a new UOW", nextInstanceNode.getModuleName());
                }
            }
            return nextInstanceNodes;
        });
    }

    public void addNextNodes(PipelineInstanceNode parentNode,
        Collection<PipelineInstanceNode> nextNodes) {
        performTransaction(() -> {
            PipelineInstanceNode databaseNode = pipelineInstanceNodeCrud()
                .retrieve(parentNode.getId());
            databaseNode.getNextNodes().addAll(nextNodes);
            pipelineInstanceNodeCrud().merge(databaseNode);
        });
    }

    public PipelineInstanceNode pipelineInstanceNode(long pipelineInstanceNodeId) {
        return performTransaction(
            () -> pipelineInstanceNodeCrud().retrieve(pipelineInstanceNodeId));
    }

    public List<PipelineInstanceNode> pipelineInstanceNodes(PipelineInstance pipelineInstance) {
        return performTransaction(() -> pipelineInstanceNodeCrud().retrieveAll(pipelineInstance));
    }

    /** Marks a given pipeline instance node as transition-complete. */
    public void markInstanceNodeTransitionComplete(long pipelineInstanceNodeId) {
        performTransaction(
            () -> pipelineInstanceNodeCrud().markTransitionComplete(pipelineInstanceNodeId));
    }

    /** Marks a given pipeline instance node as transition-incomplete. */
    public void markInstanceNodeTransitionIncomplete(long pipelineInstanceNodeId) {
        performTransaction(
            () -> pipelineInstanceNodeCrud().markTransitionIncomplete(pipelineInstanceNodeId));
    }

    /**
     * Mark the transition from this node to the next as failed. This also requires that the
     * pipeline instance state be set to TRANSITION_FAILED.
     */
    public void markInstanceNodeTransitionFailed(PipelineInstanceNode pipelineInstanceNode) {
        try {
            performTransaction(() -> {
                pipelineInstanceNodeCrud().markTransitionFailed(pipelineInstanceNode.getId());
                PipelineInstance pipelineInstance = pipelineInstanceNodeCrud()
                    .retrievePipelineInstance(pipelineInstanceNode);
                PipelineInstance.State.TRANSITION_FAILED.setExecutionClockState(pipelineInstance);
                pipelineInstance.setState(PipelineInstance.State.TRANSITION_FAILED);
                pipelineInstanceCrud().merge(pipelineInstance);
            });
        } finally {
            ZiggyMessenger.publish(new PipelineInstanceFinishedMessage());
        }
    }

    public void clearTransitionFailedState(PipelineInstanceNode pipelineInstanceNode) {
        performTransaction(() -> {
            pipelineInstanceNodeCrud().clearTransitionFailed(pipelineInstanceNode.getId());
            PipelineInstance pipelineInstance = pipelineInstanceNodeCrud()
                .retrievePipelineInstance(pipelineInstanceNode);
            PipelineInstance.State.PROCESSING.setExecutionClockState(pipelineInstance);
            pipelineInstance.setState(PipelineInstance.State.PROCESSING);
            pipelineInstanceCrud().merge(pipelineInstance);
        });
    }

    public PipelineInstanceNodeInformation pipelineInstanceNodeInformation(
        long pipelineInstanceNodeId) {
        PipelineInstanceNode instanceNode = performTransaction(
            () -> pipelineInstanceNodeCrud().retrieve(pipelineInstanceNodeId));
        TaskCounts taskCounts = pipelineTaskDisplayDataOperations().taskCounts(instanceNode);
        return new PipelineInstanceNodeInformation(instanceNode, taskCounts);
    }

    public PipelineInstanceNode bindParameterSets(PipelineDefinitionNode definitionNode,
        PipelineInstanceNode instanceNode) {
        return performTransaction(() -> {
            PipelineInstanceNode databaseNode = instanceNode.getId() == null ? merge(instanceNode)
                : pipelineInstanceNode(instanceNode.getId());
            PipelineDefinitionNode databaseDefinitionNode = pipelineDefinitionNodeCrud()
                .retrieve(definitionNode.getId());
            parametersOperations().bindParameterSets(databaseDefinitionNode.getParameterSetNames(),
                databaseNode.getParameterSets());
            return merge(databaseNode);
        });
    }

    public void addPipelineTasks(PipelineInstanceNode pipelineInstanceNode,
        List<PipelineTask> pipelineTasks) {
        performTransaction(() -> {
            PipelineInstanceNode databaseNode = pipelineInstanceNodeCrud()
                .retrieve(pipelineInstanceNode.getId());
            databaseNode.getPipelineTasks().addAll(pipelineTasks);
            pipelineInstanceNodeCrud().merge(databaseNode);
        });
    }

    public PipelineInstance pipelineInstance(PipelineInstanceNode pipelineInstanceNode) {
        return performTransaction(
            () -> pipelineInstanceNodeCrud().retrievePipelineInstance(pipelineInstanceNode));
    }

    public PipelineInstance pipelineInstance(long pipelineInstanceNodeId) {
        return performTransaction(() -> pipelineInstanceNodeCrud()
            .retrievePipelineInstance(pipelineInstanceNodeCrud().retrieve(pipelineInstanceNodeId)));
    }

    public List<PipelineTask> pipelineTasks(Collection<PipelineInstanceNode> nodes) {
        return performTransaction(() -> pipelineInstanceNodeCrud().retrievePipelineTasks(nodes));
    }

    public PipelineInstanceNode merge(PipelineInstanceNode pipelineInstanceNode) {
        return performTransaction(() -> pipelineInstanceNodeCrud().merge(pipelineInstanceNode));
    }

    public Set<DataFileType> inputDataFileTypes(PipelineInstanceNode pipelineInstanceNode) {
        return performTransaction(
            () -> pipelineInstanceNodeCrud().retrieveInputDataFileTypes(pipelineInstanceNode));
    }

    public Set<ParameterSet> parameterSets(PipelineInstanceNode pipelineInstanceNode) {
        return performTransaction(
            () -> pipelineInstanceNodeCrud().retrieveParameterSets(pipelineInstanceNode));
    }

    PipelineInstanceNodeCrud pipelineInstanceNodeCrud() {
        return pipelineInstanceNodeCrud;
    }

    PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud() {
        return pipelineDefinitionNodeCrud;
    }

    PipelineInstanceCrud pipelineInstanceCrud() {
        return pipelineInstanceCrud;
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }

    PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud() {
        return pipelineModuleDefinitionCrud;
    }

    ParametersOperations parametersOperations() {
        return parametersOperations;
    }
}
