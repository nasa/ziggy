package gov.nasa.ziggy.pipeline.definition.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Operations class for methods mainly related to {@link PipelineDefinitionNode} instances.
 *
 * @author PT
 */
public class PipelineDefinitionNodeOperations extends DatabaseOperations {

    private PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud = new PipelineDefinitionNodeCrud();
    private PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();

    public PipelineDefinitionNodeExecutionResources pipelineDefinitionNodeExecutionResources(
        PipelineDefinitionNode node) {
        return performTransaction(
            () -> pipelineDefinitionNodeCrud().retrieveExecutionResources(node));
    }

    public PipelineDefinitionNodeExecutionResources mergeExecutionResources(
        PipelineDefinitionNodeExecutionResources executionResources) {
        return performTransaction(() -> pipelineDefinitionNodeCrud().merge(executionResources));
    }

    public List<PipelineDefinitionNode> nextNodes(PipelineDefinitionNode pipelineDefinitionNode) {
        return performTransaction(
            () -> pipelineDefinitionNodeCrud().retrieveNextNodes(pipelineDefinitionNode));
    }

    /**
     * Recursively constructs a list of {@link PipelineDefinitionNode}s for a given
     * {@link PipelineDefinition}.
     */
    public List<PipelineDefinitionNode> pipelineDefinitionNodesForPipelineDefinition(
        String pipelineName) {
        return performTransaction(new ReturningDatabaseTransaction<List<PipelineDefinitionNode>>() {

            @Override
            public void catchBlock(Throwable e) {
                System.out.println("Unable to retrieve pipeline: " + e);
            }

            @Override
            public List<PipelineDefinitionNode> transaction() {
                List<PipelineDefinitionNode> pipelineDefinitionNodes = new ArrayList<>();
                PipelineDefinition pipelineDefinition = pipelineDefinitionCrud()
                    .retrieveLatestVersionForName(pipelineName);
                addNodesToList(pipelineDefinitionNodes, pipelineDefinition.getRootNodes());
                return pipelineDefinitionNodes;
            }
        });
    }

    /**
     * Performs the actual recursion required by
     * {@link #pipelineDefinitionNodesForPipelineDefinition(String)}.
     */
    private void addNodesToList(List<PipelineDefinitionNode> pipelineDefinitionNodes,
        List<PipelineDefinitionNode> nodes) {
        for (PipelineDefinitionNode node : nodes) {
            pipelineDefinitionNodes.add(node);
            addNodesToList(pipelineDefinitionNodes, node.getNextNodes());
        }
    }

    /**
     * Iteratively copies a {@link PipelineDefinitionNode} and all its child nodes. The copied node
     * can be a transient instance or a node in the database.
     */
    public PipelineDefinitionNode deepCopy(PipelineDefinitionNode pipelineDefinitionNode) {
        return performTransaction(() -> {

            // Retrieve the original if it is in the database, which is indicated by the
            // node in the argument posessing a non-null ID.
            PipelineDefinitionNode original = pipelineDefinitionNode.getId() != null
                ? pipelineDefinitionNodeCrud().retrieve(pipelineDefinitionNode.getId())
                : pipelineDefinitionNode;
            PipelineDefinitionNode copy = new PipelineDefinitionNode();
            copy.setModuleName(original.getModuleName());
            copy.setHeapSizeMb(original.getHeapSizeMb());
            copy.setMaxWorkerCount(original.getMaxWorkerCount());

            // For the collections, we want to ensure that the new node's collections are
            // distinct from the original, in case the original is not in the database and
            // the caller of this method intends to keep on using it.
            copy.getParameterSetNames().addAll(original.getParameterSetNames());
            copy.getInputDataFileTypes().addAll(original.getInputDataFileTypes());
            copy.getOutputDataFileTypes().addAll(original.getOutputDataFileTypes());
            copy.getModelTypes().addAll(original.getModelTypes());

            // Duplicate the next nodes, if any.
            List<PipelineDefinitionNode> copyNextNodes = copy.getNextNodes();
            for (PipelineDefinitionNode nextNode : original.getNextNodes()) {
                copyNextNodes.add(deepCopy(nextNode));
            }
            return copy;
        });
    }

    public Set<String> parameterSetNames(PipelineDefinitionNode pipelineDefinitionNode) {
        return performTransaction(
            () -> pipelineDefinitionNodeCrud().retrieveParameterSetNames(pipelineDefinitionNode));
    }

    public Set<DataFileType> inputDataFileTypes(PipelineDefinitionNode pipelineDefinitionNode) {
        return performTransaction(
            () -> pipelineDefinitionNodeCrud().retrieveInputDataFileTypes(pipelineDefinitionNode));
    }

    public Set<DataFileType> outputDataFileTypes(PipelineDefinitionNode pipelineDefinitionNode) {
        return performTransaction(
            () -> pipelineDefinitionNodeCrud().retrieveOutputDataFileTypes(pipelineDefinitionNode));
    }

    public Set<ModelType> modelTypes(PipelineDefinitionNode pipelineDefinitionNode) {
        return performTransaction(
            () -> pipelineDefinitionNodeCrud().retrieveModelTypes(pipelineDefinitionNode));
    }

    PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud() {
        return pipelineDefinitionNodeCrud;
    }

    PipelineDefinitionCrud pipelineDefinitionCrud() {
        return pipelineDefinitionCrud;
    }
}
