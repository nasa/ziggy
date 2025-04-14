package gov.nasa.ziggy.pipeline.definition.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Operations class for methods mainly related to {@link PipelineNode} instances.
 *
 * @author PT
 */
public class PipelineNodeOperations extends DatabaseOperations {

    private PipelineNodeCrud pipelineNodeCrud = new PipelineNodeCrud();
    private PipelineCrud pipelineCrud = new PipelineCrud();

    public PipelineNodeExecutionResources pipelineNodeExecutionResources(PipelineNode node) {
        return performTransaction(() -> pipelineNodeCrud().retrieveExecutionResources(node));
    }

    public PipelineNodeExecutionResources merge(PipelineNodeExecutionResources executionResources) {
        return performTransaction(() -> pipelineNodeCrud().merge(executionResources));
    }

    public List<PipelineNode> nextNodes(PipelineNode pipelineNode) {
        return performTransaction(() -> pipelineNodeCrud().retrieveNextNodes(pipelineNode));
    }

    /**
     * Recursively constructs a list of {@link PipelineNode}s for a given {@link Pipeline}.
     */
    public List<PipelineNode> pipelineNodesForPipeline(String pipelineName) {
        return performTransaction(new ReturningDatabaseTransaction<List<PipelineNode>>() {

            @Override
            public void catchBlock(Throwable e) {
                System.out.println("Unable to retrieve pipeline: " + e);
            }

            @Override
            public List<PipelineNode> transaction() {
                List<PipelineNode> pipelineNodes = new ArrayList<>();
                Pipeline pipeline = pipelineCrud().retrieveLatestVersionForName(pipelineName);
                addNodesToList(pipelineNodes, pipeline.getRootNodes());
                return pipelineNodes;
            }
        });
    }

    /**
     * Performs the actual recursion required by {@link #pipelineNodesForPipeline(String)}.
     */
    private void addNodesToList(List<PipelineNode> pipelineNodes, List<PipelineNode> nodes) {
        for (PipelineNode node : nodes) {
            pipelineNodes.add(node);
            addNodesToList(pipelineNodes, node.getNextNodes());
        }
    }

    /**
     * Iteratively copies a {@link PipelineNode} and all its child nodes. The copied node can be a
     * transient instance or a node in the database.
     */
    public PipelineNode deepCopy(PipelineNode pipelineNode) {
        return performTransaction(() -> {

            // Retrieve the original if it is in the database, which is indicated by the
            // node in the argument posessing a non-null ID.
            PipelineNode original = pipelineNode.getId() != null
                ? pipelineNodeCrud().retrieve(pipelineNode.getId())
                : pipelineNode;
            PipelineNode copy = new PipelineNode();
            copy.setPipelineStepName(original.getPipelineStepName());
            copy.setHeapSizeMb(original.getHeapSizeMb());
            copy.setMaxWorkerCount(original.getMaxWorkerCount());
            copy.setPipelineName(original.getPipelineName());
            copy.setSingleSubtask(original.getSingleSubtask());

            // For the collections, we want to ensure that the new node's collections are
            // distinct from the original, in case the original is not in the database and
            // the caller of this method intends to keep on using it.
            copy.getParameterSetNames().addAll(original.getParameterSetNames());
            copy.getInputDataFileTypes().addAll(original.getInputDataFileTypes());
            copy.getOutputDataFileTypes().addAll(original.getOutputDataFileTypes());
            copy.getModelTypes().addAll(original.getModelTypes());

            // Duplicate the next nodes, if any.
            List<PipelineNode> copyNextNodes = copy.getNextNodes();
            for (PipelineNode nextNode : original.getNextNodes()) {
                copyNextNodes.add(deepCopy(nextNode));
            }
            return copy;
        });
    }

    public Set<String> parameterSetNames(PipelineNode pipelineNode) {
        return performTransaction(() -> pipelineNodeCrud().retrieveParameterSetNames(pipelineNode));
    }

    public Set<DataFileType> inputDataFileTypes(PipelineNode pipelineNode) {
        return performTransaction(
            () -> pipelineNodeCrud().retrieveInputDataFileTypes(pipelineNode));
    }

    public Set<DataFileType> outputDataFileTypes(PipelineNode pipelineNode) {
        return performTransaction(
            () -> pipelineNodeCrud().retrieveOutputDataFileTypes(pipelineNode));
    }

    public Set<ModelType> modelTypes(PipelineNode pipelineNode) {
        return performTransaction(() -> pipelineNodeCrud().retrieveModelTypes(pipelineNode));
    }

    PipelineNodeCrud pipelineNodeCrud() {
        return pipelineNodeCrud;
    }

    PipelineCrud pipelineCrud() {
        return pipelineCrud;
    }
}
