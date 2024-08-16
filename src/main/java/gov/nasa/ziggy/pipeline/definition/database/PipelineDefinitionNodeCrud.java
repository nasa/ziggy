package gov.nasa.ziggy.pipeline.definition.database;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources_;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode_;

/**
 * CRUD class for {@link PipelineDefinitionNode}.
 *
 * @author PT
 */
public class PipelineDefinitionNodeCrud extends AbstractCrud<PipelineDefinitionNode> {

    @Override
    public Class<PipelineDefinitionNode> componentClass() {
        return PipelineDefinitionNode.class;
    }

    public PipelineDefinitionNode retrieve(long pipelineDefinitionNodeId) {
        ZiggyQuery<PipelineDefinitionNode, PipelineDefinitionNode> query = createZiggyQuery(
            PipelineDefinitionNode.class);
        return uniqueResult(query.column(PipelineDefinitionNode_.id).in(pipelineDefinitionNodeId));
    }

    /**
     * Retrieves the {@link PipelineDefinitionNodeExecutionResources} for a given
     * {@link PipelineDefinitionNode}. If none exists, one is created and persisted (and then
     * returned, of course).
     */
    public PipelineDefinitionNodeExecutionResources retrieveExecutionResources(
        PipelineDefinitionNode node) {

        ZiggyQuery<PipelineDefinitionNodeExecutionResources, PipelineDefinitionNodeExecutionResources> query = createZiggyQuery(
            PipelineDefinitionNodeExecutionResources.class);
        query.column(PipelineDefinitionNodeExecutionResources_.pipelineName)
            .in(node.getPipelineName());
        query.column(PipelineDefinitionNodeExecutionResources_.pipelineModuleName)
            .in(node.getModuleName());
        PipelineDefinitionNodeExecutionResources executionResources = uniqueResult(query);
        if (executionResources == null) {
            executionResources = new PipelineDefinitionNodeExecutionResources(
                node.getPipelineName(), node.getModuleName());
            persist(executionResources);
        }
        return executionResources;
    }

    public List<PipelineDefinitionNode> retrieveNextNodes(PipelineDefinitionNode node) {
        ZiggyQuery<PipelineDefinitionNode, PipelineDefinitionNode> query = createZiggyQuery(
            PipelineDefinitionNode.class);
        query.column(PipelineDefinitionNode_.id).in(node.getId());
        query.column(PipelineDefinitionNode_.nextNodes).select();
        return list(query);
    }

    public Set<String> retrieveParameterSetNames(PipelineDefinitionNode node) {
        ZiggyQuery<PipelineDefinitionNode, String> query = createZiggyQuery(
            PipelineDefinitionNode.class, String.class);
        query.column(PipelineDefinitionNode_.id).in(node.getId());
        query.column(PipelineDefinitionNode_.parameterSetNames).select();
        return new HashSet<>(list(query));
    }

    public Set<DataFileType> retrieveInputDataFileTypes(PipelineDefinitionNode node) {
        ZiggyQuery<PipelineDefinitionNode, DataFileType> query = createZiggyQuery(
            PipelineDefinitionNode.class, DataFileType.class);
        query.column(PipelineDefinitionNode_.id).in(node.getId());
        query.column(PipelineDefinitionNode_.inputDataFileTypes).select();
        return new HashSet<>(list(query));
    }

    public Set<DataFileType> retrieveOutputDataFileTypes(PipelineDefinitionNode node) {
        ZiggyQuery<PipelineDefinitionNode, DataFileType> query = createZiggyQuery(
            PipelineDefinitionNode.class, DataFileType.class);
        query.column(PipelineDefinitionNode_.id).in(node.getId());
        query.column(PipelineDefinitionNode_.outputDataFileTypes).select();
        return new HashSet<>(list(query));
    }

    public Set<ModelType> retrieveModelTypes(PipelineDefinitionNode node) {
        ZiggyQuery<PipelineDefinitionNode, ModelType> query = createZiggyQuery(
            PipelineDefinitionNode.class, ModelType.class);
        query.column(PipelineDefinitionNode_.id).in(node.getId());
        query.column(PipelineDefinitionNode_.modelTypes).select();
        return new HashSet<>(list(query));
    }
}
