package gov.nasa.ziggy.pipeline.definition.database;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources_;
import gov.nasa.ziggy.pipeline.definition.PipelineNode_;

/**
 * CRUD class for {@link PipelineNode}.
 *
 * @author PT
 */
public class PipelineNodeCrud extends AbstractCrud<PipelineNode> {

    @Override
    public Class<PipelineNode> componentClass() {
        return PipelineNode.class;
    }

    public PipelineNode retrieve(long pipelineNodeId) {
        ZiggyQuery<PipelineNode, PipelineNode> query = createZiggyQuery(PipelineNode.class);
        return uniqueResult(query.column(PipelineNode_.id).in(pipelineNodeId));
    }

    /**
     * Retrieves the {@link PipelineNodeExecutionResources} for a given {@link PipelineNode}. If
     * none exists, one is created and persisted (and then returned, of course).
     */
    public PipelineNodeExecutionResources retrieveExecutionResources(PipelineNode node) {

        ZiggyQuery<PipelineNodeExecutionResources, PipelineNodeExecutionResources> query = createZiggyQuery(
            PipelineNodeExecutionResources.class);
        query.column(PipelineNodeExecutionResources_.pipelineName).in(node.getPipelineName());
        query.column(PipelineNodeExecutionResources_.pipelineStepName)
            .in(node.getPipelineStepName());
        PipelineNodeExecutionResources executionResources = uniqueResult(query);
        if (executionResources == null) {
            executionResources = new PipelineNodeExecutionResources(node.getPipelineName(),
                node.getPipelineStepName());
            persist(executionResources);
        }
        return executionResources;
    }

    public List<PipelineNode> retrieveNextNodes(PipelineNode node) {
        ZiggyQuery<PipelineNode, PipelineNode> query = createZiggyQuery(PipelineNode.class);
        query.column(PipelineNode_.id).in(node.getId());
        query.column(PipelineNode_.nextNodes).select();
        return list(query);
    }

    public Set<String> retrieveParameterSetNames(PipelineNode node) {
        ZiggyQuery<PipelineNode, String> query = createZiggyQuery(PipelineNode.class, String.class);
        query.column(PipelineNode_.id).in(node.getId());
        query.column(PipelineNode_.parameterSetNames).select();
        return new HashSet<>(list(query));
    }

    public Set<DataFileType> retrieveInputDataFileTypes(PipelineNode node) {
        ZiggyQuery<PipelineNode, DataFileType> query = createZiggyQuery(PipelineNode.class,
            DataFileType.class);
        query.column(PipelineNode_.id).in(node.getId());
        query.column(PipelineNode_.inputDataFileTypes).select();
        return new HashSet<>(list(query));
    }

    public Set<DataFileType> retrieveOutputDataFileTypes(PipelineNode node) {
        ZiggyQuery<PipelineNode, DataFileType> query = createZiggyQuery(PipelineNode.class,
            DataFileType.class);
        query.column(PipelineNode_.id).in(node.getId());
        query.column(PipelineNode_.outputDataFileTypes).select();
        return new HashSet<>(list(query));
    }

    public Set<ModelType> retrieveModelTypes(PipelineNode node) {
        ZiggyQuery<PipelineNode, ModelType> query = createZiggyQuery(PipelineNode.class,
            ModelType.class);
        query.column(PipelineNode_.id).in(node.getId());
        query.column(PipelineNode_.modelTypes).select();
        return new HashSet<>(list(query));
    }
}
