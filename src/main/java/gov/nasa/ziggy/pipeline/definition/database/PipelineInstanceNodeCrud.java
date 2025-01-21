package gov.nasa.ziggy.pipeline.definition.database;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link PipelineInstanceNode}
 *
 * @author Todd Klaus
 */
public class PipelineInstanceNodeCrud extends AbstractCrud<PipelineInstanceNode> {

    public PipelineInstanceNodeCrud() {
    }

    public PipelineInstanceNodeCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public List<PipelineInstanceNode> retrieveAll(PipelineInstance pipelineInstance) {
        ZiggyQuery<PipelineInstance, PipelineInstanceNode> query = createZiggyQuery(
            PipelineInstance.class, PipelineInstanceNode.class);
        query.column(PipelineInstance_.pipelineInstanceNodes).select();
        query.column(PipelineInstance_.id).in(pipelineInstance.getId());
        List<PipelineInstanceNode> instanceNodes = list(query);
        return populateXmlFields(instanceNodes);
    }

    public PipelineInstanceNode markTransitionComplete(long pipelineInstanceNodeId) {
        PipelineInstanceNode node = uniqueResult(
            createZiggyQuery(PipelineInstanceNode.class).column(PipelineInstanceNode_.id)
                .in(pipelineInstanceNodeId));
        node.setTransitionComplete(true);
        node.setTransitionFailed(false);
        return merge(node);
    }

    public PipelineInstanceNode markTransitionIncomplete(long pipelineInstanceNodeId) {
        PipelineInstanceNode node = uniqueResult(
            createZiggyQuery(PipelineInstanceNode.class).column(PipelineInstanceNode_.id)
                .in(pipelineInstanceNodeId));
        node.setTransitionComplete(false);
        node.setTransitionFailed(false);
        return merge(node);
    }

    public PipelineInstanceNode markTransitionFailed(long pipelineInstanceNodeId) {
        PipelineInstanceNode node = uniqueResult(
            createZiggyQuery(PipelineInstanceNode.class).column(PipelineInstanceNode_.id)
                .in(pipelineInstanceNodeId));
        node.setTransitionComplete(false);
        node.setTransitionFailed(true);
        return merge(node);
    }

    public PipelineInstanceNode clearTransitionFailed(long pipelineInstanceNodeId) {
        PipelineInstanceNode node = uniqueResult(
            createZiggyQuery(PipelineInstanceNode.class).column(PipelineInstanceNode_.id)
                .in(pipelineInstanceNodeId));
        node.setTransitionComplete(false);
        node.setTransitionFailed(false);
        return merge(node);
    }

    /**
     * Retrieve the PipelineInstanceNode for the specified id.
     *
     * @param id
     * @return
     */
    public PipelineInstanceNode retrieve(long id) {
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = createZiggyQuery(
            PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.id).in(id);

        return uniqueResult(query);
    }

    public PipelineInstance retrievePipelineInstance(PipelineInstanceNode pipelineInstanceNode) {
        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);
        query.column(PipelineInstance_.pipelineInstanceNodes).contains(pipelineInstanceNode);
        return uniqueResult(query);
    }

    public List<PipelineTask> retrievePipelineTasks(Collection<PipelineInstanceNode> nodes) {
        ZiggyQuery<PipelineInstanceNode, PipelineTask> query = createZiggyQuery(
            PipelineInstanceNode.class, PipelineTask.class);
        query.column(PipelineInstanceNode_.id)
            .in(nodes.stream().map(PipelineInstanceNode::getId).collect(Collectors.toList()));
        query.column(PipelineInstanceNode_.pipelineTasks).select();
        return list(query);
    }

    public Set<DataFileType> retrieveInputDataFileTypes(PipelineInstanceNode pipelineInstanceNode) {

        // The main query gets the inputDataFileTypes from the PipelineDefinitionNode.
        ZiggyQuery<PipelineDefinitionNode, DataFileType> query = createZiggyQuery(
            PipelineDefinitionNode.class, DataFileType.class);
        query.column(PipelineDefinitionNode_.inputDataFileTypes).select();

        // The subquery gets the PipelineDefinitionNode ID from the PipelineInstanceNode.
        ZiggyQuery<PipelineInstanceNode, Long> definitionNodeIdQuery = query
            .ziggySubquery(PipelineInstanceNode.class, Long.class);
        definitionNodeIdQuery.column(PipelineInstanceNode_.id).in(pipelineInstanceNode.getId());
        definitionNodeIdQuery.select(definitionNodeIdQuery.getRoot()
            .get(PipelineInstanceNode_.pipelineDefinitionNode)
            .get(PipelineDefinitionNode_.id));

        // Put it all together.
        query.column(PipelineDefinitionNode_.id).in(definitionNodeIdQuery);
        return new HashSet<>(list(query));
    }

    private List<PipelineInstanceNode> populateXmlFields(List<PipelineInstanceNode> nodes) {
        return nodes.stream()
            .sorted((o1, o2) -> o1.getId().intValue() - o2.getId().intValue())
            .collect(Collectors.toList());
    }

    public Set<ParameterSet> retrieveParameterSets(PipelineInstanceNode pipelineInstanceNode) {
        ZiggyQuery<PipelineInstanceNode, ParameterSet> query = createZiggyQuery(
            PipelineInstanceNode.class, ParameterSet.class);
        query.column(PipelineInstanceNode_.id).in(pipelineInstanceNode.getId());
        query.column(PipelineInstanceNode_.parameterSets).select();
        return new HashSet<>(list(query));
    }

    public Set<ParameterSet> retrieveParameterSets(PipelineTask pipelineTask) {
        ZiggyQuery<PipelineInstanceNode, ParameterSet> query = createZiggyQuery(
            PipelineInstanceNode.class, ParameterSet.class);
        query.column(PipelineInstanceNode_.pipelineTasks).contains(pipelineTask);
        query.column(PipelineInstanceNode_.parameterSets).select();
        return new HashSet<>(list(query));
    }

    @Override
    public Class<PipelineInstanceNode> componentClass() {
        return PipelineInstanceNode.class;
    }
}
