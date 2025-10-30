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
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance_;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode_;
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
     * Retrieves the PipelineInstanceNode for the specified id.
     */
    public PipelineInstanceNode retrieve(long id) {
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = createZiggyQuery(
            PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.id).in(id);

        return uniqueResult(query);
    }

    /**
     * Retrieves the PipelineInstanceNode associated with the given pipeline task.
     */
    public PipelineInstanceNode retrieve(PipelineTask pipelineTask) {
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = createZiggyQuery(
            PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.pipelineTasks).contains(pipelineTask);
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

        // The main query gets the inputDataFileTypes from the PipelineNode.
        ZiggyQuery<PipelineNode, DataFileType> query = createZiggyQuery(PipelineNode.class,
            DataFileType.class);
        query.column(PipelineNode_.inputDataFileTypes).select();

        // The subquery gets the PipelineNode ID from the PipelineInstanceNode.
        ZiggyQuery<PipelineInstanceNode, Long> pipelineNodeIdQuery = query
            .ziggySubquery(PipelineInstanceNode.class, Long.class);
        pipelineNodeIdQuery.column(PipelineInstanceNode_.id).in(pipelineInstanceNode.getId());
        pipelineNodeIdQuery.select(pipelineNodeIdQuery.getRoot()
            .get(PipelineInstanceNode_.pipelineNode)
            .get(PipelineNode_.id));

        // Put it all together.
        query.column(PipelineNode_.id).in(pipelineNodeIdQuery);
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
