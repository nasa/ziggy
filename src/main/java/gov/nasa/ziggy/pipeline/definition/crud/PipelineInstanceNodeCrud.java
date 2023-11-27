package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
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
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = createZiggyQuery(
            PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.pipelineInstance).in(pipelineInstance);
        query.column(PipelineInstanceNode_.id).ascendingOrder();
        List<PipelineInstanceNode> instanceNodes = list(query);
        populateXmlFields(instanceNodes);
        return instanceNodes;
    }

    public PipelineInstanceNode markTransitionComplete(long pipelineInstanceNodeId) {
        PipelineInstanceNode node = uniqueResult(
            createZiggyQuery(PipelineInstanceNode.class).column(PipelineInstanceNode_.id)
                .in(pipelineInstanceNodeId));
        node.setTransitionComplete(true);
        return merge(node);
    }

    public PipelineInstanceNode markTransitionIncomplete(long pipelineInstanceNodeId) {
        PipelineInstanceNode node = uniqueResult(
            createZiggyQuery(PipelineInstanceNode.class).column(PipelineInstanceNode_.id)
                .in(pipelineInstanceNodeId));
        node.setTransitionComplete(false);
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

        PipelineInstanceNode instanceNode = uniqueResult(query);
        populateXmlFields(instanceNode);
        return instanceNode;
    }

    /**
     * Retrieve the PipelineInstanceNode for the specified PipelineInstance and
     * PipelineDefinitionNode.
     *
     * @param pipelineInstance
     * @param pipelineDefinitionNode
     * @return
     */
    public PipelineInstanceNode retrieve(PipelineInstance pipelineInstance,
        PipelineDefinitionNode pipelineDefinitionNode) {
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = createZiggyQuery(
            PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.pipelineInstance)
            .in(pipelineInstance)
            .column(PipelineInstanceNode_.pipelineDefinitionNode)
            .in(pipelineDefinitionNode);

        PipelineInstanceNode instanceNode = uniqueResult(query);
        if (instanceNode == null) {
            return null;
        }
        populateXmlFields(instanceNode);
        return instanceNode;
    }

    private void populateXmlFields(PipelineInstanceNode node) {
        node.populateXmlFields();
    }

    private void populateXmlFields(Collection<PipelineInstanceNode> nodes) {
        for (PipelineInstanceNode node : nodes) {
            populateXmlFields(node);
        }
    }

    @Override
    public Class<PipelineInstanceNode> componentClass() {
        return PipelineInstanceNode.class;
    }
}
