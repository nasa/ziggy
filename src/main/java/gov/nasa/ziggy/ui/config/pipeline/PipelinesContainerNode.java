package gov.nasa.ziggy.ui.config.pipeline;

import java.util.Collection;
import java.util.LinkedList;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.config.TreeContainerNode;
import gov.nasa.ziggy.ui.proxy.PipelineDefinitionCrudProxy;

/**
 * @author Todd Klaus
 */
public class PipelinesContainerNode extends TreeContainerNode {
    private final PipelineDefinitionCrudProxy pipelineDefinitionCrud;

    public PipelinesContainerNode() {
        super("Pipelines");
        pipelineDefinitionCrud = new PipelineDefinitionCrudProxy();
    }

    public void load() {
    }

    @Override
    public void expand(DefaultTreeModel model, DefaultMutableTreeNode node)
        throws PipelineUIException {
        if (!loaded) {
            deleteAllChildren(model, node);

            int index = 0;
            Collection<PipelineDefinition> pipelinesCollection = new LinkedList<>();
            try {
                // TODO: update when pipelines are added/deleted, or when the db session is
                // invalidated
                try {
                    pipelinesCollection = pipelineDefinitionCrud.retrieveLatestVersions();
                } catch (ConsoleSecurityException ignore) {
                }
            } catch (Throwable e) {
                throw new PipelineUIException("Failed to retrieve Pipeline list from datastore.",
                    e);
            }

            for (PipelineDefinition pipeline : pipelinesCollection) {
                DefaultMutableTreeNode newNode = new DefaultMutableTreeNode(pipeline);
                model.insertNodeInto(newNode, node, index++);
            }

            loaded = true;
        }
    }
}
