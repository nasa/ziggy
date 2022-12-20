package gov.nasa.ziggy.ui.config.pipeline;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineNodesTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(PipelineNodesTableModel.class);

    private static class NodeWrapper {
        public PipelineDefinitionNode node;
        public PipelineDefinitionNode predecessorNode;
        public String predecessorId;

        public NodeWrapper(PipelineDefinitionNode node, PipelineDefinitionNode predecessorNode) {
            this.node = node;
            this.predecessorNode = predecessorNode;
            if (predecessorNode != null) {
                predecessorId = predecessorNode.getId() + "";
            } else {
                predecessorId = "START";
            }
        }
    }

    private final PipelineDefinition pipeline;
    private List<NodeWrapper> pipelineNodes = new LinkedList<>();

    public PipelineNodesTableModel(PipelineDefinition pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public void loadFromDatabase() {
        log.debug("loadFromDatabase() - start");

        try {
            pipelineNodes = new LinkedList<>();

            // TODO: currently only supports one root node
            PipelineDefinitionNode rootNode = pipeline.getRootNodes().get(0);
            pipelineNodes.add(new NodeWrapper(rootNode, null));
            addChildren(pipelineNodes, rootNode);
        } catch (ConsoleSecurityException ignore) {
        }
        fireTableDataChanged();

        log.debug("loadFromDatabase() - end");
    }

    /**
     * @param list
     * @param node
     */
    private void addChildren(List<NodeWrapper> list, PipelineDefinitionNode node) {
        for (PipelineDefinitionNode childNode : node.getNextNodes()) {
            list.add(new NodeWrapper(childNode, node));
            addChildren(list, childNode);
        }
    }

    public PipelineDefinitionNode getPipelineNodeAtRow(int rowIndex) {
        validityCheck();
        return pipelineNodes.get(rowIndex).node;
    }

    public PipelineDefinitionNode getPredecessorForNodeAtRow(int rowIndex) {
        validityCheck();
        return pipelineNodes.get(rowIndex).predecessorNode;
    }

    @Override
    public int getRowCount() {
        validityCheck();
        return pipelineNodes.size();
    }

    @Override
    public int getColumnCount() {
        return 3;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();

        NodeWrapper nodeWrapper = pipelineNodes.get(rowIndex);

        switch (columnIndex) {
            case 0:
                return nodeWrapper.node.getId();
            case 1:
                return nodeWrapper.node.getModuleName();
            case 2:
                return nodeWrapper.predecessorId;
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        return String.class;
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "ID";
            case 1:
                return "Module Name";
            case 2:
                return "Predecessor";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
