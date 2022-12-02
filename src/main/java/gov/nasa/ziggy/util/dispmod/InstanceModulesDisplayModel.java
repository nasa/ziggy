package gov.nasa.ziggy.util.dispmod;

import java.util.LinkedList;
import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;

/**
 * @author Todd Klaus
 */
public class InstanceModulesDisplayModel extends DisplayModel {
    private List<PipelineInstanceNode> pipelineInstanceNodes = new LinkedList<>();

    public InstanceModulesDisplayModel(List<PipelineInstanceNode> pipelineInstanceNodes) {
        this.pipelineInstanceNodes = pipelineInstanceNodes;
    }

    @Override
    public int getColumnCount() {
        return 5; // name, nTasks, nSub, nComp, nFailed
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Name";
            case 1:
                return "nTasks";
            case 2:
                return "nSubmit";
            case 3:
                return "nComplt";
            case 4:
                return "nFailed";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }

    @Override
    public int getRowCount() {
        return pipelineInstanceNodes.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineInstanceNode node = pipelineInstanceNodes.get(rowIndex);

        switch (columnIndex) {
            case 0:
                return node.getPipelineDefinitionNode().getModuleName();
            case 1:
                return node.getNumTasks();
            case 2:
                return node.getNumSubmittedTasks();
            case 3:
                return node.getNumCompletedTasks();
            case 4:
                return node.getNumFailedTasks();
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }
}
