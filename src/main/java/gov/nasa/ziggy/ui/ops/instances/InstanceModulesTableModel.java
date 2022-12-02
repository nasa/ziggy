package gov.nasa.ziggy.ui.ops.instances;

import java.util.LinkedList;
import java.util.List;

import javax.swing.table.AbstractTableModel;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.ui.proxy.PipelineInstanceNodeCrudProxy;
import gov.nasa.ziggy.util.dispmod.InstanceModulesDisplayModel;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class InstanceModulesTableModel extends AbstractTableModel {
    private InstanceModulesDisplayModel instanceModulesDisplayModel = null;
    private List<PipelineInstanceNode> pipelineInstanceNodes = new LinkedList<>();

    public InstanceModulesTableModel(PipelineInstance instance) {
        pipelineInstanceNodes.clear();

        if (instance != null) {
            PipelineInstanceNodeCrudProxy pipelineInstanceNodeCrud = new PipelineInstanceNodeCrudProxy();
            pipelineInstanceNodes = pipelineInstanceNodeCrud.retrieveAll(instance);
        }

        instanceModulesDisplayModel = new InstanceModulesDisplayModel(pipelineInstanceNodes);
    }

    @Override
    public int getColumnCount() {
        return instanceModulesDisplayModel.getColumnCount();
    }

    @Override
    public int getRowCount() {
        return instanceModulesDisplayModel.getRowCount();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return instanceModulesDisplayModel.getValueAt(rowIndex, columnIndex);
    }

    public PipelineInstanceNode getPipelineNodeAt(int index) {
        return pipelineInstanceNodes.get(index);
    }

    @Override
    public String getColumnName(int column) {
        return instanceModulesDisplayModel.getColumnName(column);
    }
}
