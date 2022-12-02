package gov.nasa.ziggy.ui.ops.instances;

import java.util.LinkedList;
import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.ComboBoxModel;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.ui.proxy.PipelineInstanceNodeCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class InstanceModulesListModel extends AbstractListModel<String>
    implements ComboBoxModel<String> {
    private final List<String> moduleNames = new LinkedList<>();
    private List<PipelineInstanceNode> pipelineNodes = new LinkedList<>();

    private String selectedName = null;

    public InstanceModulesListModel(PipelineInstance instance) {
        moduleNames.clear();
        pipelineNodes.clear();

        if (instance != null) {
            PipelineInstanceNodeCrudProxy pipelineInstanceNodeCrud = new PipelineInstanceNodeCrudProxy();
            pipelineNodes = pipelineInstanceNodeCrud.retrieveAll(instance);

            for (PipelineInstanceNode node : pipelineNodes) {
                moduleNames.add(
                    node.getPipelineModuleDefinition().getName() + " (node:" + node.getId() + ")");
            }
        }

        if (moduleNames.size() > 0) {
            selectedName = moduleNames.get(0);
        }
    }

    public PipelineInstanceNode getPipelineNodeAt(int index) {
        return pipelineNodes.get(index);
    }

    public PipelineInstanceNode getSelectedPipelineNode() {
        int selectedIndex = moduleNames.indexOf(selectedName);

        if (selectedIndex != -1) {
            return pipelineNodes.get(selectedIndex);
        } else {
            return null;
        }
    }

    @Override
    public String getElementAt(int index) {
        return moduleNames.get(index);
    }

    @Override
    public int getSize() {
        return moduleNames.size();
    }

    @Override
    public Object getSelectedItem() {
        return selectedName;
    }

    @Override
    public void setSelectedItem(Object selectedItem) {
        selectedName = (String) selectedItem;
    }
}
