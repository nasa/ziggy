package gov.nasa.ziggy.ui.ops.triggers;

import java.util.LinkedList;
import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.ComboBoxModel;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;

@SuppressWarnings("serial")
public class TriggerModulesListModel extends AbstractListModel<String>
    implements ComboBoxModel<String> {
    private final List<String> moduleNames = new LinkedList<>();
    private final List<PipelineDefinitionNode> pipelineNodes = new LinkedList<>();

    private String selectedName = null;
    private final PipelineDefinition trigger;

    public TriggerModulesListModel(PipelineDefinition trigger) {
        this.trigger = trigger;

        loadFromDatabase();
    }

    public void loadFromDatabase() {
        moduleNames.clear();
        pipelineNodes.clear();

        trigger.buildPaths();

        addNodes(trigger, trigger.getRootNodes());

        if (moduleNames.size() > 0) {
            selectedName = moduleNames.get(0);
        }

        fireContentsChanged(this, 0, moduleNames.size() - 1);
    }

    private void addNodes(PipelineDefinition trigger, List<PipelineDefinitionNode> nodes) {
        for (PipelineDefinitionNode node : nodes) {
            if (node != null) {
                moduleNames.add(node.getModuleName().getName() + " (node:" + node.getId() + ")");
                pipelineNodes.add(node);
            }
            addNodes(trigger, node.getNextNodes());
        }
    }

    public PipelineDefinitionNode getPipelineNodeAt(int index) {
        return pipelineNodes.get(index);
    }

    public PipelineDefinitionNode getSelectedPipelineNode() {
        int selectedIndex = moduleNames.indexOf(selectedName);

        if (selectedIndex != -1) {
            return pipelineNodes.get(selectedIndex);
        } else {
            return null;
        }
    }

    public PipelineDefinitionNode getTriggerNodeAt(int index) {
        return pipelineNodes.get(index);
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
