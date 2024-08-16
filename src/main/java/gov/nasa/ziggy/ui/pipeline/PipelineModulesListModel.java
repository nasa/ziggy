package gov.nasa.ziggy.ui.pipeline;

import java.util.LinkedList;
import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.ComboBoxModel;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;

@SuppressWarnings("serial")
public class PipelineModulesListModel extends AbstractListModel<String>
    implements ComboBoxModel<String> {

    private final List<String> moduleNames = new LinkedList<>();
    private final List<PipelineDefinitionNode> pipelineNodes = new LinkedList<>();

    private String selectedName = null;
    private final PipelineDefinition pipeline;

    public PipelineModulesListModel(PipelineDefinition pipeline) {
        this.pipeline = pipeline;

        update();
    }

    public void update() {
        moduleNames.clear();
        pipelineNodes.clear();

        addNodes(pipeline, pipeline.getRootNodes());

        if (moduleNames.size() > 0) {
            selectedName = moduleNames.get(0);
        }

        fireContentsChanged(this, 0, moduleNames.size() - 1);
    }

    private void addNodes(PipelineDefinition pipeline, List<PipelineDefinitionNode> nodes) {
        for (PipelineDefinitionNode node : nodes) {
            if (node != null) {
                StringBuilder moduleName = new StringBuilder().append(node.getModuleName())
                    .append(" (node:")
                    .append(node.getId())
                    .append(")");
                moduleNames.add(moduleName.toString());
                pipelineNodes.add(node);
            }
            addNodes(pipeline, node.getNextNodes());
        }
    }

    public PipelineDefinitionNode getSelectedPipelineNode() {
        int selectedIndex = moduleNames.indexOf(selectedName);

        if (selectedIndex != -1) {
            return getPipelineNodeAt(selectedIndex);
        }
        return null;
    }

    public PipelineDefinitionNode getPipelineNodeAt(int index) {
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
