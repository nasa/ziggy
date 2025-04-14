package gov.nasa.ziggy.ui.pipeline;

import java.util.LinkedList;
import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.ComboBoxModel;

import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;

@SuppressWarnings("serial")
public class PipelineNodesListModel extends AbstractListModel<String>
    implements ComboBoxModel<String> {

    private final List<String> pipelineStepNames = new LinkedList<>();
    private final List<PipelineNode> pipelineNodes = new LinkedList<>();

    private String selectedName = null;
    private final Pipeline pipeline;

    public PipelineNodesListModel(Pipeline pipeline) {
        this.pipeline = pipeline;

        update();
    }

    public void update() {
        pipelineStepNames.clear();
        pipelineNodes.clear();

        addNodes(pipeline, pipeline.getRootNodes());

        if (pipelineStepNames.size() > 0) {
            selectedName = pipelineStepNames.get(0);
        }

        fireContentsChanged(this, 0, pipelineStepNames.size() - 1);
    }

    private void addNodes(Pipeline pipeline, List<PipelineNode> nodes) {
        for (PipelineNode node : nodes) {
            if (node != null) {
                StringBuilder pipelineStepName = new StringBuilder()
                    .append(node.getPipelineStepName())
                    .append(" (node:")
                    .append(node.getId())
                    .append(")");
                pipelineStepNames.add(pipelineStepName.toString());
                pipelineNodes.add(node);
            }
            addNodes(pipeline, node.getNextNodes());
        }
    }

    public PipelineNode getSelectedPipelineNode() {
        int selectedIndex = pipelineStepNames.indexOf(selectedName);

        if (selectedIndex != -1) {
            return getPipelineNodeAt(selectedIndex);
        }
        return null;
    }

    public PipelineNode getPipelineNodeAt(int index) {
        return pipelineNodes.get(index);
    }

    @Override
    public String getElementAt(int index) {
        return pipelineStepNames.get(index);
    }

    @Override
    public int getSize() {
        return pipelineStepNames.size();
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
