package gov.nasa.ziggy.ui.pipeline;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.border.BevelBorder;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.proxy.PipelineModuleDefinitionCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineNodeWidget extends javax.swing.JPanel {
    private JLabel label;
    private PipelineDefinition pipeline = null;
    private PipelineDefinitionNode pipelineNode = null;
    private PipelineDefinitionNode pipelineNodeParent = null;

    public PipelineNodeWidget() {
        buildComponent();
    }

    public PipelineNodeWidget(PipelineDefinitionNode pipelineNode,
        PipelineDefinitionNode pipelineNodeParent) {
        this.pipelineNode = pipelineNode;
        this.pipelineNodeParent = pipelineNodeParent;
        buildComponent();
    }

    public PipelineNodeWidget(PipelineDefinition pipeline) {
        this.pipeline = pipeline;
        buildComponent();
    }

    private void buildComponent() {
        JLabel nodeLabel = getLabel();

        setLayout(new GridBagLayout());
        setPreferredSize(nodeLabel.getPreferredSize());
        setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
        add(nodeLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
            GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
    }

    public boolean isStartNode() {
        return pipeline != null;
    }

    public PipelineDefinition getPipeline() {
        return pipeline;
    }

    /**
     * @return Returns the pipelineNode.
     */
    public PipelineDefinitionNode getPipelineNode() {
        return pipelineNode;
    }

    /**
     * @return Returns the pipelineNodeParent.
     */
    public PipelineDefinitionNode getPipelineNodeParent() {
        return pipelineNodeParent;
    }

    private JLabel getLabel() {
        if (label == null) {
            label = new JLabel();
            if (pipelineNode == null) {
                // START node
                label.setText("START");
                label.setFont(new java.awt.Font("Dialog", Font.BOLD, 16));
            } else {
                String uowtgShortName = "-";
                try {
                    uowtgShortName = new PipelineModuleDefinitionCrudProxy()
                        .retrieveUnitOfWorkGenerator(pipelineNode.getModuleName())
                        .newInstance()
                        .toString();
                } catch (Exception e) {
                }
                label.setText(pipelineNode.getModuleName() + " (" + uowtgShortName + ")");
            }
        }
        return label;
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new PipelineNodeWidget());
    }
}
