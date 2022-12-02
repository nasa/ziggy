package gov.nasa.ziggy.ui.config.pipeline;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.WindowConstants;
import javax.swing.border.BevelBorder;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;

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
        initGUI();
    }

    public PipelineNodeWidget(PipelineDefinitionNode pipelineNode,
        PipelineDefinitionNode pipelineNodeParent) {
        this.pipelineNode = pipelineNode;
        this.pipelineNodeParent = pipelineNodeParent;
        initGUI();
    }

    public PipelineNodeWidget(PipelineDefinition pipeline) {
        this.pipeline = pipeline;
        initGUI();
    }

    private void initGUI() {
        try {
            JLabel nodeLabel = getLabel();

            GridBagLayout thisLayout = new GridBagLayout();
            setLayout(thisLayout);
            // this.setPreferredSize(new java.awt.Dimension(180, 25));
            setPreferredSize(nodeLabel.getPreferredSize());
            setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
            this.add(nodeLabel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        } catch (Exception e) {
            e.printStackTrace();
        }
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
                    uowtgShortName = pipelineNode.getUnitOfWorkGenerator().newInstance().toString();
                } catch (Exception e) {
                }
                label.setText(pipelineNode.getModuleName().getName() + " (" + uowtgShortName + ")");
            }
        }
        return label;
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.getContentPane().add(new PipelineNodeWidget());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
