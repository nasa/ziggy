package gov.nasa.ziggy.ui.pipeline;

import java.awt.Font;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.border.BevelBorder;

import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineNodeWidget extends javax.swing.JPanel {
    private JLabel label;

    private final PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();

    public PipelineNodeWidget() {
        this(null);
    }

    public PipelineNodeWidget(PipelineNode pipelineNode) {
        buildComponent(pipelineNode);
    }

    public String text() {
        return label.getText();
    }

    private void buildComponent(PipelineNode pipelineNode) {
        label = createLabel(pipelineNode);
        setPreferredSize(label.getPreferredSize());
        setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
        add(label);
    }

    private JLabel createLabel(PipelineNode pipelineNode) {
        JLabel label = new JLabel();
        if (pipelineNode == null) {
            // START node
            label.setText("START");
            label.setFont(new java.awt.Font("Dialog", Font.BOLD, 16));
        } else {
            String uowGeneratorName = "-";
            try {
                uowGeneratorName = pipelineStepOperations()
                    .unitOfWorkGenerator(pipelineNode.getPipelineStepName())
                    .newInstance()
                    .getClass()
                    .getSimpleName();
            } catch (Exception e) {
            }
            label.setText(pipelineNode.getPipelineStepName() + " (" + uowGeneratorName + ")");
        }
        return label;
    }

    private PipelineStepOperations pipelineStepOperations() {
        return pipelineStepOperations;
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new PipelineNodeWidget());
    }
}
