package gov.nasa.ziggy.ui.pipeline;

import java.awt.Font;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.border.BevelBorder;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class PipelineNodeWidget extends javax.swing.JPanel {
    private JLabel label;

    private final PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();

    public PipelineNodeWidget() {
        this(null);
    }

    public PipelineNodeWidget(PipelineDefinitionNode pipelineNode) {
        buildComponent(pipelineNode);
    }

    public String text() {
        return label.getText();
    }

    private void buildComponent(PipelineDefinitionNode pipelineNode) {
        label = createLabel(pipelineNode);
        setPreferredSize(label.getPreferredSize());
        setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
        add(label);
    }

    private JLabel createLabel(PipelineDefinitionNode pipelineNode) {
        JLabel label = new JLabel();
        if (pipelineNode == null) {
            // START node
            label.setText("START");
            label.setFont(new java.awt.Font("Dialog", Font.BOLD, 16));
        } else {
            String uowGeneratorName = "-";
            try {
                uowGeneratorName = pipelineModuleDefinitionOperations()
                    .unitOfWorkGenerator(pipelineNode.getModuleName())
                    .newInstance()
                    .getClass()
                    .getSimpleName();
            } catch (Exception e) {
            }
            label.setText(pipelineNode.getModuleName() + " (" + uowGeneratorName + ")");
        }
        return label;
    }

    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
        return pipelineModuleDefinitionOperations;
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new PipelineNodeWidget());
    }
}
