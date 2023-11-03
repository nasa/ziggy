package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import gov.nasa.ziggy.pipeline.TriggerValidationResults;

/**
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class PipelineValidationResultsDialog extends javax.swing.JDialog {
    private final TriggerValidationResults results;

    public PipelineValidationResultsDialog(Window owner, TriggerValidationResults results) {
        super(owner, DEFAULT_MODALITY_TYPE);
        this.results = results;

        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Pipeline validation errors");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);

        pack();
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    private JPanel createDataPanel() {
        JTextArea reportTextArea = new JTextArea();
        reportTextArea.setText(results.errorReport());
        reportTextArea.setEditable(false);
        JPanel dataPanel = new JPanel(new BorderLayout());
        dataPanel.add(new JScrollPane(reportTextArea), BorderLayout.CENTER);
        return dataPanel;
    }

    public static void showValidationResults(Window owner, TriggerValidationResults results) {
        new PipelineValidationResultsDialog(owner, results).setVisible(true);
    }
}
