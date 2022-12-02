package gov.nasa.ziggy.ui.ops.triggers;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.TriggerValidationResults;

@SuppressWarnings("serial")
public class TriggerValidationResultsDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(TriggerValidationResultsDialog.class);

    private JPanel dataPanel;
    private JTextArea reportTextArea;
    private JScrollPane reportScrollPane;
    private JButton closeButton;
    private JPanel buttonPanel;
    private final TriggerValidationResults results;

    public TriggerValidationResultsDialog(JFrame frame, TriggerValidationResults results) {
        super(frame, true);

        this.results = results;

        initGUI();
    }

    public TriggerValidationResultsDialog(JDialog dialog, TriggerValidationResults results) {
        super(dialog, true);

        this.results = results;

        initGUI();
    }

    public static void showValidationResults(JDialog parent, TriggerValidationResults results) {
        TriggerValidationResultsDialog dialog = new TriggerValidationResultsDialog(parent, results);
        dialog.setVisible(true); // blocks until user closes
    }

    private void closeButtonActionPerformed(ActionEvent evt) {
        log.debug("closeButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void initGUI() {
        try {
            {
                setTitle("Trigger Validation Errors");
                getContentPane().add(getDataPanel(), BorderLayout.CENTER);
                getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            }
            this.setSize(1042, 405);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            BorderLayout dataPanelLayout = new BorderLayout();
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getReportScrollPane(), BorderLayout.CENTER);
        }
        return dataPanel;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            buttonPanel.add(getCloseButton());
        }
        return buttonPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("close");
            closeButton.addActionListener(evt -> closeButtonActionPerformed(evt));
        }
        return closeButton;
    }

    private JScrollPane getReportScrollPane() {
        if (reportScrollPane == null) {
            reportScrollPane = new JScrollPane();
            reportScrollPane.setViewportView(getReportTextArea());
        }
        return reportScrollPane;
    }

    private JTextArea getReportTextArea() {
        if (reportTextArea == null) {
            reportTextArea = new JTextArea();
            reportTextArea.setText(results.errorReport());
            reportTextArea.setEditable(false);
        }
        return reportTextArea;
    }
}
