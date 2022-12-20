package gov.nasa.ziggy.ui.ops.instances;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.table.TableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.common.ZTable;

@SuppressWarnings("serial")
public class AlertLogDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(AlertLogDialog.class);

    private JScrollPane alertLogScrollPane;
    private JPanel buttonPanel;
    private ZTable alertLogTable;
    private JButton closeButton;

    private final long pipelineInstanceId;

    public AlertLogDialog(JFrame frame, long pipelineInstanceId) {
        super(frame, true);

        this.pipelineInstanceId = pipelineInstanceId;

        initGUI();
    }

    private void closeButtonActionPerformed(ActionEvent evt) {
        log.debug("closeButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    public static void showAlertLogDialog(JFrame frame, long pipelineInstanceId) {
        AlertLogDialog dialog = new AlertLogDialog(frame, pipelineInstanceId);

        dialog.setVisible(true);
    }

    private void initGUI() {
        try {
            {
                setTitle("Alerts");
                getContentPane().add(getAlertLogScrollPane(), BorderLayout.CENTER);
                getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            }
            this.setSize(1173, 468);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JScrollPane getAlertLogScrollPane() {
        if (alertLogScrollPane == null) {
            alertLogScrollPane = new JScrollPane();
            alertLogScrollPane.setViewportView(getAlertLogTable());
        }
        return alertLogScrollPane;
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
            closeButton.addActionListener(this::closeButtonActionPerformed);
        }
        return closeButton;
    }

    private ZTable getAlertLogTable() {
        if (alertLogTable == null) {
            TableModel alertLogTableModel = new AlertLogTableModel(pipelineInstanceId);
            alertLogTable = new ZTable();
            alertLogTable.setTextWrappingEnabled(true);
            alertLogTable.setRowShadingEnabled(true);
            alertLogTable.setModel(alertLogTableModel);
        }
        return alertLogTable;
    }
}
