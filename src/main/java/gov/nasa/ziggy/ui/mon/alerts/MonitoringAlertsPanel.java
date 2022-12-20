package gov.nasa.ziggy.ui.mon.alerts;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.common.ZTable;
import gov.nasa.ziggy.ui.mon.master.Indicator;
import gov.nasa.ziggy.ui.mon.master.MasterStatusPanel;

@SuppressWarnings("serial")
public class MonitoringAlertsPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(MonitoringAlertsPanel.class);

    private JPanel buttonPanel;
    private ZTable alertTable;
    private JButton ackButton;
    private JButton clearButton;
    private JScrollPane alertTableScrollPane;
    private JCheckBox enabledCheckBox;
    private AlertMessageTableModel alertMessageTableModel;

    public MonitoringAlertsPanel() {
        initGUI();

    }

    private void enabledCheckBoxActionPerformed(ActionEvent evt) {
        log.debug("enabledCheckBox.actionPerformed, event=" + evt);

    }

    private void clearButtonActionPerformed(ActionEvent evt) {
        log.debug("clearButton.actionPerformed, event=" + evt);

        alertMessageTableModel.clear();

        MasterStatusPanel.alertsIndicator().setState(Indicator.State.GREEN);
    }

    private void ackButtonActionPerformed(ActionEvent evt) {
        log.debug("ackButton.actionPerformed, event=" + evt);

        MasterStatusPanel.alertsIndicator().setState(Indicator.State.GREEN);
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            setPreferredSize(new Dimension(400, 300));
            this.add(getButtonPanel(), BorderLayout.NORTH);
            this.add(getAlertTableScrollPane(), BorderLayout.CENTER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(20);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getEnabledCheckBox());
            buttonPanel.add(getClearButton());
            buttonPanel.add(getAckButton());
        }
        return buttonPanel;
    }

    private JCheckBox getEnabledCheckBox() {
        if (enabledCheckBox == null) {
            enabledCheckBox = new JCheckBox();
            enabledCheckBox.setText("Enabled");
            enabledCheckBox.setSelected(true);
            enabledCheckBox.addActionListener(this::enabledCheckBoxActionPerformed);
        }
        return enabledCheckBox;
    }

    private JScrollPane getAlertTableScrollPane() {
        if (alertTableScrollPane == null) {
            alertTableScrollPane = new JScrollPane();
            alertTableScrollPane.setViewportView(getAlertTable());
        }
        return alertTableScrollPane;
    }

    private JButton getClearButton() {
        if (clearButton == null) {
            clearButton = new JButton();
            clearButton.setText("Clear");
            clearButton.addActionListener(this::clearButtonActionPerformed);
        }
        return clearButton;
    }

    private ZTable getAlertTable() {
        if (alertTable == null) {
            log.info("Getting alert table together");
            alertMessageTableModel = new AlertMessageTableModel();
            alertTable = new ZTable();
            alertTable.setRowShadingEnabled(true);
            alertTable.setTextWrappingEnabled(true);
            alertTable.setModel(alertMessageTableModel);
            alertTable.setShowVerticalLines(false);
            alertTable.setShowHorizontalLines(false);
        }
        return alertTable;
    }

    private JButton getAckButton() {
        if (ackButton == null) {
            ackButton = new JButton();
            ackButton.setText("Ack");
            ackButton.addActionListener(this::ackButtonActionPerformed);
        }
        return ackButton;
    }

    public AlertMessageTableModel getAlertMessageTableModel() {
        return alertMessageTableModel;
    }

}
