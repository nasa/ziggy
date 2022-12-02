package gov.nasa.ziggy.ui.config.parameters;

import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.common.ZTable;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParamSetSelectorDialog extends javax.swing.JDialog {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ParamSetSelectorDialog.class);

    private JPanel dataPanel;
    private JScrollPane paramSetsScrollPane;
    private ZTable paramSetsTable;
    private JButton cancelButton;
    private JButton okButton;
    private JPanel actionPanel;
    private boolean cancelled = false;

    private ParameterSetsTableModel paramSetsTableModel;

    public ParamSetSelectorDialog(JFrame frame) {
        super(frame, true);
        initGUI();
    }

    public ParamSetSelectorDialog(Dialog owner) {
        super(owner, true);
        initGUI();
    }

    public ParameterSet selectParamSet() {
        setVisible(true); // blocks until user presses a button

        if (!cancelled) {
            int selectedModelRow = paramSetsTable
                .convertRowIndexToModel(paramSetsTable.getSelectedRow());
            if (selectedModelRow != -1) {
                return paramSetsTableModel.getParamSetAtRow(selectedModelRow);
            }
        }
        return null;
    }

    private void okButtonActionPerformed() {
        setVisible(false);
    }

    private void cancelButtonActionPerformed() {
        cancelled = true;

        setVisible(false);
    }

    private void initGUI() {
        try {
            {
                setTitle("Select new Parameter Set");
            }
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            setSize(400, 600);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            BorderLayout dataPanelLayout = new BorderLayout();
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getParamSetsScrollPane(), BorderLayout.CENTER);
        }
        return dataPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(50);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getOkButton());
            actionPanel.add(getCancelButton());
        }
        return actionPanel;
    }

    private JButton getOkButton() {
        if (okButton == null) {
            okButton = new JButton();
            okButton.setText("Ok");
            okButton.addActionListener(evt -> okButtonActionPerformed());
        }
        return okButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("Cancel");
            cancelButton.addActionListener(evt -> cancelButtonActionPerformed());
        }
        return cancelButton;
    }

    private JScrollPane getParamSetsScrollPane() {
        if (paramSetsScrollPane == null) {
            paramSetsScrollPane = new JScrollPane();
            paramSetsScrollPane.setViewportView(getParamSetsTable());
        }
        return paramSetsScrollPane;
    }

    private ZTable getParamSetsTable() {
        if (paramSetsTable == null) {
            try {
                paramSetsTableModel = new ParameterSetsTableModel();
                paramSetsTable = new ZTable();
                paramSetsTable.setModel(paramSetsTableModel);
            } catch (PipelineException e) {
                MessageUtil.showError(this, e);
            }
        }
        return paramSetsTable;
    }
}
