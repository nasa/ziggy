package gov.nasa.ziggy.ui.config.parameters;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterClassSelectorDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(ParameterClassSelectorDialog.class);

    private boolean cancelled = false;
    private ParameterClassSelectorPanel dataPanel;
    private JButton cancelButton;
    private JButton okButton;
    private JPanel actionPanel;

    public ParameterClassSelectorDialog(JDialog dialog) {
        super(dialog, true);
        initGUI();
    }

    public ParameterClassSelectorDialog(JFrame frame) {
        super(frame, true);
        initGUI();
    }

    public static ClassWrapper<Parameters> selectParameterClass() {
        ParameterClassSelectorDialog dialog = ZiggyGuiConsole.newParameterClassSelectorDialog();
        dialog.setVisible(true); // blocks until user presses a button

        if (!dialog.cancelled) {
            return dialog.dataPanel.getSelectedElement();
        }
        return null;
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);

            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);

            setSize(400, 600);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void okButtonActionPerformed(ActionEvent evt) {
        log.debug("okButton.actionPerformed, event=" + evt);

        ClassWrapper<Parameters> selectedClass = dataPanel.getSelectedElement();

        if (selectedClass == null) {
            MessageUtil.showError(this, "Please select a Parameters class");
        } else {
            setVisible(false);
        }
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);

        cancelled = true;

        setVisible(false);
    }

    private ParameterClassSelectorPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new ParameterClassSelectorPanel();
        }
        return dataPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(40);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getOkButton());
            actionPanel.add(getCancelButton());
        }
        return actionPanel;
    }

    private JButton getOkButton() {
        if (okButton == null) {
            okButton = new JButton();
            okButton.setText("ok");
            okButton.addActionListener(this::okButtonActionPerformed);
        }
        return okButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("cancel");
            cancelButton.addActionListener(this::cancelButtonActionPerformed);
        }
        return cancelButton;
    }
}
