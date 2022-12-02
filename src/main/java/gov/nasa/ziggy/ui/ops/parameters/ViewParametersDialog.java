package gov.nasa.ziggy.ui.ops.parameters;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;

/**
 * Dialog for viewing (read-only) a {@link Parameters} java bean.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ViewParametersDialog extends javax.swing.JDialog {
    private ViewParametersPanel parametersPanel = null;

    private JPanel dataPanel;
    private JPanel buttonPanel;
    private JButton closeButton;

    private final ParameterSet parameterSet;

    public ViewParametersDialog(JFrame frame, ParameterSet parameterSet) {
        super(frame, true);
        this.parameterSet = parameterSet;

        initGUI();
    }

    public static void viewParameters(ParameterSet parameterSet) {
        ViewParametersDialog dialog = ZiggyGuiConsole.newViewParametersDialog(parameterSet);

        dialog.setVisible(true); // blocks until user presses a button
    }

    private void closeButtonActionPerformed() {
        setVisible(false);
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            setSize(700, 500);

            setTitle("View Parameter Set: " + parameterSet.getName() + " (read-only)");
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            BorderLayout dataPanelLayout = new BorderLayout();
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getParamPanel(), BorderLayout.CENTER);
        }
        return dataPanel;
    }

    private JPanel getParamPanel() throws PipelineException {
        if (parametersPanel == null) {
            parametersPanel = new ViewParametersPanel(parameterSet);
        }
        return parametersPanel;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(40);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getCloseButton());
        }
        return buttonPanel;
    }

    private JButton getCloseButton() {
        if (closeButton == null) {
            closeButton = new JButton();
            closeButton.setText("close");
            closeButton.addActionListener(evt -> closeButtonActionPerformed());
        }
        return closeButton;
    }
}
