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
 * Dialog for viewing/editing a {@link Parameters} java bean.
 * <p>
 * This dialog box is generated from the Edit Triggers popup that is launched from the Triggers tab.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class EditParametersDialog extends javax.swing.JDialog {
    private EditParametersPanel parametersPanel = null;

    private JPanel dataPanel;
    private JPanel buttonPanel;
    private JButton cancelButton;
    private JButton saveButton;

    private final ParameterSet parameterSet;

    private boolean cancelled = false;

    public EditParametersDialog(JFrame frame, ParameterSet parameterSet) {
        super(frame, true);
        this.parameterSet = parameterSet;

        initGUI();
    }

    public static Parameters editParameters(ParameterSet parameterSet) {
        EditParametersDialog dialog = ZiggyGuiConsole.newEditParametersDialog(parameterSet);
        dialog.setVisible(true); // blocks until user presses a button

        if (!dialog.cancelled) {
            return dialog.parametersPanel.getParameters();
        }
        return null;
    }

    private void saveButtonActionPerformed() {
        setVisible(false);
    }

    private void cancelButtonActionPerformed() {
        cancelled = true;
        setVisible(false);
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            setSize(700, 500);

            setTitle("Edit Parameter Set: " + parameterSet.getName());
        } catch (Exception e) {
            MessageUtil.showError(this, e);
            e.printStackTrace();
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
            Parameters paramsObj = parameterSet.parametersInstance();
            parametersPanel = new EditParametersPanel(paramsObj);
        }
        return parametersPanel;
    }

    private JPanel getButtonPanel() {
        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(40);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getSaveButton());
            buttonPanel.add(getCancelButton());
        }
        return buttonPanel;
    }

    private JButton getSaveButton() {
        if (saveButton == null) {
            saveButton = new JButton();
            saveButton.setText("save");
            saveButton.addActionListener(evt -> saveButtonActionPerformed());
        }
        return saveButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("cancel");
            cancelButton.addActionListener(evt -> cancelButtonActionPerformed());
        }
        return cancelButton;
    }
}
