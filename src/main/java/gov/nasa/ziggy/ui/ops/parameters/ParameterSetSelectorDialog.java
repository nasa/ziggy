package gov.nasa.ziggy.ui.ops.parameters;

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
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * {@link JDialog} wrapper for the {@link ParameterSetSelectorPanel}
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetSelectorDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(ParameterSetSelectorDialog.class);

    private final Class<? extends Parameters> filterClass;
    private JPanel dataPanel;
    private JButton cancelButton;
    private JButton selectButton;
    private JPanel actionPanel;
    private ParameterSetSelectorPanel parameterSetSelectorPanel;

    private boolean cancelled = false;

    public ParameterSetSelectorDialog(JFrame frame) {
        super(frame, true);
        filterClass = null;
        initGUI();
    }

    public ParameterSetSelectorDialog(JFrame frame, Class<? extends Parameters> filterClass) {
        super(frame, true);
        this.filterClass = filterClass;
        initGUI();
    }

    /**
     * Select a parameter set of the specified type from the parameter set library
     *
     * @param filterClass
     * @return
     */
    public static ParameterSet selectParameterSet(Class<? extends Parameters> filterClass) {
        ParameterSetSelectorDialog dialog = ZiggyGuiConsole
            .newParameterSetSelectorDialog(filterClass);
        dialog.setVisible(true); // blocks until user presses a button

        if (!dialog.cancelled) {
            return dialog.parameterSetSelectorPanel.getSelected();
        } else {
            return null;
        }
    }

    /**
     * Select a parameter set from the parameter set library with no filtering
     *
     * @return
     */
    public static ParameterSet selectParameterSet() {
        ParameterSetSelectorDialog dialog = ZiggyGuiConsole.newParameterSetSelectorDialog();
        dialog.setVisible(true); // blocks until user presses a button

        if (!dialog.cancelled) {
            return dialog.parameterSetSelectorPanel.getSelected();
        } else {
            return null;
        }
    }

    private void initGUI() {
        try {
            {
                setTitle("Select Parameter Set");
                getContentPane().add(getDataPanel(), BorderLayout.CENTER);
                getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            }
            this.setSize(300, 600);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void selectButtonActionPerformed(ActionEvent evt) {
        log.debug("selectButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);

        cancelled = true;
        setVisible(false);
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            BorderLayout dataPanelLayout = new BorderLayout();
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getParameterSetSelectorPanel(), BorderLayout.CENTER);
        }
        return dataPanel;
    }

    private ParameterSetSelectorPanel getParameterSetSelectorPanel() {
        if (parameterSetSelectorPanel == null) {
            parameterSetSelectorPanel = new ParameterSetSelectorPanel(filterClass);
        }
        return parameterSetSelectorPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(40);
            actionPanelLayout.setHgap(40);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getSelectButton());
            actionPanel.add(getCancelButton());
        }
        return actionPanel;
    }

    private JButton getSelectButton() {
        if (selectButton == null) {
            selectButton = new JButton();
            selectButton.setText("Select");
            selectButton.addActionListener(this::selectButtonActionPerformed);
        }
        return selectButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("Cancel");
            cancelButton.addActionListener(this::cancelButtonActionPerformed);
        }
        return cancelButton;
    }
}
