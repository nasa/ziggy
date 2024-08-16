package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SELECT;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;

import javax.swing.JDialog;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;

/**
 * {@link JDialog} wrapper for the {@link ParameterSetSelectorPanel}
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ParameterSetSelectorDialog extends javax.swing.JDialog {

    private ParameterSetSelectorPanel parameterSetSelectorPanel;
    private boolean cancelled = false;

    private ParameterSetSelectorDialog(Window owner) {
        super(owner, DEFAULT_MODALITY_TYPE);
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("Select parameter set");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(SELECT, this::select),
            createButton(CANCEL, this::cancel)), BorderLayout.SOUTH);

        pack();
    }

    private ParameterSetSelectorPanel createDataPanel() {
        parameterSetSelectorPanel = new ParameterSetSelectorPanel();

        return parameterSetSelectorPanel;
    }

    private void select(ActionEvent evt) {
        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        cancelled = true;
        setVisible(false);
    }

    /**
     * Select a parameter set from the parameter set library with no filtering.
     */
    public static ParameterSet selectParameterSet(Window owner) {
        ParameterSetSelectorDialog dialog = new ParameterSetSelectorDialog(owner);
        dialog.setVisible(true);

        if (dialog.cancelled) {
            return null;
        }
        return dialog.parameterSetSelectorPanel.getSelected();
    }
}
