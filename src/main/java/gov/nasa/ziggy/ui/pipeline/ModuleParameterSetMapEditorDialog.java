package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;

/**
 * Dialog box that allows a user to select a module parameter set for editing.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ModuleParameterSetMapEditorDialog extends javax.swing.JDialog
    implements ParameterSetMapEditorListener {

    private ParameterSetMapEditorPanel parameterSetMapEditorPanel;
    private ParameterSetMapEditorListener mapListener;

    public ModuleParameterSetMapEditorDialog(Window owner,
        Map<String, ParameterSet> currentModuleParameters,
        Map<String, ParameterSet> currentPipelineParameters,
        Map<String, ParameterSet> editedParameterSets) {

        super(owner, DEFAULT_MODALITY_TYPE);

        buildComponent(currentModuleParameters, currentPipelineParameters, editedParameterSets);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(Map<String, ParameterSet> currentModuleParameters,
        Map<String, ParameterSet> currentPipelineParameters,
        Map<String, ParameterSet> editedParameterSets) {

        setTitle("Edit parameter sets");

        getContentPane().add(createDataPanel(currentModuleParameters, currentPipelineParameters,
            editedParameterSets), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);

        setMinimumSize(new Dimension(700, 450));
        pack();
    }

    private ParameterSetMapEditorPanel createDataPanel(
        Map<String, ParameterSet> currentModuleParameters,
        Map<String, ParameterSet> currentPipelineParameters,
        Map<String, ParameterSet> editedParameterSets) {

        parameterSetMapEditorPanel = new ParameterSetMapEditorPanel(currentModuleParameters,
            currentPipelineParameters, editedParameterSets);
        parameterSetMapEditorPanel.setMapListener(this);

        return parameterSetMapEditorPanel;
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    public ParameterSetMapEditorListener getMapListener() {
        return mapListener;
    }

    public void setMapListener(ParameterSetMapEditorListener mapListener) {
        this.mapListener = mapListener;
    }

    @Override
    public void notifyMapChanged(Object source) {
        if (mapListener != null) {
            mapListener.notifyMapChanged(this);
        }
    }

    public Map<String, ParameterSet> getModuleParameterSetByName() {
        return parameterSetMapEditorPanel.getModuleParameterSetByName();
    }
}
