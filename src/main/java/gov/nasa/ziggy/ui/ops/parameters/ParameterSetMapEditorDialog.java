package gov.nasa.ziggy.ui.ops.parameters;

import java.awt.BorderLayout;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ParameterSetMapEditorDialog extends javax.swing.JDialog
    implements ParameterSetMapEditorListener {
    private JPanel dataPanel;
    private ParameterSetMapEditorPanel parameterSetMapEditorPanel;
    private JButton closeButton;
    private JPanel buttonPanel;

    private Map<ClassWrapper<Parameters>, ParameterSetName> currentParameters = null;
    private Set<ClassWrapper<Parameters>> requiredParameters;
    private Map<ClassWrapper<Parameters>, ParameterSetName> currentPipelineParameters;

    private ParameterSetMapEditorListener mapListener;

    public ParameterSetMapEditorDialog(JFrame frame) {
        super(frame, true);
        initGUI();
    }

    public ParameterSetMapEditorDialog(JFrame frame,
        Map<ClassWrapper<Parameters>, ParameterSetName> currentParameters,
        Set<ClassWrapper<Parameters>> requiredParameters,
        Map<ClassWrapper<Parameters>, ParameterSetName> currentPipelineParameters) {
        super(frame, true);

        this.currentParameters = currentParameters;
        this.requiredParameters = requiredParameters;
        this.currentPipelineParameters = currentPipelineParameters;

        initGUI();
    }

    public ParameterSetMapEditorDialog(JDialog dialog,
        Map<ClassWrapper<Parameters>, ParameterSetName> currentParameters,
        Set<ClassWrapper<Parameters>> requiredParameters,
        Map<ClassWrapper<Parameters>, ParameterSetName> currentPipelineParameters) {
        super(dialog, true);

        this.currentParameters = currentParameters;
        this.requiredParameters = requiredParameters;
        this.currentPipelineParameters = currentPipelineParameters;

        initGUI();
    }

    private void initGUI() {
        try {
            {
                dataPanel = new JPanel();
                BorderLayout dataPanelLayout = new BorderLayout();
                getContentPane().add(dataPanel, BorderLayout.CENTER);
                dataPanel.setLayout(dataPanelLayout);
                {
                    parameterSetMapEditorPanel = new ParameterSetMapEditorPanel(currentParameters,
                        requiredParameters, currentPipelineParameters);
                    parameterSetMapEditorPanel.setMapListener(this);
                    dataPanel.add(parameterSetMapEditorPanel, BorderLayout.CENTER);
                    parameterSetMapEditorPanel
                        .setBorder(BorderFactory.createTitledBorder("Parameter Sets"));
                }
            }
            {
                buttonPanel = new JPanel();
                getContentPane().add(buttonPanel, BorderLayout.SOUTH);
                {
                    closeButton = new JButton();
                    buttonPanel.add(closeButton);
                    closeButton.setText("Close");
                    closeButton.addActionListener(evt -> closeButtonActionPerformed());
                }
            }
            this.setSize(536, 405);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void closeButtonActionPerformed() {
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

    public Map<ClassWrapper<Parameters>, ParameterSetName> getParameterSetsMap() {
        return parameterSetMapEditorPanel.getParameterSetsMap();
    }
}
