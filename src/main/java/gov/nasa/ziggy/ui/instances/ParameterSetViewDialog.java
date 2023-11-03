package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.Map;

import javax.swing.GroupLayout;
import javax.swing.JPanel;

import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ParameterSetViewDialog extends javax.swing.JDialog {

    public ParameterSetViewDialog(Window owner,
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetsMap) {

        super(owner);

        buildComponent(parameterSetsMap);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetsMap) {
        setTitle("View parameter sets");

        getContentPane().add(createDataPanel(parameterSetsMap), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);

        setMinimumSize(ZiggySwingUtils.MIN_DIALOG_SIZE);
        pack();
    }

    private JPanel createDataPanel(
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetsMap) {
        ParameterSetViewPanel parameterSetViewPanel = new ParameterSetViewPanel(parameterSetsMap);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(
            dataPanelLayout.createParallelGroup().addComponent(parameterSetViewPanel));

        dataPanelLayout.setVerticalGroup(
            dataPanelLayout.createSequentialGroup().addComponent(parameterSetViewPanel));

        return dataPanel;
    }

    private void close(ActionEvent evt) {
        setVisible(false);
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new ParameterSetViewDialog(null, null));
    }
}
