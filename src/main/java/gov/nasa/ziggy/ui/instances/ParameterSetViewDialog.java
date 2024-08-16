package gov.nasa.ziggy.ui.instances;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CLOSE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.Set;

import javax.swing.GroupLayout;
import javax.swing.JPanel;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class ParameterSetViewDialog extends javax.swing.JDialog {

    public ParameterSetViewDialog(Window owner, Set<ParameterSet> parameterSets) {

        super(owner);

        buildComponent(parameterSets);
        setLocationRelativeTo(owner);
    }

    private void buildComponent(Set<ParameterSet> parameterSets) {
        setTitle("View parameter sets");

        getContentPane().add(createDataPanel(parameterSets), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(CLOSE, this::close)),
            BorderLayout.SOUTH);
        setPreferredSize(ZiggyGuiConstants.MIN_DIALOG_SIZE);

        pack();
    }

    private JPanel createDataPanel(Set<ParameterSet> parameterSets) {
        ParameterSetViewPanel parameterSetViewPanel = new ParameterSetViewPanel(parameterSets);

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
        ZiggySwingUtils
            .displayTestDialog(new ParameterSetViewDialog(null, Set.of(new ParameterSet("foo"))));
    }
}
