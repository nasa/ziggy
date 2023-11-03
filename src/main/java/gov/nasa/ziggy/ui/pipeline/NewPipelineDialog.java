package gov.nasa.ziggy.ui.pipeline;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CREATE;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;

import javax.swing.GroupLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class NewPipelineDialog extends javax.swing.JDialog {
    private JTextField nameTextField;

    private boolean cancelled;

    public NewPipelineDialog(Window owner) {
        super(owner, DEFAULT_MODALITY_TYPE);
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        setTitle("New pipeline definition");

        getContentPane().add(createDataPanel(), BorderLayout.CENTER);
        getContentPane().add(createButtonPanel(createButton(CREATE, this::create),
            createButton(CANCEL, this::cancel)), BorderLayout.SOUTH);

        setMinimumSize(ZiggySwingUtils.MIN_DIALOG_SIZE);
        pack();
    }

    private JPanel createDataPanel() {
        JLabel name = boldLabel("Pipeline name");
        nameTextField = new JTextField();

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(
            dataPanelLayout.createParallelGroup().addComponent(name).addComponent(nameTextField));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(name)
            .addComponent(nameTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addPreferredGap(ComponentPlacement.UNRELATED));

        return dataPanel;
    }

    private void create(ActionEvent evt) {
        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        setCancelled(true);
        setVisible(false);
    }

    public String getPipelineName() {
        return nameTextField.getText();
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new NewPipelineDialog(null));
    }
}
