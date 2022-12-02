package gov.nasa.ziggy.ui.config.general;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.proxy.KeyValuePairCrudProxy;

@SuppressWarnings("serial")
public class KeyValuePairEditDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(KeyValuePairEditDialog.class);

    private KeyValuePair keyValuePair;
    private JPanel dataPanel;
    private JPanel buttonPanel;
    private JLabel keyLabel;
    private JTextField valueText;
    private JTextField keyText;
    private JLabel valueLabel;
    private JButton cancelButton;
    private JButton saveButton;

    /**
     * Auto-generated main method to display this JDialog
     */
    public static void main(String[] args) {
        log.debug("main(String[]) - start");

        JFrame frame = new JFrame();
        KeyValuePairEditDialog inst = new KeyValuePairEditDialog(frame);
        inst.setVisible(true);

        log.debug("main(String[]) - end");
    }

    public KeyValuePairEditDialog(JFrame frame) {
        super(frame, "New Key/Value Pair", true);
        initGUI();
    }

    public KeyValuePairEditDialog(JFrame parent, KeyValuePair keyValuePair) {
        super(parent, "Edit Key/Value Pair");
        this.keyValuePair = keyValuePair;
        initGUI();
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        try {
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            setSize(400, 200);
        } catch (Exception e) {
            log.error("initGUI()", e);
        }

        log.debug("initGUI() - end");
    }

    private JPanel getDataPanel() {
        log.debug("getDataPanel() - start");

        if (dataPanel == null) {
            dataPanel = new JPanel();
            GridBagLayout dataPanelLayout = new GridBagLayout();
            dataPanelLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            dataPanelLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7 };
            dataPanelLayout.rowWeights = new double[] { 0.1, 0.1 };
            dataPanelLayout.rowHeights = new int[] { 7, 7 };
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getKeyLabel(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getValueLabel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getKeyText(),
                new GridBagConstraints(1, 0, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            dataPanel.add(getValueText(),
                new GridBagConstraints(1, 1, 4, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
        }

        log.debug("getDataPanel() - end");
        return dataPanel;
    }

    private JPanel getButtonPanel() {
        log.debug("getButtonPanel() - start");

        if (buttonPanel == null) {
            buttonPanel = new JPanel();
            FlowLayout buttonPanelLayout = new FlowLayout();
            buttonPanelLayout.setHgap(40);
            buttonPanel.setLayout(buttonPanelLayout);
            buttonPanel.add(getSaveButton());
            buttonPanel.add(getCancelButton());
        }

        log.debug("getButtonPanel() - end");
        return buttonPanel;
    }

    private JButton getSaveButton() {
        log.debug("getSaveButton() - start");

        if (saveButton == null) {
            saveButton = new JButton();
            saveButton.setText("Save Changes");
            saveButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                saveButtonActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getSaveButton() - end");
        return saveButton;
    }

    private JButton getCancelButton() {
        log.debug("getCancelButton() - start");

        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("Cancel");
            cancelButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                cancelButtonActionPerformed(evt);

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getCancelButton() - end");
        return cancelButton;
    }

    private JLabel getKeyLabel() {
        log.debug("getKeyLabel() - start");

        if (keyLabel == null) {
            keyLabel = new JLabel();
            keyLabel.setText("Key");
        }

        log.debug("getKeyLabel() - end");
        return keyLabel;
    }

    private JLabel getValueLabel() {
        log.debug("getValueLabel() - start");

        if (valueLabel == null) {
            valueLabel = new JLabel();
            valueLabel.setText("Value");
        }

        log.debug("getValueLabel() - end");
        return valueLabel;
    }

    private JTextField getKeyText() {
        log.debug("getKeyText() - start");

        if (keyText == null) {
            keyText = new JTextField();
            keyText.setEditable(false);
            keyText.setText(keyValuePair.getKey());
        }

        log.debug("getKeyText() - end");
        return keyText;
    }

    private JTextField getValueText() {
        log.debug("getValueText() - start");

        if (valueText == null) {
            valueText = new JTextField();
            valueText.setText(keyValuePair.getValue());
        }

        log.debug("getValueText() - end");
        return valueText;
    }

    private void saveButtonActionPerformed(ActionEvent evt) {
        log.debug("saveButtonActionPerformed(ActionEvent) - start");

        log.debug(
            "saveButtonActionPerformed(ActionEvent) - saveButton.actionPerformed, event=" + evt);

        try {
            keyValuePair.setValue(valueText.getText());

            KeyValuePairCrudProxy keyValuePairCrud = new KeyValuePairCrudProxy();
            keyValuePairCrud.save(keyValuePair);

            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }

        log.debug("saveButtonActionPerformed(ActionEvent) - end");
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButtonActionPerformed(ActionEvent) - start");

        log.debug("cancelButtonActionPerformed(ActionEvent) - cancelButton.actionPerformed, event="
            + evt);
        setVisible(false);

        log.debug("cancelButtonActionPerformed(ActionEvent) - end");
    }
}
