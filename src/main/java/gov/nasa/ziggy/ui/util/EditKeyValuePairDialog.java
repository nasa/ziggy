package gov.nasa.ziggy.ui.util;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.SAVE;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.KeyValuePair;
import gov.nasa.ziggy.ui.util.proxy.KeyValuePairCrudProxy;

@SuppressWarnings("serial")
public class EditKeyValuePairDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(EditKeyValuePairDialog.class);

    private KeyValuePair keyValuePair;
    private JPanel dataPanel;
    private JPanel buttonPanel;
    private JLabel keyLabel;
    private JTextField valueText;
    private JTextField keyText;
    private JLabel valueLabel;
    private JButton cancelButton;
    private JButton saveButton;

    public EditKeyValuePairDialog(Window owner, KeyValuePair keyValuePair) {
        super(owner, "Edit Key/Value Pair");
        this.keyValuePair = keyValuePair;
        buildComponent();
        setLocationRelativeTo(owner);
    }

    private void buildComponent() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            getContentPane().setLayout(thisLayout);
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getButtonPanel(), BorderLayout.SOUTH);
            setSize(400, 200);
        } catch (Exception e) {
            log.error("buildComponent()", e);
        }
    }

    private JPanel getDataPanel() {

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

        return dataPanel;
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
            saveButton.setText(SAVE);
            saveButton.addActionListener(this::save);
        }

        return saveButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText(CANCEL);
            cancelButton.addActionListener(this::cancel);
        }

        return cancelButton;
    }

    private JLabel getKeyLabel() {
        if (keyLabel == null) {
            keyLabel = new JLabel();
            keyLabel.setText("Key");
        }

        return keyLabel;
    }

    private JLabel getValueLabel() {
        if (valueLabel == null) {
            valueLabel = new JLabel();
            valueLabel.setText("Value");
        }

        return valueLabel;
    }

    private JTextField getKeyText() {
        if (keyText == null) {
            keyText = new JTextField();
            keyText.setEditable(false);
            keyText.setText(keyValuePair.getKey());
        }

        return keyText;
    }

    private JTextField getValueText() {
        if (valueText == null) {
            valueText = new JTextField();
            valueText.setText(keyValuePair.getValue());
        }

        return valueText;
    }

    private void save(ActionEvent evt) {
        try {
            keyValuePair.setValue(valueText.getText());

            KeyValuePairCrudProxy keyValuePairCrud = new KeyValuePairCrudProxy();
            keyValuePairCrud.save(keyValuePair);

            setVisible(false);
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void cancel(ActionEvent evt) {
        setVisible(false);
    }

    public static void main(String[] args) {
        ZiggySwingUtils
            .displayTestDialog(new EditKeyValuePairDialog(null, new KeyValuePair("key", "value")));
    }
}
