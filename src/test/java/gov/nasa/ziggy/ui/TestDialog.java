package gov.nasa.ziggy.ui;

import java.awt.Color;
import java.awt.GridLayout;
import java.text.NumberFormat;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.border.Border;
import javax.swing.border.LineBorder;
import javax.swing.text.NumberFormatter;

import gov.nasa.ziggy.ui.common.ExecuteOnValidityCheck;
import gov.nasa.ziggy.ui.common.ValidityTestingFormattedTextField;

public class TestDialog extends JDialog {

    JButton displayValueButton;
    JButton conditionalButton;
    ValidityTestingFormattedTextField formattedTextBox;
    ValidityTestingFormattedTextField plainTextBox;
    Border invalidBorder = new LineBorder(Color.RED, 2);

    public TestDialog() {
        super((JDialog) null, true);
        initGUI();
    }

    private void initGUI() {
        setSize(300, 200);
        GridLayout gridLayout = new GridLayout(4, 1);
        setLayout(gridLayout);
        getContentPane().add(getFormattedTextBox());
        getContentPane().add(getPlainTextBox());
        getContentPane().add(getDisplayValueButton());
        getContentPane().add(getConditionalButton());
    }

    private ExecuteOnValidityCheck checkFieldsAndEnableButtons = valid -> setConditionalButtonState();

    // floating-point formatted text field with limits between 2 and 4.
    private ValidityTestingFormattedTextField getPlainTextBox() {
        if (plainTextBox == null) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            NumberFormatter formatter = new NumberFormatter(numberFormat);
            formatter.setValueClass(Float.class);
            formatter.setMinimum(2F);
            formatter.setMaximum(4F);
            plainTextBox = new ValidityTestingFormattedTextField(formatter);
            plainTextBox.setEmptyIsValid(true);
            plainTextBox.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);
        }
        return plainTextBox;
    }

    // Formatted text field: only accepts values between 10 and 20;
    // uses PERSIST loss-of-focus logic.
    private ValidityTestingFormattedTextField getFormattedTextBox() {
        if (formattedTextBox == null) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            NumberFormatter formatter = new NumberFormatter(numberFormat);
            formatter.setValueClass(Integer.class);
            formatter.setMinimum(10);
            formatter.setMaximum(20);
            formattedTextBox = new ValidityTestingFormattedTextField(formatter);
            formattedTextBox.setEmptyIsValid(true);
            formattedTextBox.setExecuteOnValidityCheck(checkFieldsAndEnableButtons);

        }
        return formattedTextBox;
    }

    private void setConditionalButtonState() {
        boolean allValid = getPlainTextBox().isValidState() && getFormattedTextBox().isValidState();
        getConditionalButton().setEnabled(allValid);
    }

    // Displays the value of the formatted text field
    private JButton getDisplayValueButton() {
        if (displayValueButton == null) {
            displayValueButton = new JButton();
            displayValueButton.setText("Display Value");
            displayValueButton.addActionListener(e -> {
                Object boxValue = formattedTextBox.getValue();
                if (boxValue == null) {
                    JOptionPane.showMessageDialog(null, "Text field is null");
                } else {
                    JOptionPane.showMessageDialog(null,
                        "Value of formatted text field: " + formattedTextBox.getValue().toString());
                }
            });
        }
        return displayValueButton;
    }

    // This button should only be enabled when the formatted text field has a valid value
    private JButton getConditionalButton() {
        if (conditionalButton == null) {
            conditionalButton = new JButton();
            conditionalButton.setText("Conditional");
            conditionalButton.addActionListener(e -> {
                JOptionPane.showMessageDialog(null, "Button pressed");
            });
            conditionalButton.setEnabled(false);
        }
        return conditionalButton;
    }

    public static void main(String[] args) {
        TestDialog t = new TestDialog();
        t.setVisible(true);
    }
}
