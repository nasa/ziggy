package gov.nasa.ziggy.ui.util;

import java.awt.Color;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.text.Format;
import java.text.ParseException;
import java.util.function.Consumer;

import javax.swing.JFormattedTextField;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.border.LineBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.apache.commons.lang3.StringUtils;

/**
 * Subclass of {@link JFormattedTextField} which provides the following additional functionality:
 * <ol>
 * <li>Instances check on every keystroke whether they remain valid (i.e., whether the contents of
 * the text field can be parsed by the instance's formatter).
 * <li>The value is updated on every keystroke, provided that the current text field is valid.
 * <li>A public method, {@link isValid}, allows the user to determine at any time whether any
 * instance is currently valid.
 * <li>For invalid instances, a red border appears inside the text field when the instance loses
 * focus, and disappears when the instance regains the focus.
 * <li>Each instance can be provided with an instance of the {@link ExecuteOnValidityCheck}
 * functional interface, which can perform actions as part of the validity check depending on
 * whether the check indicates that the instance is currently valid or invalid.
 * <li>Disabled instances are automatically cleared of any values and are treated as valid.
 * </ol>
 * The user is also able to select whether an empty text box constitutes a valid or invalid state.
 *
 * @author PT
 */
public class ValidityTestingFormattedTextField extends JFormattedTextField {

    private static final long serialVersionUID = 20230511L;

    public static final Border INVALID_BORDER = new LineBorder(Color.RED, 2);

    private boolean validState;
    private Consumer<Boolean> executeOnValidityCheck;
    private boolean emptyIsValid;

    public ValidityTestingFormattedTextField() {
        buildComponent();
    }

    public ValidityTestingFormattedTextField(Format format) {
        super(format);
        buildComponent();
    }

    public ValidityTestingFormattedTextField(JFormattedTextField.AbstractFormatter formatter) {
        super(formatter);
        buildComponent();
    }

    public ValidityTestingFormattedTextField(JFormattedTextField.AbstractFormatterFactory factory) {
        super(factory);
        buildComponent();
    }

    public ValidityTestingFormattedTextField(JFormattedTextField.AbstractFormatterFactory factory,
        Object currentValue) {
        super(factory, currentValue);
        buildComponent();
    }

    public ValidityTestingFormattedTextField(Object value) {
        super(value);
        buildComponent();
    }

    private void buildComponent() {
        setFocusLostBehavior(JFormattedTextField.PERSIST);
        validState = false;
        getDocument().addDocumentListener(new DocumentListener() {

            @Override
            public void insertUpdate(DocumentEvent e) {
                checkForValidState();
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                checkForValidState();
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                checkForValidState();
            }
        });
        addFocusListener(new FocusListener() {

            @Override
            public void focusGained(FocusEvent e) {
                setBorder(UIManager.getLookAndFeel().getDefaults().getBorder("TextField.border"));
            }

            @Override
            public void focusLost(FocusEvent e) {
                if (!isEnabled()) {
                    validState = true;
                    setValue(null);
                }
                if (!isValidState()) {
                    setBorder(INVALID_BORDER);
                }
            }
        });
    }

    private void checkForValidState() {
        try {
            commitEdit();
            validState = true;
            if (StringUtils.isEmpty(getText()) && !emptyIsValid && isEnabled()) {
                validState = false;
            }
        } catch (ParseException e) {
            if (StringUtils.isEmpty(getText()) && emptyIsValid || !isEnabled()) {
                validState = true;
                setValue(null);
            } else {
                validState = false;
            }
        } finally {
            if (validState) {
                setBorder(UIManager.getLookAndFeel().getDefaults().getBorder("TextField.border"));
            }
            if (executeOnValidityCheck != null) {
                executeOnValidityCheck.accept(validState);
            }
        }
    }

    public boolean isValidState() {
        return validState;
    }

    public void setExecuteOnValidityCheck(Consumer<Boolean> executeOnValidityCheck) {
        this.executeOnValidityCheck = executeOnValidityCheck;
    }

    public void setEmptyIsValid(boolean emptyIsValid) {
        this.emptyIsValid = emptyIsValid;
        if (emptyIsValid && getValue() == null) {
            validState = true;
        }
        if (!emptyIsValid && getValue() == null) {
            validState = false;
        }
        if (executeOnValidityCheck != null) {
            executeOnValidityCheck.accept(validState);
        }
    }
}
