package gov.nasa.ziggy.ui.util;

import java.awt.Color;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.text.Format;
import java.text.ParseException;
import java.util.function.Consumer;

import javax.swing.JFormattedTextField;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.border.LineBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass of {@link JFormattedTextField} which provides the following additional functionality:
 * <ol>
 * <li>Instances check on every keystroke whether they remain valid (i.e., whether the contents of
 * the text field can be parsed by the instance's formatter).
 * <li>The value is updated on every keystroke, provided that the current text field is valid;
 * otherwise, the value is undefined. The method {@link #getText()} will always return the content
 * of the field.
 * <li>A public method, {@link ValidityTestingFormattedTextField#isValidState()}, allows the user to
 * determine at any time whether any instance is currently valid.
 * <li>For invalid instances, a red border appears inside the text field.
 * <li>Each instance can be provided with an instance of the {@link Consumer}{@code <Boolean>}
 * functional interface, which can perform actions as part of the validity check depending on
 * whether the check indicates that the instance is currently valid or invalid.
 * <li>Disabled instances are automatically cleared of any values and are treated as valid.
 * </ol>
 * Use {@link #setEmptyIsValid(boolean)} to choose whether an empty text box constitutes a valid or
 * invalid state.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ValidityTestingFormattedTextField extends JFormattedTextField
    implements DocumentListener {

    private static final long serialVersionUID = 20240111L;
    private static final Logger log = LoggerFactory
        .getLogger(ValidityTestingFormattedTextField.class);

    private static final Border INVALID_BORDER = new LineBorder(Color.RED, 2);

    private boolean validState;
    private Consumer<Boolean> executeOnValidityCheck;
    private boolean emptyIsValid;
    private boolean priorValidState;
    private String priorText;

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
        addFocusListener(new FocusAdapter() {

            @Override
            public void focusGained(FocusEvent evt) {
                // The selection needs to be called from invokeLater to take effect.
                // The selection triggers remoteUpdate() and insertUpdate() calls and possibly an
                // avalanche of error dialogs if one of the fields are invalid. Therefore, wait
                // until the selection is done before adding the document listener.
                SwingUtilities.invokeLater(() -> {
                    selectAll();
                    getDocument().addDocumentListener(ValidityTestingFormattedTextField.this);
                });
            }

            @Override
            public void focusLost(FocusEvent evt) {
                getDocument().removeDocumentListener(ValidityTestingFormattedTextField.this);
            }
        });
    }

    @Override
    public void insertUpdate(DocumentEvent evt) {
        checkForValidState();
    }

    @Override
    public void removeUpdate(DocumentEvent evt) {
        checkForValidState();
    }

    @Override
    public void changedUpdate(DocumentEvent evt) {
        checkForValidState();
    }

    @Override
    public void setEnabled(boolean enable) {
        super.setEnabled(enable);
        checkForValidState();
    }

    @Override
    public void setText(String text) {
        super.setText(text);
        checkForValidState();
    }

    @Override
    public void setValue(Object value) {
        super.setValue(value);
        checkForValidState();
    }

    private void checkForValidState() {
        try {
            validState = true;
            commitEdit();
        } catch (ParseException e) {
            validState = StringUtils.isBlank(getText()) && emptyIsValid || !isEnabled();
        } finally {
            updateBorder(validState);

            // Skip check if neither field nor state has changed.
            log.debug("priorText={}, text={}, value={}, priorValidState={}, validState={}",
                priorText, getText(), getValue(), priorValidState, validState);
            if (executeOnValidityCheck != null
                && (!getText().equals(priorText) || validState != priorValidState)) {
                executeOnValidityCheck.accept(validState);
            }

            priorText = getText();
            priorValidState = validState;
        }
    }

    public void updateBorder(boolean validState) {
        setBorder(
            validState ? UIManager.getLookAndFeel().getDefaults().getBorder("TextField.border")
                : INVALID_BORDER);
    }

    public boolean isValidState() {
        return validState;
    }

    /**
     * Sets the function to be called when the field is updated. The parameter is true if the field
     * is valid; otherwise, it is false.
     */
    public void setExecuteOnValidityCheck(Consumer<Boolean> executeOnValidityCheck) {
        this.executeOnValidityCheck = executeOnValidityCheck;
    }

    /**
     * Sets whether an empty field is valid. The default is false. This method access the content
     * and enabled state of the field, so call this method after those operations.
     */
    public void setEmptyIsValid(boolean emptyIsValid) {
        this.emptyIsValid = emptyIsValid;
        if (StringUtils.isBlank(getText()) && isEnabled()) {
            validState = emptyIsValid;
        }
        updateBorder(validState);
        if (executeOnValidityCheck != null) {
            executeOnValidityCheck.accept(validState);
        }
    }
}
