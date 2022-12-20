package gov.nasa.ziggy.ui.common;

import javax.swing.JTextField;
import javax.swing.text.Document;
import javax.swing.text.DocumentFilter;

/**
 * Version of {@link JTextField} that accepts only non-negative integer values.
 *
 * @author PT
 */
public class IntTextField extends FilteredTextField {

    private static final long serialVersionUID = 20210811L;

    public IntTextField() {
    }

    public IntTextField(Document doc, String text, int columns) {
        super(doc, text, columns);
    }

    public IntTextField(int columns) {
        super(columns);
    }

    public IntTextField(String text) {
        super(text);
    }

    public IntTextField(String text, int columns) {
        super(text, columns);
    }

    @Override
    protected DocumentFilter filter() {
        return new IntFilter();
    }

}
