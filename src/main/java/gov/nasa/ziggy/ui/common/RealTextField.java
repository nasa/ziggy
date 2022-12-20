package gov.nasa.ziggy.ui.common;

import javax.swing.JTextField;
import javax.swing.text.Document;
import javax.swing.text.DocumentFilter;

/**
 * Version of {@link JTextField} that accepts only non-negative real values.
 *
 * @author PT
 */
public class RealTextField extends FilteredTextField {

    private static final long serialVersionUID = 20210811L;

    public RealTextField() {
    }

    public RealTextField(Document doc, String text, int columns) {
        super(doc, text, columns);
    }

    public RealTextField(int columns) {
        super(columns);
    }

    public RealTextField(String text) {
        super(text);
    }

    public RealTextField(String text, int columns) {
        super(text, columns);
    }

    @Override
    protected DocumentFilter filter() {
        return new RealFilter();
    }

}
