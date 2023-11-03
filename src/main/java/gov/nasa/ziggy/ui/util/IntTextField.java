package gov.nasa.ziggy.ui.util;

import java.util.regex.Pattern;

import javax.swing.JTextField;
import javax.swing.text.Document;
import javax.swing.text.DocumentFilter;

/**
 * Version of {@link JTextField} that accepts only non-negative integer values.
 *
 * @author PT
 */
public class IntTextField extends FilteredTextField {

    private static final long serialVersionUID = 20230511L;

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

    /**
     * Text field filter class that accepts only positive integer values.
     *
     * @author PT
     */
    private static class IntFilter extends NumericFilter {

        // Pattern matcher for a non-negative integer -- contains only digits, no
        // decimal point or minus sign needed.
        private static final Pattern POSITIVE_INT_PATTERN = Pattern.compile("\\d*");
        private static final Pattern[] PATTERNS = { POSITIVE_INT_PATTERN };

        @Override
        protected Pattern[] patterns() {
            return PATTERNS;
        }
    }
}
