package gov.nasa.ziggy.ui.util;

import java.util.regex.Pattern;

import javax.swing.JTextField;
import javax.swing.text.Document;
import javax.swing.text.DocumentFilter;

/**
 * Version of {@link JTextField} that accepts only non-negative real values.
 *
 * @author PT
 */
public class RealTextField extends FilteredTextField {

    private static final long serialVersionUID = 20230511L;

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

    /**
     * Text field filter class that accepts only positive real values.
     *
     * @author PT
     */
    private static class RealFilter extends NumericFilter {

        // Pattern matcher for a non-negative real, based on a design pattern in
        // Stack Overflow:
        // https://stackoverflow.com/a/51994332
        private static final Pattern POSITIVE_REAL_PATTERN = Pattern
            .compile("[+]?(\\d+|\\d+\\.\\d+|\\.\\d+|\\d+\\.)([eE]\\d+)?");
        private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
        private static final Pattern EMPTY_PATTERN = Pattern.compile("");
        private static final Pattern[] PATTERNS = { POSITIVE_REAL_PATTERN, WHITESPACE_PATTERN,
            EMPTY_PATTERN };

        @Override
        protected Pattern[] patterns() {
            return PATTERNS;
        }
    }
}
