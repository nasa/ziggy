package gov.nasa.ziggy.ui.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.DocumentFilter;

/**
 * Filter that allows a text box to only accept numeric values. Based on a design on Stack Overflow:
 * https://stackoverflow.com/a/11093360/12166780
 * <p>
 * This is an abstract class, so only its concrete subclasses can actually be used. Those classes
 * must override the pattern() method.
 *
 * @author PT
 */
public abstract class NumericFilter extends DocumentFilter {

    protected abstract Pattern[] patterns();

    @Override
    public void insertString(FilterBypass fb, int offset, String string, AttributeSet attrs)
        throws BadLocationException {

        Document doc = fb.getDocument();
        StringBuilder sb = new StringBuilder();
        sb.append(doc.getText(0, doc.getLength()));
        sb.insert(offset, string);

        if (test(sb.toString())) {
            super.insertString(fb, offset, string, attrs);
        }
    }

    /**
     * Checks to see whether the string is consistent with a non-negative integer. The original
     * Stack Overflow method used an Integer.parseInt() wrapped in a try-catch block, but here we
     * try something that doesn't rely on throwing an exception as an ordinary part of business!
     *
     * @param text
     * @return
     */
    private boolean test(String text) {
        Pattern[] patterns = patterns();
        for (Pattern pattern : patterns) {
            Matcher m = pattern.matcher(text);
            boolean matches = m.matches();
            if (matches) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void replace(FilterBypass fb, int offset, int length, String text, AttributeSet attrs)
        throws BadLocationException {

        Document doc = fb.getDocument();
        StringBuilder sb = new StringBuilder();
        sb.append(doc.getText(0, doc.getLength()));
        sb.replace(offset, offset + length, text);

        if (test(sb.toString())) {
            super.replace(fb, offset, length, text, attrs);
        }
    }

    @Override
    public void remove(FilterBypass fb, int offset, int length) throws BadLocationException {

        Document doc = fb.getDocument();
        StringBuilder sb = new StringBuilder();
        sb.append(doc.getText(0, doc.getLength()));
        sb.delete(offset, offset + length);

        if (test(sb.toString())) {
            super.remove(fb, offset, length);
        }
    }
}
