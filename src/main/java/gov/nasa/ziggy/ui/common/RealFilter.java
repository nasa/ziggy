package gov.nasa.ziggy.ui.common;

import java.util.regex.Pattern;

/**
 * Text field filter class that accepts only positive real values.
 *
 * @author PT
 */
public class RealFilter extends NumericFilter {

    // Pattern matcher for a non-negative real, based on a design pattern in
    // Stack Overflow:
    // https://stackoverflow.com/a/51994332
    static final Pattern POSITIVE_REAL_PATTERN = Pattern
        .compile("[+]?(\\d+|\\d+\\.\\d+|\\.\\d+|\\d+\\.)([eE]\\d+)?");
    static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
    static final Pattern EMPTY_PATTERN = Pattern.compile("");
    static final Pattern[] PATTERNS = new Pattern[] { POSITIVE_REAL_PATTERN, WHITESPACE_PATTERN,
        EMPTY_PATTERN };

    @Override
    protected Pattern[] patterns() {
        return PATTERNS;
    }

}
