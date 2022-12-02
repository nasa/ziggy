package gov.nasa.ziggy.ui.common;

import java.util.regex.Pattern;

/**
 * Text field filter class that accepts only positive integer values.
 *
 * @author PT
 */
public class IntFilter extends NumericFilter {

    // Pattern matcher for a non-negative integer -- contains only digits, no
    // decimal point or minus sign needed.
    static final Pattern POSITIVE_INT_PATTERN = Pattern.compile("\\d*");
    static final Pattern[] PATTERNS = new Pattern[] { POSITIVE_INT_PATTERN };

    @Override
    protected Pattern[] patterns() {
        return PATTERNS;
    }

}
