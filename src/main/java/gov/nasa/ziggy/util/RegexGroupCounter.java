package gov.nasa.ziggy.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides a count of the number of groups in a regular expression.
 *
 * @author PT
 */
public class RegexGroupCounter {

    public static final Pattern GROUP_PATTERN = Pattern.compile("\\(([^)]+)\\)");

    public static int groupCount(String regex) {
        Matcher groupMatcher = GROUP_PATTERN.matcher(regex);
        int iGroup = 0;
        while (groupMatcher.find()) {
            iGroup++;
        }
        return iGroup;
    }
}
