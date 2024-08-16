package gov.nasa.ziggy.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utility functions for {@link String}s.
 *
 * @author Forrest Girouard
 * @author Sean McCauliff
 * @author Todd Klaus
 * @author Thomas Han
 */
public class ZiggyStringUtils {
    private static final Logger log = LoggerFactory.getLogger(ZiggyStringUtils.class);

    public static final String EMPTY = org.apache.commons.lang3.StringUtils.EMPTY;

    /**
     * A pattern that represents part of an enumerated constant name that either has no underscore
     * or has one leading underscore. For example, if <code>ONE_TWO</code> is an enumeration
     * constant, it has two parts, "ONE" and "_TWO". This pattern is used to convert an enumeration
     * constant name to camel-case, part by part.
     */
    private static final Pattern CONSTANT_PART = Pattern.compile("_?([A-Za-z0-9])([A-Za-z0-9]*)");

    /**
     * A pattern that matches positive and negative numbers with optional leading or trailing space.
     * There is one capturing group for the number.
     */
    public static final Pattern NUMERIC = Pattern.compile("^\\s*(-?[0-9]+)\\s*$");

    /**
     * A pattern that matches numeric ranges defined by two numbers separated by a dash and optional
     * spaces. For example, 1-3, or even -1 - -2 will match. There are two capturing groups for each
     * of the two numbers that define the range.
     */
    public static final Pattern NUMERIC_RANGE = Pattern
        .compile("^\\s*(-?[0-9]+)\\s*-\\s*(-?[0-9]+)\\s*$");

    /**
     * Convert a string to array of String
     *
     * @param input
     * @return
     */
    public static String[] convertStringArray(String input) {
        checkNotNull(input, "input");

        log.debug("convertStringArray got " + input);
        StringTokenizer st = new StringTokenizer(input, ",");
        String[] results = new String[st.countTokens()];
        int i = 0;
        while (st.hasMoreTokens()) {
            results[i++] = st.nextToken().trim();
        }
        return results;
    }

    /**
     * Translate a constant field name to an acronym (for example, FOO_BAR -&#62; fb).
     *
     * @param value the name of the constant field (uppercase and underscores)
     * @return representation of input value in camel case
     */
    public static String constantToAcronym(String value) {
        checkNotNull(value, "value");

        StringBuilder builder = new StringBuilder();
        int index = 0;
        while (index < value.length()) {
            int underscore = value.indexOf('_', index);
            if (underscore != 0) {
                builder.append(value.substring(index, index + 1).toLowerCase());
            }
            if (underscore == -1) {
                index = value.length();
            } else {
                index = underscore + 1;
            }
        }
        return builder.length() > 0 ? builder.toString() : value;
    }

    /**
     * Translate a constant field name to camel case.
     *
     * @param value the name of the constant field (uppercase and underscores)
     * @return representation of input value in camel case
     */
    public static String constantToCamel(String value) {
        checkNotNull(value, "value");

        Matcher matcher = CONSTANT_PART.matcher(value);
        StringBuffer buf = new StringBuffer();
        while (matcher.find()) {
            if (buf.length() == 0) {
                // First part is all lower case.
                matcher.appendReplacement(buf,
                    matcher.group(1).toLowerCase() + matcher.group(2).toLowerCase());
            } else if (Character.isDigit(matcher.group(1).charAt(0))) {
                // Underscore followed by a digit is all lower case preceded by hyphen.
                matcher.appendReplacement(buf,
                    "-" + matcher.group(1) + matcher.group(2).toLowerCase());
            } else {
                // All parts but the first are capitalized.
                matcher.appendReplacement(buf,
                    matcher.group(1).toUpperCase() + matcher.group(2).toLowerCase());
            }
        }
        matcher.appendTail(buf);

        return buf.toString();
    }

    /**
     * Translate a constant field name to camel case with spaces.
     *
     * @param value the name of the constant field (uppercase and underscores)
     * @return representation of input value in camel case with spaces.
     */
    public static String constantToCamelWithSpaces(String value) {
        checkNotNull(value, "value");

        Matcher matcher = CONSTANT_PART.matcher(value);
        StringBuffer buf = new StringBuffer();
        while (matcher.find()) {
            if (buf.length() > 0) {
                // All but the first are preceded by a space.
                matcher.appendReplacement(buf,
                    " " + matcher.group(1).toUpperCase() + matcher.group(2).toLowerCase());
            } else {
                // The first has no preceding space, but is otherwise the same.
                matcher.appendReplacement(buf,
                    matcher.group(1).toUpperCase() + matcher.group(2).toLowerCase());
            }
        }
        matcher.appendTail(buf);

        return buf.toString();
    }

    /**
     * Translate a constant field name to sentence case with spaces. For example, FOO_BAR becomes
     * Foo bar.
     *
     * @param value the name of the constant field (uppercase and underscores)
     * @return representation of input value in sentence case with spaces.
     */
    public static String constantToSentenceWithSpaces(String value) {
        checkNotNull(value, "value");

        Matcher matcher = CONSTANT_PART.matcher(value);
        StringBuffer buf = new StringBuffer();
        while (matcher.find()) {
            if (buf.length() > 0) {
                // All but the first are preceded by a space.
                matcher.appendReplacement(buf,
                    " " + matcher.group(1).toLowerCase() + matcher.group(2).toLowerCase());
            } else {
                // The first has no preceding space, but is otherwise the same.
                matcher.appendReplacement(buf,
                    matcher.group(1).toUpperCase() + matcher.group(2).toLowerCase());
            }
        }
        matcher.appendTail(buf);

        return buf.toString();
    }

    /**
     * Translate a constant field name to a lowercase, hyphen-separated string (for example, FOO_BAR
     * -&#62; foo-bar). This is useful when converting enums values to command-line commands.
     *
     * @param value the name of the constant field (uppercase and underscores)
     * @return hyphen-separated, lowercase string
     */
    public static String constantToHyphenSeparatedLowercase(String value) {
        String s = value.replace('_', '-');

        return s.toLowerCase();
    }

    /**
     * Return an elapsed time in display form. If endTime &#60;= startTime, endTime is assumed to be
     * uninitialized and elapsed time is computed from startTime to current time.
     *
     * @param startTime
     * @param endTime
     * @return
     */
    public static String elapsedTime(long startTime, long endTime) {
        long current = System.currentTimeMillis();
        long duration;

        if (startTime == 0) {
            return "-";
        }

        if (endTime > startTime) {
            // completed
            duration = endTime - startTime;
        } else {
            // still going
            duration = current - startTime;
        }

        return DurationFormatUtils.formatDuration(duration, "HH:mm:ss");
    }

    public static String elapsedTime(Date startTime, Date endTime) {
        checkNotNull(startTime, "startTime");
        checkNotNull(endTime, "startTime");

        return elapsedTime(startTime.getTime(), endTime.getTime());
    }

    /**
     * Returns a list of Longs for the given string, which contains a comma-separated list of
     * numbers and ranges. For example, for the input "1, 3-5, 7", the list [ 1L, 3L, 4L, 5L, 7L ]
     * is returned. Ranges can only contain increasing numbers. For example 3-1 won't add any values
     * to the list. However, 2-2 will add 2 to the list.
     *
     * @param s the input string
     * @return a non-null list of numbers, which is empty if the string is null or empty
     * @throws NumberFormatException if an element between the commas isn't a long or a range
     */

    public static List<Long> extractNumericRange(String s) {
        if (StringUtils.isBlank(s)) {
            return new ArrayList<>();
        }

        List<Long> numbers = new ArrayList<>();
        for (String element : s.split(",")) {
            Matcher matcher = NUMERIC_RANGE.matcher(element);
            if (matcher.matches()) {
                numbers.addAll(LongStream
                    .rangeClosed(Long.parseLong(matcher.group(1).trim()),
                        Long.parseLong(matcher.group(2).trim()))
                    .boxed()
                    .collect(Collectors.toList()));
            } else if (NUMERIC.matcher(element).matches()) {
                numbers.add(Long.valueOf(element.trim()));
            } else {
                throw new NumberFormatException("Invalid element " + element);
            }
        }
        return numbers;
    }

    public static String pad(String s, int desiredLength) {
        StringBuilder sb = new StringBuilder(s);
        int length = s.length();
        while (length < desiredLength) {
            sb.append(" ");
            length++;
        }
        return sb.toString();
    }

    /**
     * Converts the buffer to a string in hex with the correct padding that is a byte with a value
     * of 12 is converted to the string "0c" not "c".
     *
     * @param buf A byte buffer. Must not be null.
     * @param off The offset into the byte array to start conversion.
     * @param len The number of bytes to convert.
     * @return A string of length zero of more.
     */
    public static String toHexString(byte[] buf, int off, int len) {
        checkNotNull(buf, "buf");
        checkArgument(off >= 0, "off must be non-negative");
        checkArgument(len >= 0, "len must be non-negative");

        final int nbytes = buf.length - off;
        if (off > buf.length - 1 && off == buf.length && nbytes != 0) {
            throw new IllegalArgumentException("Offset is larger than array.");
        }

        if (len > nbytes) {
            throw new IllegalArgumentException("Len is too long.");
        }

        StringBuilder bldr = new StringBuilder(nbytes * 2);
        for (int i = off; i < off + len; i++) {
            // Could make this faster with lookup table.
            bldr.append(String.format("%02x", buf[i]));
        }
        return bldr.toString();
    }

    /**
     * Truncates a string.
     *
     * @param s A string. This may be null.
     * @param len The new length. If len greater or equal to s.length() then this will return s.
     * Otherwise the string will contain the characters in indices [0,len).
     * @return If s null then the return value will be null.
     */
    public static String truncate(String s, int len) {
        checkArgument(len >= 0, "len must be non-negative");

        if (s == null) {
            return null;
        }
        if (s.length() <= len) {
            return s;
        }
        return s.substring(0, len);
    }

    /**
     * Break a string into an 80 characters per line.
     *
     * @param s Assumes input string is not null and does not already contain line breaks.
     */
    public static String breakAt80Characters(String s) {
        StringBuilder bldr = new StringBuilder(s.length() + s.length() / 80 + 1);
        for (int i = 0; i < s.length(); i += 80) {
            bldr.append(s.substring(i, Math.min(i + 80, s.length())));
            bldr.append('\n');
        }
        return bldr.toString();
    }

    /**
     * <p>
     * Removes leading and trailing whitespace, and whitespace around list element delimiters,
     * within a list of values. Must allow whitespace around the list delimiters.
     * </p>
     * <p>
     * Default scope for unit testing.
     * </p>
     *
     * @param value the list value
     * @return the trimmed list value
     */
    public static String trimListWhitespace(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().replaceAll("\\s*,\\s*", ",");
    }

    /**
     * Searches a list of Strings for any occurrence of one or more target Strings. All of the
     * Strings that contain a target String are returned. If the target Strings are empty or null,
     * the original list of Strings is returned.
     *
     * @param strings List of Strings to search for targets.
     * @param targets Array of strings to search for.
     * @return Strings from the original list that contain any of the strings in the target array.
     */
    public static List<String> stringsContainingTargets(List<String> strings, String... targets) {

        List<String> stringsWithTargets = new ArrayList<>();
        if (targets == null || targets.length == 0) {
            return strings;
        }
        for (String string : strings) {
            for (String target : targets) {
                if (string.contains(target)) {
                    stringsWithTargets.add(string);
                    break;
                }
            }
        }
        return stringsWithTargets;
    }

    public static List<String> splitStringAtCommas(String parentString) {
        return Arrays.asList(parentString.trim().split("\\s*,\\s*"));
    }

    /**
     * Takes a {@link String} that contains line terminations and breaks it at those terminations,
     * returning the resulting {link String}s as a {@link List}.
     */
    public static List<String> splitStringAtLineTerminations(String string) {
        String[] splitString = string.split(System.lineSeparator());
        return Arrays.asList(splitString);
    }

    /**
     * Checks a {@link Collection} of {@link String}s for duplicates, and returns any that are
     * found.
     */
    public static List<String> duplicateStrings(Collection<String> strings) {
        Set<String> uniqueStrings = new HashSet<>();
        return strings.stream()
            .filter(s -> !uniqueStrings.add(s))
            .distinct()
            .collect(Collectors.toList());
    }
}
