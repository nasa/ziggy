package gov.nasa.ziggy.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * A date formatter that parses and emits ISO 8601 dates in UTC. The formats used in this class
 * include date:yyyy-MM-dd; dateTime: yyyy-MM-ddTHH:mm:ssZ, yyyy-MM-ddTHH:mm:ss.SSSZ. Use this
 * formatter to keep the dates uniform throughout.
 * <p>
 * Usage is via the static methods {@link #dateFormatter()}, {@link #dateTimeFormatter()},
 * {@link #dateTimeMillisFormatter()} and {@link #dateTimeLocalFormatter()}. DO NOT SHARE THE
 * RETURNED INSTANCES BETWEEN THREADS as they are not thread-safe (see {@link DateFormat}).
 * <p>
 * See <a href=
 * "http://nlp.fi.muni.cz/nlp/files/iso8601.txt">http://nlp.fi.muni.cz/nlp/files/iso8601.txt</a>.
 *
 * @author Bill Wohler
 */
public final class Iso8601Formatter {

    private static final ThreadLocal<Map<String, SimpleDateFormat>> dateFormatters = ThreadLocal
        .withInitial(HashMap::new);

    /** The {@link SimpleDateFormat} format string for a date-only formatter. */
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";

    /**
     * The {@link SimpleDateFormat} format string for a combined date/time formatter with a space
     * separator. This is similar to {@link Date#toString()}, but without the milliseconds. The
     * space separator is not permitted in ISO 8601, but it is tolerated in RFC 3339.
     */
    private static final String JAVA_DATE_SANS_MILLIS_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss";

    /**
     * The {@link SimpleDateFormat} format string for a combined date/time formatter.
     */
    private static final String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final String DATE_TIME_MILLIS_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    /**
     * Formatter for ISO 8601 "basic" (no separators) and local time
     */
    private static final String DATE_TIME_LOCAL_NOSEP_STRING = "yyyyMMdd'T'HHmmss";

    /**
     * No instances.
     */
    private Iso8601Formatter() {
    }

    /**
     * Creates a date formatter for the given format and sets its time zone to UTC.
     *
     * @param format the format.
     * @return a date format.
     */
    private static DateFormat createUtcDateFormatter(String format) {
        return createDateFormatter(format, TimeZone.getTimeZone("UTC"));
    }

    /**
     * Creates a date formatter for the given format and sets its time zone to local.
     *
     * @param format the format.
     * @return a date format.
     */
    private static DateFormat createLocalDateFormatter(String format) {
        return createDateFormatter(format, TimeZone.getDefault());
    }

    private static DateFormat createDateFormatter(String format, TimeZone timeZone) {
        Map<String, SimpleDateFormat> formatters = dateFormatters.get();
        if (formatters.containsKey(format)) {
            return formatters.get(format);
        }
        SimpleDateFormat dateFormatter = new SimpleDateFormat(format);
        dateFormatter.setTimeZone(timeZone);
        formatters.put(format, dateFormatter);
        return dateFormatter;
    }

    /**
     * Returns a new instance of a ISO 8601 date-only formatter that displays time in UTC. The
     * returned formatter should only be used in a single thread.
     *
     * @return a date formatter.
     */
    public static DateFormat dateFormatter() {
        return createUtcDateFormatter(DATE_FORMAT_STRING);
    }

    /**
     * Returns a new instance of a ISO 8601 combined date/time formatter that displays time in UTC.
     * The returned formatter should only be used in a single thread.
     *
     * @return a date/time formatter.
     */
    public static DateFormat dateTimeFormatter() {
        return createUtcDateFormatter(DATE_TIME_FORMAT_STRING);
    }

    /**
     * Returns a new instance of a ISO 8601 combined date/time, milliseconds formatter that displays
     * time in UTC. The returned formatter should only be used in a single thread.
     *
     * @return a date/time formatter.
     */
    public static DateFormat dateTimeMillisFormatter() {
        return createUtcDateFormatter(DATE_TIME_MILLIS_FORMAT_STRING);
    }

    /**
     * Returns a new instance of ISO 8601 "basic" formatted date/time (no separators) in the local
     * time zone. The returned formatter should only be used in a single thread.
     *
     * @return a date/time formatter.
     */
    public static DateFormat dateTimeLocalFormatter() {
        return createLocalDateFormatter(DATE_TIME_LOCAL_NOSEP_STRING);
    }

    /**
     * Returns a new instance of a Java Date-like combined date/time formatter that displays time in
     * the local time zone without milliseconds. The returned formatter should only be used in a
     * single thread.
     *
     * @return a date/time formatter.
     */
    public static DateFormat javaDateTimeSansMillisLocalFormatter() {
        return createLocalDateFormatter(JAVA_DATE_SANS_MILLIS_FORMAT_STRING);
    }
}
