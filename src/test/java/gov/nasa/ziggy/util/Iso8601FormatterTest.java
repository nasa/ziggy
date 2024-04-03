package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

public class Iso8601FormatterTest {

    private Date date;

    @Before
    public void setUp() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2024, 00, 03, 16, 52, 40); // local time
        calendar.set(Calendar.MILLISECOND, 0);
        date = calendar.getTime();
    }

    @Test
    public void testDateFormatter() {
        assertEquals("2024-01-04", Iso8601Formatter.dateFormatter().format(date));

        // Second call adds coverage for the cached formatter code.
        assertEquals("2024-01-04", Iso8601Formatter.dateFormatter().format(date));
    }

    @Test
    public void testDateTimeFormatter() {
        assertEquals("2024-01-04T00:52:40Z", Iso8601Formatter.dateTimeFormatter().format(date));
    }

    @Test
    public void testDateTimeMillisFormatter() {
        assertEquals("2024-01-04T00:52:40.000Z",
            Iso8601Formatter.dateTimeMillisFormatter().format(date));
    }

    @Test
    public void testDateTimeLocalFormatter() {
        assertEquals("20240103T165240", Iso8601Formatter.dateTimeLocalFormatter().format(date));
    }

    @Test
    public void testJavaDateTimeSansMillisLocalFormatter() {
        assertEquals("2024-01-03 16:52:40",
            Iso8601Formatter.javaDateTimeSansMillisLocalFormatter().format(date));
    }
}
