package gov.nasa.ziggy.report;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ReportValueFormatTest {

    @Test
    public void testFormat() {
        ReportValueFormat reportValueFormat = new ReportValueFormat();
        assertEquals("0", reportValueFormat.format(0));
        assertEquals("10", reportValueFormat.format(10));
        assertEquals("10,000", reportValueFormat.format(10000));
        assertEquals("0.1234", reportValueFormat.format(0.12343));
        assertEquals("1,234.1234", reportValueFormat.format(1234.12343));
        assertEquals("1,234.123", reportValueFormat.format(1234.12300));
    }
}
