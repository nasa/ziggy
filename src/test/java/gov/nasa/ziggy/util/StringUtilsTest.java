package gov.nasa.ziggy.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;

/**
 * Tests the {@link StringUtils} class.
 *
 * @author Bill Wohler
 * @author Forrest Girouard
 */
public class StringUtilsTest {
    @Test(expected = NullPointerException.class)
    public void testConstantToHyphenSeparatedLowercaseNull() {
        StringUtils.constantToHyphenSeparatedLowercase(null);
    }

    @Test
    public void testTrimListWhitespace() {

        verifyTrimListWhitespace("", "");
        verifyTrimListWhitespace(" ", "");

        verifyTrimListWhitespace("1", "1");
        verifyTrimListWhitespace(" 1", "1");
        verifyTrimListWhitespace("1 ", "1");
        verifyTrimListWhitespace("  1  ", "1");

        verifyTrimListWhitespace("1,2", "1,2");
        verifyTrimListWhitespace(" 1,2", "1,2");
        verifyTrimListWhitespace("1,2 ", "1,2");
        verifyTrimListWhitespace("1 ,2", "1,2");
        verifyTrimListWhitespace("1, 2", "1,2");
        verifyTrimListWhitespace("  1  ,  2  ", "1,2");
    }

    private void verifyTrimListWhitespace(String value, String expected) {
        assertEquals(expected, StringUtils.trimListWhitespace(value));
    }

    @Test
    public void testConstantToHyphenSeparatedLowercase() {
        assertEquals("", StringUtils.constantToHyphenSeparatedLowercase(""));
        assertEquals("foo", StringUtils.constantToHyphenSeparatedLowercase("foo"));
        assertEquals("foo-bar", StringUtils.constantToHyphenSeparatedLowercase("foo_bar"));
        assertEquals("-foo-bar-", StringUtils.constantToHyphenSeparatedLowercase("_foo_bar_"));
        assertEquals("foo", StringUtils.constantToHyphenSeparatedLowercase("FOO"));
        assertEquals("foo-bar", StringUtils.constantToHyphenSeparatedLowercase("FOO_BAR"));
        assertEquals("-foo-bar-", StringUtils.constantToHyphenSeparatedLowercase("_FOO_BAR_"));
    }

    @Test
    public void testToHexString() {
        String s = StringUtils.toHexString(new byte[0], 0, 0);
        assertEquals("", s);

        byte[] md5 = { (byte) 0xcd, (byte) 0xe1, (byte) 0xb9, (byte) 0x6c, (byte) 0x1b, (byte) 0x79,
            (byte) 0xfc, (byte) 0x62, (byte) 0x18, (byte) 0x55, (byte) 0x28, (byte) 0x3e,
            (byte) 0xae, (byte) 0x37, (byte) 0x0d, (byte) 0x0c };
        assertEquals(16, md5.length);
        s = StringUtils.toHexString(md5, 0, md5.length);
        assertEquals("cde1b96c1b79fc621855283eae370d0c", s);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testToHexStringBadLen() {
        StringUtils.toHexString(new byte[2], 0, 3);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testToHexStringBadOff() {
        StringUtils.toHexString(new byte[2], 10, 1);
    }

    @Test
    public void testTruncate() {
        assertEquals(null, StringUtils.truncate(null, 10));
        assertSame("s", StringUtils.truncate("s", 10));
        assertEquals("012345", StringUtils.truncate("0123456789", 6));
    }

    @Test
    public void testConvertStringArray() {
        String[] array = StringUtils.convertStringArray("a, b, c");
        assertArrayEquals(new String[] { "a", "b", "c" }, array);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertStringArrayWithNullString() {
        StringUtils.convertStringArray(null);
    }

    @Test
    public void testConstantToAcronym() {
        String acronym = StringUtils.constantToAcronym("FOO_BAR");
        assertEquals("fb", acronym);
    }

    @Test
    public void testConstantToAcronymWithLeadingUnderscore() {
        String acronym = StringUtils.constantToAcronym("_FOO_BAR");
        assertEquals("fb", acronym);
    }

    @Test
    public void testConstantToAcronymWithEmptyString() {
        String acronym = StringUtils.constantToAcronym("");
        assertEquals("", acronym);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstantToAcronymWithNullString() {
        StringUtils.constantToAcronym(null);
    }

    @Test
    public void testConstantToCamel() {
        String camel = StringUtils.constantToCamel("FOO_BAR");
        assertEquals("fooBar", camel);
    }

    @Test
    public void testConstantToCamelWithLeadingUnderscore() {
        String camel = StringUtils.constantToCamel("_FOO_BAR");
        assertEquals("fooBar", camel);
    }

    @Test
    public void testConstantToCamelWithUnderscoreDigit() {
        String camel = StringUtils.constantToCamel("FOO_BAR_1_2");
        assertEquals("fooBar-1-2", camel);

        camel = StringUtils.constantToCamel("FOO_BAR_1ABC_2");
        assertEquals("fooBar-1abc-2", camel);

        camel = StringUtils.constantToCamel("COVARIANCE_MATRIX_1_2");
        assertEquals("covarianceMatrix-1-2", camel);
    }

    @Test
    public void testConstantToCamelWithEmptyString() {
        String camel = StringUtils.constantToCamel("");
        assertEquals("", camel);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstantToCamelWithNullString() {
        StringUtils.constantToCamel(null);
    }

    @Test
    public void testConstantToCamelWithSpaces() {
        String camel = StringUtils.constantToCamelWithSpaces("FOO_BAR");
        assertEquals("Foo Bar", camel);
    }

    @Test
    public void testConstantToCamelWithSpacesWithLeadingUnderscore() {
        String camel = StringUtils.constantToCamelWithSpaces("_FOO_BAR");
        assertEquals("Foo Bar", camel);
    }

    @Test
    public void testConstantToCamelWithSpacesWithUnderscoreDigit() {
        String camel = StringUtils.constantToCamelWithSpaces("FOO_BAR_1_2");
        assertEquals("Foo Bar 1 2", camel);

        camel = StringUtils.constantToCamelWithSpaces("FOO_BAR_1ABC_2");
        assertEquals("Foo Bar 1abc 2", camel);
    }

    @Test
    public void testConstantToCamelWithSpacesWithEmptyString() {
        String camel = StringUtils.constantToCamelWithSpaces("");
        assertEquals("", camel);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstantToCamelWithSpacesWithNullString() {
        StringUtils.constantToCamelWithSpaces(null);
    }

    @Test
    public void testElapsedTime() {
        String elapsedTime = StringUtils.elapsedTime(1000, 2000);
        assertEquals("00:00:01", elapsedTime);
    }

    @Test
    public void testElapsedTimeFromStartToCurrent() {
        String elapsedTime = StringUtils.elapsedTime(1000, 0);

        // The exact string is unknown, so just check that it is something large.
        assertTrue(elapsedTime.length() > 11);
    }

    @Test
    public void testElapsedTimeWithUninitializedStartTime() {
        String elapsedTime = StringUtils.elapsedTime(0, 2000);
        assertEquals("-", elapsedTime);
    }

    @Test
    public void testElapsedTimeWithDates() {
        String elapsedTime = StringUtils.elapsedTime(new Date(1000), new Date(2000));
        assertEquals("00:00:01", elapsedTime);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testElapsedTimeWithDatesWithNullStartTime() {
        StringUtils.elapsedTime(null, new Date(2000));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testElapsedTimeWithDatesWithNullEndTime() {
        StringUtils.elapsedTime(new Date(1000), null);
    }
}
