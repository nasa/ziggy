package gov.nasa.ziggy.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;

/**
 * Tests the {@link ZiggyStringUtils} class.
 *
 * @author Bill Wohler
 * @author Forrest Girouard
 */
public class StringUtilsTest {
    @Test(expected = NullPointerException.class)
    public void testConstantToHyphenSeparatedLowercaseNull() {
        ZiggyStringUtils.constantToHyphenSeparatedLowercase(null);
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
        assertEquals(expected, ZiggyStringUtils.trimListWhitespace(value));
    }

    @Test
    public void testConstantToHyphenSeparatedLowercase() {
        assertEquals("", ZiggyStringUtils.constantToHyphenSeparatedLowercase(""));
        assertEquals("foo", ZiggyStringUtils.constantToHyphenSeparatedLowercase("foo"));
        assertEquals("foo-bar", ZiggyStringUtils.constantToHyphenSeparatedLowercase("foo_bar"));
        assertEquals("-foo-bar-", ZiggyStringUtils.constantToHyphenSeparatedLowercase("_foo_bar_"));
        assertEquals("foo", ZiggyStringUtils.constantToHyphenSeparatedLowercase("FOO"));
        assertEquals("foo-bar", ZiggyStringUtils.constantToHyphenSeparatedLowercase("FOO_BAR"));
        assertEquals("-foo-bar-", ZiggyStringUtils.constantToHyphenSeparatedLowercase("_FOO_BAR_"));
    }

    @Test
    public void testToHexString() {
        String s = ZiggyStringUtils.toHexString(new byte[0], 0, 0);
        assertEquals("", s);

        byte[] md5 = { (byte) 0xcd, (byte) 0xe1, (byte) 0xb9, (byte) 0x6c, (byte) 0x1b, (byte) 0x79,
            (byte) 0xfc, (byte) 0x62, (byte) 0x18, (byte) 0x55, (byte) 0x28, (byte) 0x3e,
            (byte) 0xae, (byte) 0x37, (byte) 0x0d, (byte) 0x0c };
        assertEquals(16, md5.length);
        s = ZiggyStringUtils.toHexString(md5, 0, md5.length);
        assertEquals("cde1b96c1b79fc621855283eae370d0c", s);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testToHexStringBadLen() {
        ZiggyStringUtils.toHexString(new byte[2], 0, 3);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testToHexStringBadOff() {
        ZiggyStringUtils.toHexString(new byte[2], 10, 1);
    }

    @Test
    public void testTruncate() {
        assertEquals(null, ZiggyStringUtils.truncate(null, 10));
        assertSame("s", ZiggyStringUtils.truncate("s", 10));
        assertEquals("012345", ZiggyStringUtils.truncate("0123456789", 6));
    }

    @Test
    public void testConvertStringArray() {
        String[] array = ZiggyStringUtils.convertStringArray("a, b, c");
        assertArrayEquals(new String[] { "a", "b", "c" }, array);
    }

    @Test(expected = NullPointerException.class)
    public void testConvertStringArrayWithNullString() {
        ZiggyStringUtils.convertStringArray(null);
    }

    @Test
    public void testConstantToAcronym() {
        String acronym = ZiggyStringUtils.constantToAcronym("FOO_BAR");
        assertEquals("fb", acronym);
    }

    @Test
    public void testConstantToAcronymWithLeadingUnderscore() {
        String acronym = ZiggyStringUtils.constantToAcronym("_FOO_BAR");
        assertEquals("fb", acronym);
    }

    @Test
    public void testConstantToAcronymWithEmptyString() {
        String acronym = ZiggyStringUtils.constantToAcronym("");
        assertEquals("", acronym);
    }

    @Test(expected = NullPointerException.class)
    public void testConstantToAcronymWithNullString() {
        ZiggyStringUtils.constantToAcronym(null);
    }

    @Test
    public void testConstantToCamel() {
        String camel = ZiggyStringUtils.constantToCamel("FOO_BAR");
        assertEquals("fooBar", camel);
    }

    @Test
    public void testConstantToCamelWithLeadingUnderscore() {
        String camel = ZiggyStringUtils.constantToCamel("_FOO_BAR");
        assertEquals("fooBar", camel);
    }

    @Test
    public void testConstantToCamelWithUnderscoreDigit() {
        String camel = ZiggyStringUtils.constantToCamel("FOO_BAR_1_2");
        assertEquals("fooBar-1-2", camel);

        camel = ZiggyStringUtils.constantToCamel("FOO_BAR_1ABC_2");
        assertEquals("fooBar-1abc-2", camel);

        camel = ZiggyStringUtils.constantToCamel("COVARIANCE_MATRIX_1_2");
        assertEquals("covarianceMatrix-1-2", camel);
    }

    @Test
    public void testConstantToCamelWithEmptyString() {
        String camel = ZiggyStringUtils.constantToCamel("");
        assertEquals("", camel);
    }

    @Test(expected = NullPointerException.class)
    public void testConstantToCamelWithNullString() {
        ZiggyStringUtils.constantToCamel(null);
    }

    @Test
    public void testConstantToCamelWithSpaces() {
        String camel = ZiggyStringUtils.constantToCamelWithSpaces("FOO_BAR");
        assertEquals("Foo Bar", camel);
    }

    @Test
    public void testConstantToCamelWithSpacesWithLeadingUnderscore() {
        String camel = ZiggyStringUtils.constantToCamelWithSpaces("_FOO_BAR");
        assertEquals("Foo Bar", camel);
    }

    @Test
    public void testConstantToCamelWithSpacesWithUnderscoreDigit() {
        String camel = ZiggyStringUtils.constantToCamelWithSpaces("FOO_BAR_1_2");
        assertEquals("Foo Bar 1 2", camel);

        camel = ZiggyStringUtils.constantToCamelWithSpaces("FOO_BAR_1ABC_2");
        assertEquals("Foo Bar 1abc 2", camel);
    }

    @Test
    public void testConstantToCamelWithSpacesWithEmptyString() {
        String camel = ZiggyStringUtils.constantToCamelWithSpaces("");
        assertEquals("", camel);
    }

    @Test(expected = NullPointerException.class)
    public void testConstantToCamelWithSpacesWithNullString() {
        ZiggyStringUtils.constantToCamelWithSpaces(null);
    }

    @Test
    public void testConstantToSentenceWithSpacesWithLeadingUnderscore() {
        String sentence = ZiggyStringUtils.constantToSentenceWithSpaces("_FOO_BAR");
        assertEquals("Foo bar", sentence);
    }

    @Test
    public void testConstantToSentenceWithSpacesWithUnderscoreDigit() {
        String sentence = ZiggyStringUtils.constantToSentenceWithSpaces("FOO_BAR_1_2");
        assertEquals("Foo bar 1 2", sentence);

        sentence = ZiggyStringUtils.constantToSentenceWithSpaces("FOO_BAR_1ABC_2");
        assertEquals("Foo bar 1abc 2", sentence);
    }

    @Test
    public void testConstantToSentenceWithSpacesWithEmptyString() {
        String sentence = ZiggyStringUtils.constantToSentenceWithSpaces("");
        assertEquals("", sentence);
    }

    @Test(expected = NullPointerException.class)
    public void testConstantToSentenceWithSpacesWithNullString() {
        ZiggyStringUtils.constantToSentenceWithSpaces(null);
    }

    @Test
    public void testElapsedTime() {
        String elapsedTime = ZiggyStringUtils.elapsedTime(1000, 2000);
        assertEquals("00:00:01", elapsedTime);
    }

    @Test
    public void testElapsedTimeFromStartToCurrent() {
        String elapsedTime = ZiggyStringUtils.elapsedTime(1000, 0);

        // The exact string is unknown, so just check that it is something large.
        assertTrue(elapsedTime.length() > 11);
    }

    @Test
    public void testElapsedTimeWithUninitializedStartTime() {
        String elapsedTime = ZiggyStringUtils.elapsedTime(0, 2000);
        assertEquals("-", elapsedTime);
    }

    @Test
    public void testElapsedTimeWithDates() {
        String elapsedTime = ZiggyStringUtils.elapsedTime(new Date(1000), new Date(2000));
        assertEquals("00:00:01", elapsedTime);
    }

    @Test(expected = NullPointerException.class)
    public void testElapsedTimeWithDatesWithNullStartTime() {
        ZiggyStringUtils.elapsedTime(null, new Date(2000));
    }

    @Test(expected = NullPointerException.class)
    public void testElapsedTimeWithDatesWithNullEndTime() {
        ZiggyStringUtils.elapsedTime(new Date(1000), null);
    }
}
