package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RegexBackslashManagerTest {

    @Test
    public void testToSingleBackslash() {
        assertEquals("foo bar", RegexBackslashManager.toSingleBackslash("foo bar"));
        assertEquals("foo\\bar", RegexBackslashManager.toSingleBackslash("foo\\bar"));
    }

    @Test
    public void testToDoubleBackslash() {
        assertEquals("foo bar", RegexBackslashManager.toDoubleBackslash("foo bar"));
        assertEquals("foo\\\\bar", RegexBackslashManager.toDoubleBackslash("foo\\bar"));
    }
}
