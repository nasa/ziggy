package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RegexGroupCounterTest {

    @Test
    public void testGroupCount() {
        assertEquals(0, RegexGroupCounter.groupCount("foobar"));
        assertEquals(1, RegexGroupCounter.groupCount("(foo)bar"));
        assertEquals(2, RegexGroupCounter.groupCount("(foo)(bar)"));
        // TODO Fix the method so the next test passes
        // assertEquals(3, RegexGroupCounter.groupCount("before ((foo)(bar)) after"));
    }
}
