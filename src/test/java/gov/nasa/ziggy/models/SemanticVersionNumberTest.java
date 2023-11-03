package gov.nasa.ziggy.models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test class for SemanticVersionNumber.
 *
 * @author PT
 */
public class SemanticVersionNumberTest {

    @Test
    public void testConstructor() {

        SemanticVersionNumber s = new SemanticVersionNumber("10.5.2");
        assertEquals(10, s.getMajor());
        assertEquals(5, s.getMinor());
        assertEquals(2, s.getPatch());

        s = new SemanticVersionNumber(13, 6, 4);
        assertEquals(13, s.getMajor());
        assertEquals(6, s.getMinor());
        assertEquals(4, s.getPatch());
    }

    @Test
    public void testToString() {
        SemanticVersionNumber s = new SemanticVersionNumber(13, 6, 4);
        assertEquals("13.6.4", s.toString());
    }

    @Test
    public void testCompareTo() {

        int c = new SemanticVersionNumber(10, 3, 2).compareTo(new SemanticVersionNumber(11, 5, 4));
        assertEquals(-1, c);
        c = new SemanticVersionNumber(11, 5, 4).compareTo(new SemanticVersionNumber(10, 3, 2));
        assertEquals(1, c);
        SemanticVersionNumber x = new SemanticVersionNumber(10, 3, 2);
        SemanticVersionNumber y = new SemanticVersionNumber(9, 5, 4);
        SemanticVersionNumber z = new SemanticVersionNumber(9, 4, 10);
        assertTrue(x.compareTo(y) > 0);
        assertTrue(y.compareTo(z) > 0);
        assertTrue(x.compareTo(z) > 0);
    }
}
