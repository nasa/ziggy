package gov.nasa.ziggy.data.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import gov.nasa.ziggy.data.management.DataFileTestUtils.DataFileInfoSample1;

/**
 * Class of unit tests for the DataFileInfo class.
 *
 * @author PT
 */
public class DataFileInfoTest {

    @Test
    public void testStringArgConstructor() {
        DataFileInfoSample1 d = new DataFileInfoSample1("pa-123456789-100-results.h5");
        Path p = d.getName();
        assertEquals("pa-123456789-100-results.h5", p.toString());
    }

    @Test
    public void testPathArgConstructor() {
        DataFileInfoSample1 d = new DataFileInfoSample1(Paths.get("pa-123456789-100-results.h5"));
        Path p = d.getName();
        assertEquals("pa-123456789-100-results.h5", p.toString());
    }

    @Test
    public void testPathValid() {
        DataFileInfoSample1 d = new DataFileInfoSample1();
        assertTrue(d.pathValid(Paths.get("pa-123456789-100-results.h5")));
        assertFalse(d.pathValid(Paths.get("some-other-string.h5")));
    }

    @Test
    public void testCompareTo() {
        DataFileInfoSample1 d1 = new DataFileInfoSample1("pa-123456789-100-results.h5");
        DataFileInfoSample1 d2 = new DataFileInfoSample1("pa-123456789-101-results.h5");
        assertTrue(d1.compareTo(d2) < 0);
        assertTrue(d1.compareTo(d1) == 0);
    }
}
