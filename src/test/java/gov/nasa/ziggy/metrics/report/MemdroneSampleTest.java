package gov.nasa.ziggy.metrics.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

/**
 * @author Todd Klaus
 */
public class MemdroneSampleTest {
    private static final String VALID_LINE = "Tue Aug 14 06:39:13 PDT 2012 43085  845252 tps";
    private static final String DEFUNCT_LINE = "Tue Aug 14 06:39:13 PDT 2012 43085 845252 tps <defunct>";
    private static final String INVALID_LINE = "Tue Aug 14 06:39:13 PDT 2012 a b tps";

    @Test
    public void testValid() {
        MemdroneSample s = new MemdroneSample(VALID_LINE);

        assertEquals("processName", "tps", s.getProcessName());
        assertEquals("processId", "43085", s.getProcessId());
        assertEquals("timestampMillis", 1344951553000L, s.getTimestampMillis());
        assertEquals("percentMemory", 845252, s.getMemoryKilobytes());
    }

    @Test
    public void testDefunct() {
        MemdroneSample s = new MemdroneSample(DEFUNCT_LINE);

        assertFalse("isValid", s.isValid());
    }

    @Test
    public void testInvalid() {
        MemdroneSample s = new MemdroneSample(INVALID_LINE);

        assertFalse("isValid", s.isValid());
    }
}
