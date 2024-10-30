package gov.nasa.ziggy.util;

import static gov.nasa.ziggy.util.ZiggyUtils.tryPatiently;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Tests the {@link ZiggyUtils} class.
 *
 * @author Bill Wohler
 */
public class ZiggyUtilsTest {

    int value;

    @Before
    public void setUp() {
        value = 0;
    }

    @Test
    public void testTryPatiently() {
        assertEquals(null, tryPatiently("foo", 0, 1, () -> 42));
        assertEquals(42, tryPatiently("foo", 1, 1, () -> 42).intValue());

        int loopCount = 4;
        assertEquals(loopCount, tryPatiently("foo", loopCount, 1, () -> {
            if (value++ < loopCount - 1) {
                throw new Exception("test");
            }
            return value;
        }).intValue());
    }

    @Test(expected = NullPointerException.class)
    public void testTryPatientlyWithNullMessage() {
        tryPatiently(null, 1, 1, null);
    }

    @Test(expected = NullPointerException.class)
    public void testTryPatientlyWithNullSupplier() {
        tryPatiently("foo", 1, 1, null);
    }

    @Test(expected = PipelineException.class)
    public void testTryPatientlyWithException() {
        int loopCount = 4;
        assertEquals(loopCount, tryPatiently("foo", loopCount - 1, 1, () -> {
            if (value++ < loopCount - 1) {
                throw new Exception("test");
            }
            return value;
        }).intValue());
    }
}
