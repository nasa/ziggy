package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SystemProxyTest {

    private static final long EARLY_IN_2024 = 1704391820427L;

    @Test
    public void testCurrentTimeMillis() {
        assertTrue(SystemProxy.currentTimeMillis() > EARLY_IN_2024);

        SystemProxy.setUserTime(EARLY_IN_2024);
        assertEquals(EARLY_IN_2024, SystemProxy.currentTimeMillis());
    }

    @Test
    public void testExit() {
        SystemProxy.disableExit();
        SystemProxy.exit(42);
        assertEquals(Integer.valueOf(42), SystemProxy.getLatestExitCode());
    }
}
