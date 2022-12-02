package gov.nasa.ziggy.util.os;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the OS-specific implementation of the {@link CpuInfo} class.
 *
 * @author Forrest Girouard
 */
public class MemInfoTest {
    @Test
    public void test() throws Exception {
        MemInfo memInfo = OperatingSystemType.getInstance().getMemInfo();

        long totalMemory = memInfo.getTotalMemoryKB();
        long freeMemory = memInfo.getFreeMemoryKB();
        long buffers = memInfo.getBuffersKB();
        long cached = memInfo.getCachedKB();
        long cachedSwap = memInfo.getCachedSwapedKB();
        long totalSwap = memInfo.getTotalSwapKB();
        long freeSwap = memInfo.getFreeSwapKB();

        assertTrue("totalMemory", totalMemory >= 0);
        assertTrue("freeMemory", freeMemory >= 0);
        assertTrue("buffers", buffers >= 0);
        assertTrue("cached", cached >= 0);
        assertTrue("cachedSwap", cachedSwap >= 0);
        assertTrue("totalSwap", totalSwap >= 0);
        assertTrue("freeSwap", freeSwap >= 0);
    }
}
