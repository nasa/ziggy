package gov.nasa.ziggy.util.os;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the OS-specific implementation of the {@link CpuInfo} class.
 *
 * @author Forrest Girouard
 */
public class CpuInfoTest {
    private static final Logger log = LoggerFactory.getLogger(CpuInfoTest.class);

    @Test
    public void test() throws Exception {
        CpuInfo cpuInfo = OperatingSystemType.newInstance().getCpuInfo();

        int numCores = cpuInfo.getNumCores();

        log.info(String.format("%s: %d", cpuInfo.getNumCoresKey(), numCores));

        assertTrue("NumCores", numCores > 0);
    }
}
