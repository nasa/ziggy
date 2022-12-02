package gov.nasa.ziggy.metrics.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Todd Klaus
 */
public class MemdroneLogTest {
    private static final Logger log = LoggerFactory.getLogger(MemdroneLogTest.class);

    private static final String MEMDRONE_LOG_PATH = "test/data/memdrone/memdrone-r190i1n5.txt.gz";
    private static final String[] expectedPids = { "62960", "62954", "54531", "89594", "62956",
        "62963", "72050", "83687", "62959", "96229", "20017", "47885", "50848" };
    private static final int[] expectedSampleCounts = { 109003, 21253, 17881, 1, 42567, 25968,
        15783, 209673, 85938, 10369, 1, 1, 13542 };

    @Test
    public void testParse() throws Exception {
        File memdroneFile = new File(MEMDRONE_LOG_PATH);
        InputStream input = new GZIPInputStream(new FileInputStream(memdroneFile));
        MemdroneLog mLog = new MemdroneLog(input);
        Map<String, DescriptiveStatistics> contents = mLog.getLogContents();

        Set<String> processIds = contents.keySet();
        assertEquals("numKeys", expectedPids.length, processIds.size());

        for (String pid : processIds) {
            log.info("pid: " + pid + ", N=" + contents.get(pid).getN());
        }

        for (int i = 0; i < expectedPids.length; i++) {
            String expectedPid = expectedPids[i];
            int expectedSampleCount = expectedSampleCounts[i];
            DescriptiveStatistics stats = contents.get(expectedPid);

            assertTrue("contains " + expectedPid, processIds.contains(expectedPid));
            assertEquals("expectedSampleCount[" + expectedPid + "]", expectedSampleCount,
                stats.getN());
        }
    }
}
