package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class PbsLogParserTest {

    private static final Path PBS_LOG_FILE_DIR = TEST_DATA.resolve("PbsLogParser");
    private RemoteJobInformation job0Information;
    private RemoteJobInformation job1Information;
    private List<RemoteJobInformation> remoteJobsInformation;

    @Before
    public void setUp() {
        job0Information = new RemoteJobInformation(
            PBS_LOG_FILE_DIR.resolve("pbs-log-comment-and-status").toString(), "test1");
        job0Information.setJobId(1023L);
        job1Information = new RemoteJobInformation(
            PBS_LOG_FILE_DIR.resolve("pbs-log-status-no-comment.txt").toString(), "test2");
        job1Information.setJobId(1024L);
        remoteJobsInformation = List.of(job0Information, job1Information);
    }

    @Test
    public void testExitCommentByJobId() {
        Map<Long, String> exitCommentByJobId = new PbsLogParser()
            .exitCommentByJobId(remoteJobsInformation);
        assertNotNull(exitCommentByJobId.get(1023L));
        assertEquals("job killed: walltime 1818 exceeded limit 1800",
            exitCommentByJobId.get(1023L));
        assertEquals(1, exitCommentByJobId.size());
    }

    @Test
    public void testExitStatusByJobId() {
        Map<Long, Integer> exitStatusByJobId = new PbsLogParser()
            .exitStatusByJobId(remoteJobsInformation);
        assertNotNull(exitStatusByJobId.get(1023L));
        assertEquals(271, exitStatusByJobId.get(1023L).intValue());
        assertNotNull(exitStatusByJobId.get(1024L));
        assertEquals(0, exitStatusByJobId.get(1024L).intValue());
        assertEquals(2, exitStatusByJobId.size());
    }

    @Test
    public void testMissingPbsLog() {
        RemoteJobInformation job2Information = new RemoteJobInformation("no-such-file", "test3");
        job2Information.setJobId(1025L);
        Map<Long, Integer> exitStatusByJobId = new PbsLogParser()
            .exitStatusByJobId(remoteJobsInformation);
        assertNull(exitStatusByJobId.get(1025L));
        assertNotNull(exitStatusByJobId.get(1024L));
        assertNotNull(exitStatusByJobId.get(1023L));
        Map<Long, String> exitCommentByJobId = new PbsLogParser()
            .exitCommentByJobId(remoteJobsInformation);
        assertNull(exitCommentByJobId.get(1025L));
        assertNull(exitCommentByJobId.get(1024L));
        assertNotNull(exitCommentByJobId.get(1023L));
    }
}
