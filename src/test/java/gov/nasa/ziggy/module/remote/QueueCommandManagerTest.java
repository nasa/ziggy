package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_QUEUE_COMMAND_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.RemoteJob.RemoteJobQstatInfo;

/**
 * Unit test class for the {@link QueueCommandManager}.
 *
 * @author PT
 */
public class QueueCommandManagerTest {

    private QueueCommandManager cmdManager;

    @Rule
    public ZiggyPropertyRule queueCommandClassPropertyRule = new ZiggyPropertyRule(
        REMOTE_QUEUE_COMMAND_CLASS, "gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests");

    @Before
    public void setup() {

        // make sure that we have an instance of the test class
        cmdManager = Mockito.spy(QueueCommandManager.newInstance());
        Mockito.when(cmdManager.hostname()).thenReturn("host");
        Mockito.when(cmdManager.user()).thenReturn("user");
    }

    /**
     * Test getQstatInfoByJobName method.
     */
    @Test
    public void testGetQstatInfoByJobName() {

        // create multiple jobs that have the same job name for return
        String job1 = "9101316.batch user low    340-23788-tps   5   5 04:00 F 02:33  254%";
        String job2 = "9101317.batch user low    340-23788-tps   5   5 04:00 F 02:33  254%";
        mockQstatCall("-u user", new String[] { "340-23788-tps" }, job1, job2);

        // execute the method
        List<String> returnedJobs = cmdManager.getQstatInfoByTaskName("user", "340-23788-tps");
        assertEquals(2, returnedJobs.size());
        assertTrue(returnedJobs.get(0).equals(job1));
        assertTrue(returnedJobs.get(1).equals(job2));
    }

    /**
     * Tests serverNames method
     */
    @Test
    public void testServerNames() {

        // create the return strings
        String job1 = "Job: 9101154.batch.example.com";
        String owner1 = "    Job_Owner = user@host2.example.com";
        String job2 = "Job: 9101189.batch.example.com";
        String owner2 = "    Job_Owner = user@host1.example.com";

        // set up the returns for the expected call to qstat
        mockQstatCall("-xf 9101189 9101154", new String[] { "Job:", "Job_Owner" }, job1, owner1,
            job2, owner2);

        // create the list of job IDs -- note, create in a different order from the
        // return order
        List<Long> jobIds = new ArrayList<>();
        jobIds.add(9101189L);
        jobIds.add(9101154L);

        // execute the method
        Map<Long, String> jobIdServerMap = cmdManager.serverNames(jobIds);
        assertEquals(2, jobIdServerMap.size());
        assertTrue(jobIdServerMap.containsKey(9101154L));
        assertEquals("host2", jobIdServerMap.get(9101154L));
        assertTrue(jobIdServerMap.containsKey(9101189L));
        assertEquals("host1", jobIdServerMap.get(9101189L));
    }

    /**
     * Tests the method that returns an exit status for a job.
     */
    @Test
    public void testExitStatus() {

        // create the string that gets returned
        String returnString = "    Exit_status = 0";
        mockQstatCall("-xf 9101154", new String[] { "Exit_status" }, returnString);

        assertEquals(0, cmdManager.exitStatus(9101154L).intValue());
    }

    /**
     * Test that an appropriate {@link RemoteJobQstatInfo} instance is returned when all necessary
     * information is present in the return from qstat.
     */
    @Test
    public void testRemoteJobQstatInfo() {

        // Mock a return that has both the select value and the wall time value
        mockQstatCall("-xf 1234567",
            new String[] { "Resource_List.select", "resources_used.walltime" },
            "    resources_used.walltime = 14:15:00", "    Resource_List.select = 1:model=has");
        RemoteJob.RemoteJobQstatInfo info = cmdManager.remoteJobQstatInfo(1234567);
        assertEquals(1, info.getNodes());
        assertEquals("has", info.getModel());
        assertEquals("14:15:00", info.getWallTime());
    }

    /**
     * Test that an appropriate {@link RemoteJobQstatInfo} instance is returned when qstat does not
     * return a walltime (i.e., job hasn't started yet).
     */
    @Test
    public void testRemoteJobQstatInfoNoWalltime() {

        // Mock a return that has both the select value and the wall time value
        mockQstatCall("-xf 1234567",
            new String[] { "Resource_List.select", "resources_used.walltime" },
            "    Resource_List.select = 1:model=has");
        RemoteJob.RemoteJobQstatInfo info = cmdManager.remoteJobQstatInfo(1234567);
        assertEquals(1, info.getNodes());
        assertEquals("has", info.getModel());
        assertNull(info.getWallTime());
    }

    /**
     * Test that an appropriate {@link RemoteJobQstatInfo} instance is returned when qstat does not
     * return any information (i.e., job is so old that it's not available via qstat -x).
     */
    @Test
    public void testRemoteJobQstatInfoNothingReturned() {

        // Mock a return that has both the select value and the wall time value
        mockQstatCall("-xf 1234567",
            new String[] { "Resource_List.select", "resources_used.walltime" });
        RemoteJob.RemoteJobQstatInfo info = cmdManager.remoteJobQstatInfo(1234567);
        assertEquals(0, info.getNodes());
        assertNull(info.getModel());
        assertNull(info.getWallTime());
    }

    /**
     * Test deleteJobsByJobId() method.
     */
    @Test
    public void testDeleteJobsByJobId() {
        List<Long> jobIds = new ArrayList<>();
        jobIds.add(1234567L);
        jobIds.add(1234568L);
        jobIds.add(1234587L);

        cmdManager.deleteJobsByJobId(jobIds);
        Mockito.verify(cmdManager, Mockito.times(1)).qdel("1234567 1234568 1234587 ");
    }

    @Test
    public void testRemoteJobInformation() {
        RemoteJob remoteJob = new RemoteJob();
        remoteJob.setFinished(false);
        remoteJob.setJobId(1234567L);
        mockQstatCall("-xf 1234567",
            new String[] { QueueCommandManager.JOBNAME, QueueCommandManager.OUTPUT_PATH },
            "    Job_Name = dv-118-36426.0",
            "    Output_Path = draco.nas.nasa.gov:/non/existent/path");
        RemoteJobInformation remoteJobInformation = cmdManager.remoteJobInformation(remoteJob);
        assertNotNull(remoteJobInformation);
        assertEquals(1234567L, remoteJobInformation.getJobId());
        assertEquals("/non/existent/path", remoteJobInformation.getLogFile());
        assertEquals("dv-118-36426.0", remoteJobInformation.getJobName());
    }

    // Mocks the cmdManager in this test to return the correct list of strings when
    // given the expected qstat command line
    public void mockQstatCall(String command, String[] grepArgs, String... responses) {
        if (grepArgs != null) {
            mockQstatCall(cmdManager, command, grepArgs, responses);
        } else {
            mockQstatCall(cmdManager, command, responses);
        }
    }

    // mocks any command manager to return the correct list of strings when given
    // the expected qstat command line
    public static void mockQstatCall(QueueCommandManager cmdManager, String command,
        String[] grepArgs, String... responses) {
        List<String> replies = new ArrayList<>();
        if (responses != null) {
            Collections.addAll(replies, responses);
        }
        Mockito.doReturn(replies).when(cmdManager).qstat(command, grepArgs);
    }

    public static void mockQstatCall(QueueCommandManager cmdManager, String command,
        String... responses) {
        List<String> replies = new ArrayList<>();
        if (responses != null) {
            Collections.addAll(replies, responses);
        }
        Mockito.doReturn(replies).when(cmdManager).qstat(command, (String[]) null);
    }
}
