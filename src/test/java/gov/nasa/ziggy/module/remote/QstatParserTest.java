package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_QUEUE_COMMAND_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

public class QstatParserTest {

    private QueueCommandManager cmdManager;

    @Rule
    public ZiggyPropertyRule queueCommandClassPropertyRule = new ZiggyPropertyRule(
        REMOTE_QUEUE_COMMAND_CLASS, "gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests");
    private PipelineTask pipelineTask;

    @Before
    public void setup() {

        // make sure that we have an instance of the command manager that we can use
        cmdManager = Mockito.spy(QueueCommandManager.newInstance());
        Mockito.when(cmdManager.hostname()).thenReturn("host1");
        Mockito.when(cmdManager.user()).thenReturn("user");
        pipelineTask = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask.taskBaseName()).thenReturn("100-200-tps");
    }

    @Test
    public void testPopulateJobIds() {

        QstatParser qstatParser = new QstatParser(cmdManager);
        // mock the returns for each of the 3 tasks: note that the first task will
        // return exactly 2 jobs, the second task will return 3 jobs (different server
        // name for one of them), and the 3rd job will return nothing.

        String qstat1a = "1234567.batch user low    100-200-tps.0  5   5 04:00 R 02:33  254%";
        String qstat1b = "1234568.batch user low    100-200-tps.1  5   5 04:00 R 02:33  254%";
        String qstat2a = "1234587.batch user low    100-201-tps.0  5   5 04:00 R 02:33  254%";
        String qstat2b = "1234597.batch user low    100-201-tps.1  5   5 04:00 R 02:33  254%";
        String qstat2c = "1234599.batch user low    100-201-tps    5   5 04:00 R 02:33  254%";

        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-200-tps" },
            qstat1a, qstat1b);
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-201-tps" },
            qstat2a, qstat2b, qstat2c);
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-202-tps" },
            (String[]) null);

        // mock the return of the server names
        String[] jobOrOwner = { "Job:", "Job_Owner" };
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234567 1234568", jobOrOwner,
            "Job: 1234567.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234568.batch.example.com", "    Job_Owner = user@host1.example.com");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234587 1234597 1234599", jobOrOwner,
            "Job: 1234587.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234597.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234599.batch.example.com", "    Job_Owner = user@host2.example.com");

        // Create the RemoteJobInformation instances for a task.
        RemoteJobInformation task200p0Information = new RemoteJobInformation("pbsLogfile",
            "100-200-tps.0");
        RemoteJobInformation task200p1Information = new RemoteJobInformation("pbsLogfile",
            "100-200-tps.1");
        qstatParser.populateJobIds(pipelineTask,
            List.of(task200p0Information, task200p1Information));
        assertEquals(1234567L, task200p0Information.getJobId());
        assertEquals(1234568L, task200p1Information.getJobId());
    }

    @Test
    public void testJobIdByName() {

        QstatParser qstatParser = new QstatParser(cmdManager);
        // mock the returns for each of the 3 tasks: note that the first task will
        // return exactly 2 jobs, the second task will return 3 jobs (different server
        // name for one of them), and the 3rd job will return nothing.

        String qstat1a = "1234567.batch user low    100-200-tps.0  5   5 04:00 R 02:33  254%";
        String qstat1b = "1234568.batch user low    100-200-tps.1  5   5 04:00 R 02:33  254%";
        String qstat2a = "1234587.batch user low    100-201-tps.0  5   5 04:00 R 02:33  254%";
        String qstat2b = "1234597.batch user low    100-201-tps.1  5   5 04:00 R 02:33  254%";
        String qstat2c = "1234599.batch user low    100-201-tps    5   5 04:00 R 02:33  254%";

        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-200-tps" },
            qstat1a, qstat1b);
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-201-tps" },
            qstat2a, qstat2b, qstat2c);
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-202-tps" },
            (String[]) null);

        // mock the return of the server names
        String[] jobOrOwner = { "Job:", "Job_Owner" };
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234567 1234568", jobOrOwner,
            "Job: 1234567.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234568.batch.example.com", "    Job_Owner = user@host1.example.com");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234587 1234597 1234599", jobOrOwner,
            "Job: 1234587.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234597.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234599.batch.example.com", "    Job_Owner = user@host2.example.com");

        // Obtain the Map for task 100-200-tps
        Map<String, Long> jobIdByName = qstatParser.jobIdByName("100-200-tps");
        assertTrue(jobIdByName.containsKey("100-200-tps.0"));
        assertEquals(1234567L, jobIdByName.get("100-200-tps.0").longValue());
        assertTrue(jobIdByName.containsKey("100-200-tps.1"));
        assertEquals(1234568L, jobIdByName.get("100-200-tps.1").longValue());
        assertEquals(2, jobIdByName.size());
    }
}
