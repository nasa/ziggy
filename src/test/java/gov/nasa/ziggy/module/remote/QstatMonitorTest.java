package gov.nasa.ziggy.module.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.PropertyNames;

/**
 * Unit test class for QstatMonitor class, QstatQuery class, and the builder for the QstatMonitor
 * class.
 *
 * @author PT
 */
public class QstatMonitorTest {

    private QueueCommandManager cmdManager;

    @Before
    public void setup() {

        // make sure that we have an instance of the command manager that we can use
        System.setProperty(PropertyNames.QUEUE_COMMAND_CLASS_PROP_NAME,
            "gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests");
        cmdManager = Mockito.spy(QueueCommandManager.newInstance());
        Mockito.when(cmdManager.hostname()).thenReturn("host1");
        Mockito.when(cmdManager.user()).thenReturn("user");
    }

    @After
    public void teardown() {

        // remove the system properties that direct the factory to use a test class
        // for the command manager
        System.clearProperty(PropertyNames.QUEUE_COMMAND_CLASS_PROP_NAME);
    }

    /**
     * Test all constructor syntaxes. Also exercises the getOwner(), getServerName(), and
     * getQstatCommandManager() methods.
     */
    @Test
    public void testConstructors() {

        // 2-argument constructor
        QstatMonitor monitor = new QstatMonitor("user", "server");
        assertEquals("user", monitor.getOwner());
        assertEquals("server", monitor.getServerName());
        assertTrue(monitor.getQstatCommandManager() instanceof QueueCommandManagerForUnitTests);

        // 1-argument constructor
        monitor = new QstatMonitor(cmdManager);
        assertEquals("user", monitor.getOwner());
        assertEquals("host1", monitor.getServerName());
        assertSame(cmdManager, monitor.getQstatCommandManager());

    }

    /**
     * Tests addToMonitor methods. Also exercises the jobsInMonitor() method.
     */
    @Test
    public void testAddToMonitoring() {
        StateFile stateFile = new StateFile("tps", 100, 200);
        QstatMonitor monitor = new QstatMonitor(cmdManager);
        monitor.addToMonitoring(stateFile);
        Set<String> jobsInMonitor = monitor.getJobsInMonitor();
        assertEquals(1, jobsInMonitor.size());
        assertTrue(jobsInMonitor.contains("100-200-tps"));

        PipelineTask task = Mockito.mock(PipelineTask.class);
        Mockito.when(task.taskBaseName()).thenReturn(PipelineTask.taskBaseName(100L, 201L, "tps"));

        monitor.addToMonitoring(task);
        jobsInMonitor = monitor.getJobsInMonitor();
        assertEquals(2, jobsInMonitor.size());
        assertTrue(jobsInMonitor.contains("100-200-tps"));
        assertTrue(jobsInMonitor.contains("100-201-tps"));
    }

    /**
     * Tests the endMonitor method.
     */
    @Test
    public void testEndMonitoring() {
        QstatMonitor monitor = new QstatMonitor(cmdManager);
        monitor.addToMonitoring(new StateFile("tps", 100, 200));
        monitor.addToMonitoring(new StateFile("tps", 100, 201));
        monitor.endMonitoring(new StateFile("tps", 100, 200));
        Set<String> jobsInMonitor = monitor.getJobsInMonitor();
        assertEquals(1, jobsInMonitor.size());
        assertTrue(jobsInMonitor.contains("100-201-tps"));
    }

    /**
     * Exercises the update method. Also exercises the jobId method, the isFinished method, the
     * exitStatus method, and the exitComment method.
     */
    @Test
    public void testUpdate() {
        QstatMonitor monitor = new QstatMonitor(cmdManager);
        monitor.addToMonitoring(new StateFile("tps", 100, 200));
        monitor.addToMonitoring(new StateFile("tps", 100, 201));
        monitor.addToMonitoring(new StateFile("tps", 100, 202));

        // mock the returns for each of the 3 tasks: note that the first task will
        // return exactly 1 job, the second task will return 2 jobs (different server
        // name), and the 3rd job will return nothing.

        String header1 = "                                                   Req'd    Elap";
        String header2 = "JobID          User    Queue Jobname       TSK Nds wallt S wallt  Eff";
        String header3 = "-------------- ------- ----- ------------- --- --- ----- - ----- ----";

        String qstat1 = "1234567.batch user low    100-200-tps    5   5 04:00 R 02:33  254%";
        String qstat2a = "1234587.batch user low    100-201-tps    5   5 04:00 R 02:33  254%";
        String qstat2b = "1234597.batch user low    100-201-tps    5   5 04:00 R 02:33  254%";

        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-200-tps" },
            qstat1);
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-201-tps" },
            qstat2a, qstat2b);
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "100-202-tps" },
            (String[]) null);

        // mock the return of the server names
        String[] jobOrOwner = new String[] { "Job:", "Job_Owner" };
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234567", jobOrOwner,
            "Job: 1234567.batch.example.com", "    Job_Owner = user@host1.example.com");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234587 1234597", jobOrOwner,
            "Job: 1234587.batch.example.com", "    Job_Owner = user@host2.example.com",
            "Job: 1234597.batch.example.com", "    Job_Owner = user@host1.example.com");

        monitor.update();

        // set up the pipeline tasks so we can retrieve job IDs

        PipelineTask task1 = Mockito.mock(PipelineTask.class);
        Mockito.when(task1.taskBaseName()).thenReturn(PipelineTask.taskBaseName(100L, 200L, "tps"));

        PipelineTask task2 = Mockito.mock(PipelineTask.class);
        Mockito.when(task2.taskBaseName()).thenReturn(PipelineTask.taskBaseName(100L, 201L, "tps"));

        Mockito.when(task2.getId()).thenReturn(201L);

        Set<Long> task1Jobs = monitor.allIncompleteJobIds(task1);
        Set<Long> task2Jobs = monitor.allIncompleteJobIds(task2);
        assertEquals(1, task1Jobs.size());
        assertTrue(task1Jobs.contains(1234567L));
        assertEquals(1, task2Jobs.size());
        assertTrue(task2Jobs.contains(1234597L));

        // now perform an update -- this allows the state of each job to be
        // updated

        String qstat1Update = "1234567.batch user low    100-200-tps    5   5 04:00 F 02:33  254%";
        String qstat2Update = "1234597.batch user low    100-201-tps    5   5 04:00 R 02:34  254%";
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-x 1234567 1234597", header1, header2,
            header3, qstat1Update, qstat2Update);

        // before we perform the further update, neither task should be finished
        assertFalse(monitor.isFinished(new StateFile("tps", 100, 200)));
        assertFalse(monitor.isFinished(new StateFile("tps", 100, 201)));
        monitor.update();

        // now the first task should be finished but not the second
        assertTrue(monitor.isFinished(new StateFile("tps", 100, 200)));
        assertFalse(monitor.isFinished(new StateFile("tps", 100, 201)));

        // finally test the exit status retriever
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234567",
            new String[] { "Exit_status" }, "Exit_status = 0");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234567", new String[] { "comment" },
            "comment = finished, okay?");
        Map<Long, Integer> exitStatusMap = monitor.exitStatus(new StateFile("tps", 100, 200));
        assertEquals(1, exitStatusMap.size());
        assertEquals(0, exitStatusMap.get(1234567L).intValue());
        Map<Long, String> exitCommentMap = monitor.exitComment(new StateFile("tps", 100, 200));
        assertEquals(1, exitCommentMap.size());
        assertEquals("finished, okay?", exitCommentMap.get(1234567L));
    }

    /**
     * Exercises the update method for the use-case in which tasks produce multiple jobs.
     */
    @Test
    public void testUpdateMultipleJobsPerTask() {
        QstatMonitor monitor = new QstatMonitor(cmdManager);
        monitor.addToMonitoring(new StateFile("tps", 100, 200));
        monitor.addToMonitoring(new StateFile("tps", 100, 201));
        monitor.addToMonitoring(new StateFile("tps", 100, 202));

        // mock the returns for each of the 3 tasks: note that the first task will
        // return exactly 2 jobs, the second task will return 3 jobs (different server
        // name for one of them), and the 3rd job will return nothing.

        String header1 = "                                                   Req'd    Elap";
        String header2 = "JobID          User    Queue Jobname       TSK Nds wallt S wallt  Eff";
        String header3 = "-------------- ------- ----- ------------- --- --- ----- - ----- ----";

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
        String[] jobOrOwner = new String[] { "Job:", "Job_Owner" };
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234567 1234568", jobOrOwner,
            "Job: 1234567.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234568.batch.example.com", "    Job_Owner = user@host1.example.com");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234587 1234597 1234599", jobOrOwner,
            "Job: 1234587.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234597.batch.example.com", "    Job_Owner = user@host1.example.com",
            "Job: 1234599.batch.example.com", "    Job_Owner = user@host2.example.com");

        monitor.update();

        // set up the pipeline tasks so we can retrieve job IDs

        PipelineTask task1 = Mockito.mock(PipelineTask.class);
        Mockito.when(task1.taskBaseName()).thenReturn(PipelineTask.taskBaseName(100L, 200L, "tps"));

        PipelineTask task2 = Mockito.mock(PipelineTask.class);
        Mockito.when(task2.taskBaseName()).thenReturn(PipelineTask.taskBaseName(100L, 201L, "tps"));

        Set<Long> task1Jobs = monitor.allIncompleteJobIds(task1);
        Set<Long> task2Jobs = monitor.allIncompleteJobIds(task2);
        assertEquals(2, task1Jobs.size());
        assertTrue(task1Jobs.contains(1234567L));
        assertTrue(task1Jobs.contains(1234568L));
        assertEquals(2, task2Jobs.size());
        assertTrue(task2Jobs.contains(1234587L));
        assertTrue(task2Jobs.contains(1234597L));

        // now perform an update -- this allows the state of each job to be
        // updated

        String qstat1aUpdate = "1234567.batch user low    100-200-tps.0  5   5 04:00 F 02:33  254%";
        String qstat1bUpdate = "1234568.batch user low    100-200-tps.1  5   5 04:00 F 02:33  254%";
        String qstat2aUpdate = "1234587.batch user low    100-201-tps.0  5   5 04:00 R 02:34  254%";
        String qstat2bUpdate = "1234597.batch user low    100-201-tps.1  5   5 04:00 F 02:34  254%";
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-x 1234567 1234568 1234587 1234597",
            header1, header2, header3, qstat1aUpdate, qstat1bUpdate, qstat2aUpdate, qstat2bUpdate);

        // before we perform the further update, neither task should be finished
        assertFalse(monitor.isFinished(new StateFile("tps", 100, 200)));
        assertFalse(monitor.isFinished(new StateFile("tps", 100, 201)));
        monitor.update();

        // now the first task should be finished but not the second
        assertTrue(monitor.isFinished(new StateFile("tps", 100, 200)));
        assertFalse(monitor.isFinished(new StateFile("tps", 100, 201)));

        // finally test the exit status retriever
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234567",
            new String[] { "Exit_status" }, "Exit_status = 0");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234567", new String[] { "comment" },
            "comment = finished, okay?");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234568",
            new String[] { "Exit_status" }, "Exit_status = 1");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234568", new String[] { "comment" },
            "comment = really finished");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234587",
            new String[] { "Exit_status" }, (String[]) null);
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234587", new String[] { "comment" },
            (String[]) null);
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234597",
            new String[] { "Exit_status" }, "Exit_status = 2");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 1234597", new String[] { "comment" },
            "comment = crashed and burned");
        Map<Long, Integer> exitStatusMap = monitor.exitStatus(new StateFile("tps", 100, 200));
        assertEquals(2, exitStatusMap.size());
        assertEquals(0, exitStatusMap.get(1234567L).intValue());
        assertEquals(1, exitStatusMap.get(1234568L).intValue());
        Map<Long, String> exitCommentMap = monitor.exitComment(new StateFile("tps", 100, 200));
        assertEquals(2, exitCommentMap.size());
        assertEquals("finished, okay?", exitCommentMap.get(1234567L));
        assertEquals("really finished", exitCommentMap.get(1234568L));

        exitStatusMap = monitor.exitStatus(new StateFile("tps", 100, 201));
        assertEquals(1, exitStatusMap.size());
        assertEquals(2, exitStatusMap.get(1234597L).intValue());
        exitCommentMap = monitor.exitComment(new StateFile("tps", 100, 201));
        assertEquals(1, exitCommentMap.size());
        assertEquals("crashed and burned", exitCommentMap.get(1234597L));
    }

}
