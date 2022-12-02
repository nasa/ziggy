package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hibernate.Hibernate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.module.remote.QueueCommandManagerTest;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.database.TestUtils;

/**
 * Unit tests for the {@link PipelineTaskOperations} class.
 *
 * @author PT
 */
public class PipelineTaskOperationsTest {

    private QueueCommandManager cmdManager;
    private PipelineTaskOperations pipelineTaskOperations = spy(PipelineTaskOperations.class);

    @Before
    public void setUp() {
        TestUtils.setUpDatabase();
        System.setProperty(PropertyNames.QUEUE_COMMAND_CLASS_PROP_NAME,
            "gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests");

        cmdManager = spy(QueueCommandManager.newInstance());
        when(pipelineTaskOperations.queueCommandManager()).thenReturn(cmdManager);
    }

    @After
    public void tearDown() {
        TestUtils.tearDownDatabase();
        System.clearProperty(PropertyNames.QUEUE_COMMAND_CLASS_PROP_NAME);
    }

    @Test
    public void testCreateRemoteJobsFromQstat() {

        PipelineTask task = createPipelineTask();
        pipelineTaskOperations.createRemoteJobsFromQstat(task.getId());

        // retrieve the task from the database
        @SuppressWarnings("unchecked")
        Set<RemoteJob> remoteJobs = (Set<RemoteJob>) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineTask newTask = new PipelineTaskCrud().retrieve(1L);
                Hibernate.initialize(newTask.getRemoteJobs());
                return newTask.getRemoteJobs();
            });
        assertEquals(3, remoteJobs.size());
        assertTrue(remoteJobs.contains(new RemoteJob(9101154)));
        assertTrue(remoteJobs.contains(new RemoteJob(9102337)));
        assertTrue(remoteJobs.contains(new RemoteJob(6020203)));
        for (RemoteJob job : remoteJobs) {
            assertFalse(job.isFinished());
            assertEquals(0, job.getCostEstimate(), 1e-9);
        }
    }

    @Test
    public void testUpdateJobs() {

        // Create the task with 3 remote jobs
        createPipelineTask();
        pipelineTaskOperations.createRemoteJobsFromQstat(1L);

        mockRemoteJobUpdates();

        PipelineTask task = (PipelineTask) DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTask databaseTask = new PipelineTaskCrud().retrieve(1L);
            pipelineTaskOperations.updateJobs(databaseTask);
            return databaseTask;
        });

        // Check for the expected results
        List<RemoteJob> remoteJobs = new ArrayList<>(task.getRemoteJobs());
        RemoteJob job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9101154)));
        assertTrue(job.isFinished());
        assertEquals(20.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9102337)));
        assertFalse(job.isFinished());
        assertEquals(8.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(6020203)));
        assertFalse(job.isFinished());
        assertEquals(0, job.getCostEstimate(), 1e-9);

        // Make sure that the database was also updated
        PipelineTask databaseTask = (PipelineTask) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineTask retrievedTask = new PipelineTaskCrud().retrieve(1L);
                Hibernate.initialize(retrievedTask.getRemoteJobs());
                return retrievedTask;
            });
        assertEquals(3, databaseTask.getRemoteJobs().size());
        for (RemoteJob remoteJob : databaseTask.getRemoteJobs()) {
            RemoteJob otherJob = remoteJobs.get(remoteJobs.indexOf(remoteJob));
            assertEquals(otherJob.isFinished(), remoteJob.isFinished());
            assertEquals(otherJob.getCostEstimate(), remoteJob.getCostEstimate(), 1e-9);
        }
    }

    @Test
    public void testUpdateJobsForPipelineInstance() {

        // Create the task with 3 remote jobs
        PipelineTask task = createPipelineTask();
        pipelineTaskOperations.createRemoteJobsFromQstat(1L);

        mockRemoteJobUpdates();
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<PipelineTask> updatedTasks = pipelineTaskOperations
                    .updateJobs(task.getPipelineInstance());
                for (PipelineTask updatedTask : updatedTasks) {
                    Hibernate.initialize(updatedTask.getRemoteJobs());
                }
                return updatedTasks;
            });

        assertEquals(1, tasks.size());
        PipelineTask updatedTask = tasks.get(0);

        // Check for the expected results
        List<RemoteJob> remoteJobs = new ArrayList<>(updatedTask.getRemoteJobs());
        RemoteJob job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9101154)));
        assertTrue(job.isFinished());
        assertEquals(20.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(9102337)));
        assertFalse(job.isFinished());
        assertEquals(8.0, job.getCostEstimate(), 1e-9);
        job = remoteJobs.get(remoteJobs.indexOf(new RemoteJob(6020203)));
        assertFalse(job.isFinished());
        assertEquals(0, job.getCostEstimate(), 1e-9);

        // Make sure that the database was also updated
        PipelineTask databaseTask = (PipelineTask) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineTask retrievedTask = new PipelineTaskCrud().retrieve(1L);
                Hibernate.initialize(retrievedTask.getRemoteJobs());
                return retrievedTask;
            });
        assertEquals(3, databaseTask.getRemoteJobs().size());
        for (RemoteJob remoteJob : databaseTask.getRemoteJobs()) {
            RemoteJob otherJob = remoteJobs.get(remoteJobs.indexOf(remoteJob));
            assertEquals(otherJob.isFinished(), remoteJob.isFinished());
            assertEquals(otherJob.getCostEstimate(), remoteJob.getCostEstimate(), 1e-9);
        }

    }

    private PipelineTask createPipelineTask() {

        PipelineTask task = new PipelineTask();
        task.setId(1L);
        PipelineInstance instance = new PipelineInstance();
        instance.setId(1L);
        PipelineModuleDefinition modDef = new PipelineModuleDefinition("tps");
        PipelineDefinitionNode defNode = new PipelineDefinitionNode(modDef.getName(), "dummy");
        PipelineInstanceNode instNode = new PipelineInstanceNode(instance, defNode, modDef);
        task.setPipelineInstance(instance);
        task.setPipelineInstanceNode(instNode);
        DatabaseTransactionFactory.performTransaction(() -> {
            new PipelineInstanceCrud().create(instance);
            new PipelineModuleDefinitionCrud().create(modDef);
            new PipelineDefinitionCrud().create(defNode);
            new PipelineInstanceNodeCrud().create(instNode);
            return null;
        });
        DatabaseTransactionFactory.performTransaction(() -> {
            new PipelineTaskCrud().create(task);
            return null;
        });

        // Mock up the QueueCommandManager so that it returns 3 jobs for the pipeline task
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-u user", new String[] { "1-1-tps" },
            "9101154.batch user low   1-1-tps.0   5   5 04:00 F 01:34 419%",
            "9102337.batch user low   1-1-tps.1   5   5 04:00 F 01:34 419%",
            "6020203.batch user low   1-1-tps.2   5   5 04:00 F 01:34 419%");

        // add mocking that gets the correct server in each case
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 6020203 9101154 9102337",
            new String[] { "Job:", "Job_Owner" }, "Job: 6020203.batch.example.com",
            "    Job_Owner = user@host.example.com", "Job: 9101154.batch.example.com",
            "    Job_Owner = user@host.example.com", "Job: 9102337.batch.example.com",
            "    Job_Owner = user@host.example.com");

        return task;
    }

    private void mockRemoteJobUpdates() {

        // Set up the QueueCommandManager to inform the operations class that
        // one of the tasks is complete, one is running, and one is still queued
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 9101154",
            new String[] { "Exit_status" }, "    Exit_status = 0");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 9102337",
            new String[] { "Exit_status" }, "");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 6020203",
            new String[] { "Exit_status" }, "");

        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 9101154",
            new String[] { QueueCommandManager.SELECT, QueueCommandManager.WALLTIME },
            "    " + QueueCommandManager.WALLTIME + " = 10:00:00",
            "    " + QueueCommandManager.SELECT + " = 2:model=bro");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 9102337",
            new String[] { QueueCommandManager.SELECT, QueueCommandManager.WALLTIME },
            "    " + QueueCommandManager.WALLTIME + " = 05:00:00",
            "    " + QueueCommandManager.SELECT + " = 2:model=has");
        QueueCommandManagerTest.mockQstatCall(cmdManager, "-xf 6020203",
            new String[] { QueueCommandManager.SELECT, QueueCommandManager.WALLTIME }, "");
    }

}
