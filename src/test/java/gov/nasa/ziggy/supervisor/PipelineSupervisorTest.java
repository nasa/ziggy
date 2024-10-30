package gov.nasa.ziggy.supervisor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests;
import gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests.QueueDeleteCommand;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.messages.HaltTasksRequest;
import gov.nasa.ziggy.services.messages.TaskHaltedMessage;

/**
 * Unit tests for {@link PipelineSupervisor} class.
 * <p>
 * Given the literal centrality of the supervisor to everything Ziggy does, you would expect that we
 * would test it to within an inch of its life. However: Most of the supervisor's functions are
 * derived from the worker class in Spiffy, in which context there were exactly zero unit tests.
 * Hence, for the time being, all we can really manage is to add tests for new functionality and
 * hope that someday we get the chance to backfill tests for legacy functionality.
 *
 * @author PT
 * @author Bill Wohler
 */
public class PipelineSupervisorTest {

    @Rule
    public ZiggyPropertyRule queueCommandRule = new ZiggyPropertyRule(
        PropertyName.REMOTE_QUEUE_COMMAND_CLASS,
        "gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests");

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    private PipelineSupervisor supervisor;
    private PipelineTaskOperations pipelineTaskOperations = Mockito
        .mock(PipelineTaskOperations.class);
    private PipelineTaskDataOperations pipelineTaskDataOperations = Mockito
        .mock(PipelineTaskDataOperations.class);
    private AlertService alertService = Mockito.mock(AlertService.class);
    private Map<PipelineTask, List<Long>> jobIdsByTask = new HashMap<>();
    private List<PipelineTask> tasks = new ArrayList<>();

    private PipelineTask pipelineTask1;
    private PipelineTask pipelineTask2;
    private PipelineTask pipelineTask3;
    private PipelineTask pipelineTask4;

    @Before
    public void setUp() {
        supervisor = Mockito.spy(new PipelineSupervisor(1, 12000));
        Mockito.doReturn(alertService).when(supervisor).alertService();
        Mockito.doReturn(pipelineTaskOperations).when(supervisor).pipelineTaskOperations();
        Mockito.doReturn(pipelineTaskDataOperations).when(supervisor).pipelineTaskDataOperations();
        Mockito.doReturn(jobIdsByTask).when(supervisor).jobIdsByTask(ArgumentMatchers.anyList());

        pipelineTask1 = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(1L).when(pipelineTask1).getId();
        pipelineTask2 = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(2L).when(pipelineTask2).getId();
        pipelineTask3 = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(3L).when(pipelineTask3).getId();
        pipelineTask4 = Mockito.spy(PipelineTask.class);
        Mockito.doReturn(4L).when(pipelineTask4).getId();
    }

    /** Verify that the task-halt command works when no remote tasks are present. */
    @Test
    public void testHaltTasksEmptyStateFileCollection() {
        supervisor.haltRemoteTasks(
            new HaltTasksRequest(Set.of(pipelineTask1, pipelineTask2, pipelineTask3)));
        assertEquals(0, queueCommandManager().getQueueDeleteCommands().size());
        Mockito.verify(supervisor, times(0))
            .publishTaskHaltedMessage(ArgumentMatchers.any(PipelineTask.class));
    }

    /**
     * Verify that the halt-tasks command works when the state file collection and the halt request
     * have some overlap in tasks but not complete (specifically, each of them has a task that the
     * other one does not).
     */
    @Test
    public void testHaltTasksAllSuccessful() {

        jobIdsByTask.put(pipelineTask1, List.of(100L, 101L));
        jobIdsByTask.put(pipelineTask2, List.of(102L, 103L));
        jobIdsByTask.put(pipelineTask3, List.of(105L, 106L));

        tasks.add(pipelineTask1);
        tasks.add(pipelineTask2);
        tasks.add(pipelineTask4);

        HaltTasksRequest request = new HaltTasksRequest(tasks);
        supervisor.haltRemoteTasks(request);
        List<QueueDeleteCommand> queueDeleteCommands = queueCommandManager()
            .getQueueDeleteCommands();
        assertEquals(2, queueDeleteCommands.size());
        QueueDeleteCommand queueDeleteCommand = queueDeleteCommands.get(0);
        assertEquals(0, queueDeleteCommand.getReturnCode());
        assertTrue(queueDeleteCommand.getJobIds().contains(100L));
        assertTrue(queueDeleteCommand.getJobIds().contains(101L));
        assertEquals(2, queueDeleteCommand.getJobIds().size());
        queueDeleteCommand = queueDeleteCommands.get(1);
        assertEquals(0, queueDeleteCommand.getReturnCode());
        assertTrue(queueDeleteCommand.getJobIds().contains(102L));
        assertTrue(queueDeleteCommand.getJobIds().contains(103L));
        assertEquals(2, queueDeleteCommand.getJobIds().size());
        Mockito.verify(supervisor).publishTaskHaltedMessage(pipelineTask1);
        Mockito.verify(supervisor).publishTaskHaltedMessage(pipelineTask2);
        Mockito.verify(supervisor, times(0)).publishTaskHaltedMessage(pipelineTask3);
        Mockito.verify(supervisor, times(0)).publishTaskHaltedMessage(pipelineTask4);
    }

    /**
     * Verify that the correct action is taken when not all the tasks to be halted are in fact
     * successfully halted.
     */
    @Test
    public void testHaltTasksSomeFailures() {
        jobIdsByTask.put(pipelineTask1, List.of(100L, 101L));
        jobIdsByTask.put(pipelineTask2, List.of(102L, 203L));
        jobIdsByTask.put(pipelineTask3, List.of(105L, 106L));

        tasks.add(pipelineTask1);
        tasks.add(pipelineTask2);
        tasks.add(pipelineTask4);

        // Two kill task requests should have gone out.
        HaltTasksRequest request = new HaltTasksRequest(tasks);
        supervisor.haltRemoteTasks(request);
        List<QueueDeleteCommand> queueDeleteCommands = queueCommandManager()
            .getQueueDeleteCommands();
        assertEquals(2, queueDeleteCommands.size());
        QueueDeleteCommand queueDeleteCommand = queueDeleteCommands.get(0);
        assertEquals(0, queueDeleteCommand.getReturnCode());
        assertTrue(queueDeleteCommand.getJobIds().contains(100L));
        assertTrue(queueDeleteCommand.getJobIds().contains(101L));
        assertEquals(2, queueDeleteCommand.getJobIds().size());
        queueDeleteCommand = queueDeleteCommands.get(1);
        assertEquals(1, queueDeleteCommand.getReturnCode());
        assertTrue(queueDeleteCommand.getJobIds().contains(102L));
        assertTrue(queueDeleteCommand.getJobIds().contains(203L));
        assertEquals(2, queueDeleteCommand.getJobIds().size());

        // Only the case where qdel returned 0 should publish a task message.
        Mockito.verify(supervisor).publishTaskHaltedMessage(pipelineTask1);
        Mockito.verify(supervisor, times(0)).publishTaskHaltedMessage(pipelineTask2);
        Mockito.verify(supervisor, times(0)).publishTaskHaltedMessage(pipelineTask3);
        Mockito.verify(supervisor, times(0)).publishTaskHaltedMessage(pipelineTask4);
    }

    /** Verify that the correct action is taken when a {@link TaskHaltedMessage} arrives. */
    @Test
    public void testHandleHaltedTaskMessage() {
        assertFalse(PipelineSupervisor.taskOnHaltedTaskList(pipelineTask1));
        TaskHaltedMessage message = new TaskHaltedMessage(pipelineTask1);
        supervisor.handleTaskHaltedMessage(message);
        assertTrue(PipelineSupervisor.taskOnHaltedTaskList(pipelineTask1));
        Mockito.verify(alertService)
            .generateAndBroadcastAlert("PI", pipelineTask1, Severity.ERROR, "Task 1 halted");
        Mockito.verify(pipelineTaskDataOperations).taskErrored(pipelineTask1);
    }

    public QueueCommandManagerForUnitTests queueCommandManager() {
        return (QueueCommandManagerForUnitTests) supervisor.queueCommandManager();
    }
}
