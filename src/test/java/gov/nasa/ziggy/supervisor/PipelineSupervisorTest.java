package gov.nasa.ziggy.supervisor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests;
import gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests.QueueDeleteCommand;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.messages.KillTasksRequest;
import gov.nasa.ziggy.services.messages.KilledTaskMessage;
import gov.nasa.ziggy.util.Requestor;

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
 */
public class PipelineSupervisorTest implements Requestor {

    @Rule
    public ZiggyPropertyRule queueCommandRule = new ZiggyPropertyRule(
        PropertyName.REMOTE_QUEUE_COMMAND_CLASS,
        "gov.nasa.ziggy.module.remote.QueueCommandManagerForUnitTests");

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    private PipelineSupervisor supervisor;
    private PipelineOperations operations = Mockito.mock(PipelineOperations.class);
    private AlertService alertService = Mockito.mock(AlertService.class);
    private Set<StateFile> stateFiles = new HashSet<>();

    @Before
    public void setUp() {
        supervisor = Mockito.spy(new PipelineSupervisor(1, 12000));
        Mockito.doReturn(alertService).when(supervisor).alertService();
        Mockito.doReturn(operations).when(supervisor).pipelineOperations();
        Mockito.doReturn(stateFiles).when(supervisor).remoteTaskStateFiles();
    }

    /** Verify that the task-kill command works when the state file collection is null. */
    @Test
    public void testKillTasksNullStateFileCollection() {
        stateFiles = null;
        supervisor.killRemoteTasks(KillTasksRequest.forTaskIds(this, Set.of(1L, 2L, 3L)));
        assertEquals(0, queueCommandManager().getQueueDeleteCommands().size());
        Mockito.verify(supervisor, times(0))
            .publishKilledTaskMessage(ArgumentMatchers.any(KillTasksRequest.class),
                ArgumentMatchers.any(Long.class));
    }

    /** Verify that the task-kill command works when the state file collection is empty. */
    @Test
    public void testKillTasksEmptyStateFileCollection() {
        supervisor.killRemoteTasks(KillTasksRequest.forTaskIds(this, Set.of(1L, 2L, 3L)));
        assertEquals(0, queueCommandManager().getQueueDeleteCommands().size());
        Mockito.verify(supervisor, times(0))
            .publishKilledTaskMessage(ArgumentMatchers.any(KillTasksRequest.class),
                ArgumentMatchers.any(Long.class));
    }

    /**
     * Verify that the kill-tasks command works when the state file collection and the kill request
     * have some overlap in tasks but not complete (specifically, each of them has a task that the
     * other one does not).
     */
    @Test
    public void killTasksAllSuccessful() {
        StateFile stateFile1 = Mockito.mock(StateFile.class);
        Mockito.when(stateFile1.getPipelineTaskId()).thenReturn(1L);
        stateFiles.add(stateFile1);
        StateFile stateFile2 = Mockito.mock(StateFile.class);
        Mockito.when(stateFile2.getPipelineTaskId()).thenReturn(2L);
        stateFiles.add(stateFile2);
        StateFile stateFile3 = Mockito.mock(StateFile.class);
        Mockito.when(stateFile3.getPipelineTaskId()).thenReturn(3L);
        stateFiles.add(stateFile3);

        KillTasksRequest request = KillTasksRequest.forTaskIds(this, Set.of(1L, 2L, 4L));
        supervisor.killRemoteTasks(request);
        Map<Long, QueueDeleteCommand> queueDeleteCommands = queueCommandManager()
            .getQueueDeleteCommands();
        assertEquals(2, queueDeleteCommands.size());
        assertNull(queueDeleteCommands.get(3L));
        assertNull(queueDeleteCommands.get(4L));
        assertEquals(stateFile1, queueDeleteCommands.get(1L).getStateFile());
        assertEquals(0, queueDeleteCommands.get(1L).getReturnCode());
        assertEquals(stateFile2, queueDeleteCommands.get(2L).getStateFile());
        assertEquals(0, queueDeleteCommands.get(2L).getReturnCode());
        Mockito.verify(supervisor).publishKilledTaskMessage(request, 1L);
        Mockito.verify(supervisor).publishKilledTaskMessage(request, 2L);
        Mockito.verify(supervisor, times(0)).publishKilledTaskMessage(request, 3L);
        Mockito.verify(supervisor, times(0)).publishKilledTaskMessage(request, 4L);
    }

    /**
     * Verify that the correct action is taken when not all the tasks to be killed are in fact
     * successfully killed.
     */
    @Test
    public void testKillTasksSomeFailures() {
        StateFile stateFile1 = Mockito.mock(StateFile.class);
        Mockito.when(stateFile1.getPipelineTaskId()).thenReturn(1L);
        stateFiles.add(stateFile1);
        StateFile stateFile2 = Mockito.mock(StateFile.class);
        Mockito.when(stateFile2.getPipelineTaskId()).thenReturn(150L);
        stateFiles.add(stateFile2);
        StateFile stateFile3 = Mockito.mock(StateFile.class);
        Mockito.when(stateFile3.getPipelineTaskId()).thenReturn(3L);
        stateFiles.add(stateFile3);

        // Two kill task requests should have gone out.
        KillTasksRequest request = KillTasksRequest.forTaskIds(this, Set.of(1L, 150L, 4L));
        supervisor.killRemoteTasks(request);
        Map<Long, QueueDeleteCommand> queueDeleteCommands = queueCommandManager()
            .getQueueDeleteCommands();
        assertEquals(2, queueDeleteCommands.size());
        assertNull(queueDeleteCommands.get(3L));
        assertNull(queueDeleteCommands.get(4L));

        // One of the two had the qdel command return 0, the other returned 1.
        assertEquals(stateFile1, queueDeleteCommands.get(1L).getStateFile());
        assertEquals(0, queueDeleteCommands.get(1L).getReturnCode());
        assertEquals(stateFile2, queueDeleteCommands.get(150L).getStateFile());
        assertEquals(1, queueDeleteCommands.get(150L).getReturnCode());

        // Only the case where qdel returned 0 should publish a task message.
        Mockito.verify(supervisor).publishKilledTaskMessage(request, 1L);
        Mockito.verify(supervisor, times(0)).publishKilledTaskMessage(request, 150L);
        Mockito.verify(supervisor, times(0)).publishKilledTaskMessage(request, 3L);
        Mockito.verify(supervisor, times(0)).publishKilledTaskMessage(request, 4L);
    }

    /** Verify that the correct action is taken when a {@link KilledTaskMessage} arrives. */
    @Test
    public void testHandleKilledTaskMessage() {
        assertFalse(PipelineSupervisor.taskOnKilledTaskList(1L));
        KilledTaskMessage message = new KilledTaskMessage(this, 1L);
        supervisor.handleKilledTaskMessage(message);
        assertTrue(PipelineSupervisor.taskOnKilledTaskList(1L));
        Mockito.verify(alertService)
            .generateAndBroadcastAlert("PI", 1L, AlertService.Severity.ERROR, "Task 1 halted");
        Mockito.verify(operations).setTaskState(1L, PipelineTask.State.ERROR);
    }

    public QueueCommandManagerForUnitTests queueCommandManager() {
        return (QueueCommandManagerForUnitTests) supervisor.getQueueCommandManager();
    }

    @Override
    public Object requestorIdentifier() {
        return Integer.valueOf(0);
    }
}
