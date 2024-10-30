package gov.nasa.ziggy.ui.status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.messages.WorkerStatusMessage;
import gov.nasa.ziggy.ui.status.WorkerStatusPanel.WorkerStatusTableModel;

/**
 * Unit tests for {@link WorkerStatusTableModel} class.
 *
 * @author PT
 * @author Bill Wohler
 */
public class WorkerStatusTableModelTest {

    private WorkerStatusTableModel tableModel;
    private PipelineTask pipelineTask2;
    private PipelineTask pipelineTask3;

    @Before
    public void setUp() {

        // Set up a table model that doesn't try to redraw the (non existent) table.
        tableModel = spy(WorkerStatusTableModel.class);
        Mockito.doNothing().when(tableModel).redrawTable();

        pipelineTask2 = spy(PipelineTask.class);
        when(pipelineTask2.getId()).thenReturn(2L);
        pipelineTask3 = spy(PipelineTask.class);
        when(pipelineTask3.getId()).thenReturn(3L);
    }

    @Test
    public void testAddMessage() {

        // The model starts out empty.
        assertTrue(tableModel.statusMessages().isEmpty());
        assertTrue(tableModel.messageSet().isEmpty());

        // Add a not-final-message message.
        WorkerStatusMessage message = new WorkerStatusMessage(1, "awesome", "2", pipelineTask3,
            "dummy", "single", 1234L, false);
        tableModel.updateModel(message);

        // There should be a message in the Map and in the Set.
        assertEquals(1, tableModel.statusMessages().size());
        assertEquals(1, tableModel.messageSet().size());

        // The Map message should be up to date.
        assertTrue(tableModel.statusMessages().get(message));

        // The model should have called the redrawTable() method once.
        Mockito.verify(tableModel, times(1)).redrawTable();
    }

    @Test
    public void testReplaceMessage() {

        // Add a not-final-message message.
        WorkerStatusMessage message = new WorkerStatusMessage(1, "awesome", "2", pipelineTask3,
            "dummy", "single", 1234L, false);
        tableModel.updateModel(message);

        // Add another message for the same task
        message = new WorkerStatusMessage(1, "more awesome", "2", pipelineTask3, "dummy", "single",
            5678L, false);
        tableModel.updateModel(message);

        // There should be a message in the Map and in the Set.
        assertEquals(1, tableModel.statusMessages().size());
        assertEquals(1, tableModel.messageSet().size());

        // The second message should be the one that's in the Map and the Set
        Iterator<WorkerStatusMessage> messageIterator = tableModel.statusMessages()
            .keySet()
            .iterator();
        message = messageIterator.next();
        assertEquals("more awesome", message.getState());
        messageIterator = tableModel.messageSet().iterator();
        message = messageIterator.next();
        assertEquals("more awesome", message.getState());

        // There should be two calls to redraw the table
        Mockito.verify(tableModel, times(2)).redrawTable();
    }

    @Test
    public void testFinalMessage() {

        // Add a not-final-message message.
        WorkerStatusMessage message = new WorkerStatusMessage(1, "awesome", "2", pipelineTask3,
            "dummy", "single", 1234L, false);
        tableModel.updateModel(message);

        // Send a final message for the same task.
        message = new WorkerStatusMessage(1, "more awesome", "2", pipelineTask3, "dummy", "single",
            5678L, true);
        tableModel.updateModel(message);

        // The model should now be empty.
        assertTrue(tableModel.statusMessages().isEmpty());
        assertTrue(tableModel.messageSet().isEmpty());

        // There should be two calls to redraw the table.
        Mockito.verify(tableModel, times(2)).redrawTable();
    }

    @Test
    public void testMessageOrdering() {

        // Add a not-final-message message.
        WorkerStatusMessage message = new WorkerStatusMessage(1, "awesome", "2", pipelineTask3,
            "dummy", "single", 1234L, false);
        tableModel.updateModel(message);

        // Add a message for a task with a lower number.
        message = new WorkerStatusMessage(1, "awesome", "2", pipelineTask2, "dummy", "single",
            1234L, false);
        tableModel.updateModel(message);

        // There should be 2 messages in the model.
        assertEquals(2, tableModel.statusMessages().size());
        assertEquals(2, tableModel.messageSet().size());

        // The second message to be added should be first when iterating over the model.
        Iterator<WorkerStatusMessage> messageIterator = tableModel.messageSet().iterator();
        message = messageIterator.next();
        assertEquals(pipelineTask2, message.getPipelineTask());
        message = messageIterator.next();
        assertEquals(pipelineTask3, message.getPipelineTask());
    }

    @Test
    public void testMessageOutdating() {

        // Add a not-final-message message.
        WorkerStatusMessage message = new WorkerStatusMessage(1, "awesome", "2", pipelineTask3,
            "dummy", "single", 1234L, false);
        tableModel.updateModel(message);

        // On the first call, the message should be marked as outdated.
        tableModel.removeOutdatedMessages();
        assertEquals(1, tableModel.statusMessages().size());
        assertFalse(tableModel.statusMessages().get(message));

        // On the second call, the message should be gone.
        tableModel.removeOutdatedMessages();
        assertTrue(tableModel.statusMessages().isEmpty());
        assertTrue(tableModel.messageSet().isEmpty());

        // There should be two calls to redraw the table.
        Mockito.verify(tableModel, times(2)).redrawTable();
    }

    @Test
    public void testMessageLifeCycle() {

        // Add a not-final-message message.
        WorkerStatusMessage message = new WorkerStatusMessage(1, "awesome", "2", pipelineTask3,
            "dummy", "single", 1234L, false);
        tableModel.updateModel(message);

        assertEquals(1, tableModel.statusMessages().size());
        assertTrue(tableModel.statusMessages().get(message));
        Mockito.verify(tableModel, times(1)).redrawTable();

        // Update the message up-to-date flag (emulates a reaction to heartbeat detection).
        // Note that we don't redraw the table when we're just marking messages as out of date.
        tableModel.removeOutdatedMessages();
        assertEquals(1, tableModel.statusMessages().size());
        assertFalse(tableModel.statusMessages().get(message));
        Mockito.verify(tableModel, times(1)).redrawTable();

        // Send a new message from the same task.
        message = new WorkerStatusMessage(1, "more awesome", "2", pipelineTask3, "dummy", "single",
            1234L, false);
        tableModel.updateModel(message);
        assertEquals(1, tableModel.statusMessages().size());
        assertTrue(tableModel.statusMessages().get(message));
        Mockito.verify(tableModel, times(2)).redrawTable();
        Iterator<WorkerStatusMessage> messageIterator = tableModel.messageSet().iterator();
        assertEquals("more awesome", messageIterator.next().getState());

        // Update the message-up-to-date flag.
        tableModel.removeOutdatedMessages();
        assertEquals(1, tableModel.statusMessages().size());
        assertFalse(tableModel.statusMessages().get(message));
        Mockito.verify(tableModel, times(2)).redrawTable();

        // Send a final message.
        message = new WorkerStatusMessage(1, "more awesome", "2", pipelineTask3, "dummy", "single",
            1234L, true);
        tableModel.updateModel(message);
        assertTrue(tableModel.statusMessages().isEmpty());
        Mockito.verify(tableModel, times(3)).redrawTable();
    }
}
