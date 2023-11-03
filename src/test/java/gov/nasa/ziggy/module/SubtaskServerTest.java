package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.module.SubtaskServer.Response;
import gov.nasa.ziggy.module.SubtaskServer.ResponseType;

/**
 * Unit tests for {@link SubtaskServer} class.
 *
 * @author PT
 */
public class SubtaskServerTest {

    private SubtaskAllocator subtaskAllocator;
    private SubtaskServer subtaskServer;
    private SubtaskClient subtaskClient;

    @Before
    public void setUp() {
        subtaskAllocator = mock(SubtaskAllocator.class);
        subtaskServer = spy(new SubtaskServer(50, new TaskConfigurationManager()));
        doReturn(subtaskAllocator).when(subtaskServer).subtaskAllocator();
        subtaskClient = new SubtaskClient();
    }

    @After
    public void tearDown() {
        subtaskServer.shutdown();
    }

    @Test
    public void testStartAndStopListener() throws InterruptedException {
        subtaskServer.start();
        assertTrue(
            TestEventDetector.detectTestEvent(1000L, () -> subtaskServer.isListenerRunning()));
        subtaskServer.shutdown();
        assertTrue(
            TestEventDetector.detectTestEvent(1000L, () -> !subtaskServer.isListenerRunning()));
    }

    @Test
    public void testNextSubtask() throws InterruptedException {
        subtaskServer.start();
        when(subtaskAllocator.nextSubtask()).thenReturn(new SubtaskAllocation(ResponseType.OK, 10));

        Response response = subtaskClient.nextSubtask();
        assertTrue(SubtaskServer.getRequestQueue().isEmpty());
        assertEquals(ResponseType.OK, response.status);
        assertEquals(10, response.subtaskIndex);

        Mockito.verify(subtaskAllocator).nextSubtask();
    }

    @Test
    public void testNoMoreSubtasks() throws InterruptedException {
        subtaskServer.start();
        when(subtaskAllocator.nextSubtask())
            .thenReturn(new SubtaskAllocation(ResponseType.NO_MORE, -1));

        Response response = subtaskClient.nextSubtask();
        assertTrue(SubtaskServer.getRequestQueue().isEmpty());
        assertEquals(ResponseType.NO_MORE, response.status);
        assertEquals(-1, response.subtaskIndex);

        Mockito.verify(subtaskAllocator).nextSubtask();
    }

    @Test
    public void testReportDone() throws InterruptedException {
        subtaskServer.start();

        Response response = subtaskClient.reportSubtaskComplete(10);

        assertTrue(SubtaskServer.getRequestQueue().isEmpty());
        assertEquals(ResponseType.OK, response.status);
        assertEquals(-1, response.subtaskIndex);

        Mockito.verify(subtaskAllocator).markSubtaskComplete(10);
    }

    @Test
    public void testReportLocked() throws InterruptedException {
        subtaskServer.start();

        Response response = subtaskClient.reportSubtaskLocked(10);

        assertTrue(SubtaskServer.getRequestQueue().isEmpty());
        assertEquals(ResponseType.OK, response.status);
        assertEquals(-1, response.subtaskIndex);

        Mockito.verify(subtaskAllocator).markSubtaskLocked(10);
    }

    @Test
    public void testInterrupt() throws InterruptedException {
        subtaskServer.start();
        subtaskServer.getListenerThread().interrupt();
        assertTrue(TestEventDetector.detectTestEvent(1000L,
            () -> subtaskServer.getListenerThread().getState().equals(Thread.State.TERMINATED)));
    }
}
