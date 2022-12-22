package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.module.SubtaskServer.Request;
import gov.nasa.ziggy.module.SubtaskServer.RequestType;
import gov.nasa.ziggy.module.SubtaskServer.Response;
import gov.nasa.ziggy.module.SubtaskServer.ResponseType;

/**
 * Unit tests for the {@link SubtaskClient} class.
 *
 * @author PT
 */
public class SubtaskClientTest {

    private SubtaskClient subtaskClient;

    // We need to do some of the SubtaskClient work in a separate thread because the
    // client blocks until it receives a reply from the server.
    private ExecutorService threadPool;
    private ArrayBlockingQueue<Request> requestQueue;

    @Before
    public void setUp() {
        subtaskClient = new SubtaskClient();
        threadPool = Executors.newSingleThreadExecutor();
        requestQueue = SubtaskServer.getRequestQueue();
    }

    @After
    public void tearDown() {
        threadPool.shutdownNow();
        requestQueue.clear();
    }

    @Test
    public void testReportSubtaskComplete() throws InterruptedException, ExecutionException {

        assertTrue(requestQueue.isEmpty());

        // Use the other thread to send the report.
        Future<Response> response = threadPool
            .submit(() -> subtaskClient.reportSubtaskComplete(100));

        // Check that the report showed up in the queue.
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> !requestQueue.isEmpty()));
        Request request = requestQueue.take();
        assertEquals(subtaskClient, request.client);
        assertEquals(RequestType.REPORT_DONE, request.type);
        assertEquals(100, request.subtaskIndex);

        // Send a response.
        subtaskClient.submitResponse(new Response(ResponseType.OK));

        // See whether the response landed.
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> response.isDone()));
        Response responseContent = response.get();
        assertEquals(ResponseType.OK, responseContent.status);

    }

    @Test
    public void testReportSubtaskLocked() throws InterruptedException, ExecutionException {

        assertTrue(requestQueue.isEmpty());

        // Use the other thread to send the report.
        Future<Response> response = threadPool.submit(() -> subtaskClient.reportSubtaskLocked(100));

        // Check that the report showed up in the queue.
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> !requestQueue.isEmpty()));
        Request request = requestQueue.take();
        assertEquals(subtaskClient, request.client);
        assertEquals(RequestType.REPORT_LOCKED, request.type);
        assertEquals(100, request.subtaskIndex);

        // Send a response.
        subtaskClient.submitResponse(new Response(ResponseType.OK));

        // See whether the response landed.
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> response.isDone()));
        Response responseContent = response.get();
        assertEquals(ResponseType.OK, responseContent.status);

    }

    @Test
    public void testNextSubtask() throws InterruptedException, ExecutionException {

        assertTrue(requestQueue.isEmpty());

        // Use the other thread to send the request.
        Future<Response> response = threadPool.submit(() -> subtaskClient.nextSubtask());

        // Check that the report showed up in the queue.
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> !requestQueue.isEmpty()));
        Request request = requestQueue.take();
        assertEquals(subtaskClient, request.client);
        assertEquals(RequestType.GET_NEXT, request.type);
        assertEquals(-1, request.subtaskIndex);

        // Send back a "try again" response.
        subtaskClient.submitResponse(new Response(ResponseType.TRY_AGAIN));

        // There should be another request submitted
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> !requestQueue.isEmpty()));
        request = requestQueue.take();
        assertEquals(subtaskClient, request.client);
        assertEquals(RequestType.GET_NEXT, request.type);
        assertEquals(-1, request.subtaskIndex);

        // The client should still be waiting.
        assertFalse(response.isDone());

        // Now send a response that supplies an actual subtask for the client.
        subtaskClient.submitResponse(new Response(ResponseType.OK, 100));

        // See whether the response landed.
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> response.isDone()));
        Response responseContent = response.get();
        assertEquals(ResponseType.OK, responseContent.status);
        assertEquals(100, responseContent.subtaskIndex);
    }

    @Test
    public void testNextSubtaskNoMore() throws InterruptedException, ExecutionException {

        assertTrue(requestQueue.isEmpty());

        // Use the other thread to send the request.
        Future<Response> response = threadPool.submit(() -> subtaskClient.nextSubtask());

        // Send back a "no more" response.
        subtaskClient.submitResponse(new Response(ResponseType.NO_MORE));

        // See whether the response landed.
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> response.isDone()));
        Response responseContent = response.get();
        assertEquals(ResponseType.NO_MORE, responseContent.status);
    }

    @Test
    public void testInterruption() throws ExecutionException, InterruptedException {

        // Use the other thread to send the request.
        Future<Response> response = threadPool.submit(() -> subtaskClient.nextSubtask());

        // Check that the report showed up in the queue.
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> !requestQueue.isEmpty()));

        assertFalse(response.isDone());

        // Attempt to interrupt the method, which is blocked waiting for a reply.
        threadPool.shutdownNow();
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> response.isDone()));

        try {
            response.get();
            assertFalse("ExecutionException did not occur", true);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            StackTraceElement[] stackTrace = t.getStackTrace();
            assertEquals(InterruptedException.class, t.getClass());

            // Make sure the interrupt was in the correct location.
            boolean correctInterruptLocation = false;
            for (StackTraceElement element : stackTrace) {
                if (element.getClassName().equals("gov.nasa.ziggy.module.SubtaskClient")
                    && element.getMethodName().equals("receive")) {
                    correctInterruptLocation = true;
                    break;
                }
            }
            assertTrue(correctInterruptLocation);
        }
    }

    @Test
    public void testInterruptBeforeSendingRequest() throws InterruptedException {

        // Here we want to do something peculiar, to wit: we want the SubtaskClient
        // thread to wait for the interrupt to appear, and then try to submit a
        // request to the server!
        Future<Response> response = threadPool.submit(() -> {
            TestEventDetector.detectTestEvent(1000L, () -> Thread.currentThread().isInterrupted());
            return subtaskClient.nextSubtask();
        });
        threadPool.shutdownNow();

        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> response.isDone()));
        assertTrue(requestQueue.isEmpty());

        try {
            response.get();
            assertFalse("ExecutionException did not occur", true);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            StackTraceElement[] stackTrace = t.getStackTrace();
            assertEquals(InterruptedException.class, t.getClass());

            // Make sure the interrupt was in the correct location.
            boolean correctInterruptLocation = false;
            for (StackTraceElement element : stackTrace) {
                if (element.getClassName().equals("gov.nasa.ziggy.module.SubtaskClient")
                    && element.getMethodName().equals("request")) {
                    correctInterruptLocation = true;
                    break;
                }
            }
            assertTrue(correctInterruptLocation);
        }
    }
}
