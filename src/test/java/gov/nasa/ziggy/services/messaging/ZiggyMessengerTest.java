package gov.nasa.ziggy.services.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messaging.MessagingTestUtils.Message1;
import gov.nasa.ziggy.services.messaging.MessagingTestUtils.Message2;

/**
 * Unit tests for the {@link ZiggyMessenger} class.
 *
 * @author PT
 */
public class ZiggyMessengerTest {

    private final List<String> stringsFromMessages = new ArrayList<>();

    @After
    public void tearDown() {
        stringsFromMessages.clear();
        ZiggyMessenger.reset();
    }

    /**
     * Test that subscriptions are properly managed and that multiple subscriptions for a single
     * message class is supported.
     */
    @Test
    public void testSubscribe() {

        ZiggyMessenger.subscribe(Message1.class, message -> {
            // Do nothing.
        });
        ZiggyMessenger.subscribe(Message1.class, message -> {
            // Still do nothing.
        });
        ZiggyMessenger.subscribe(Message2.class, message -> {
            // Even more nothing is done.
        });

        Map<Class<? extends PipelineMessage>, Collection<MessageAction<?>>> subscriptions = ZiggyMessenger
            .getSubscriptions();
        assertEquals(2, subscriptions.size());
        assertEquals(2, subscriptions.get(Message1.class).size());
        assertEquals(1, subscriptions.get(Message2.class).size());
    }

    /**
     * Test the basic {@link ZiggyMessenger#publish(PipelineMessage)} method.
     */
    @Test
    public void testPublishNoCountdownLatch() {
        ZiggyMessenger.setStoreMessages(true);

        ZiggyMessenger.publish(new Message1("signed"));
        TestEventDetector.detectTestEvent(1000L,
            () -> (ZiggyMessenger.getMessagesFromOutgoingQueue().size() > 0));
        assertTrue(ZiggyMessenger.getOutgoingMessageQueue().isEmpty());
        assertEquals(1, ZiggyMessenger.getMessagesFromOutgoingQueue().size());
        assertTrue(ZiggyMessenger.getMessagesFromOutgoingQueue().get(0) instanceof Message1);
    }

    /**
     * Test the {@link ZiggyMessenger#publish(PipelineMessage, CountDownLatch)} method, which allows
     * the user to pair a {@link CountDownLatch} with the {@link PipelineMessage}, such that the
     * CountDownLatch gets decremented when the message is acted upon.
     */
    @Test
    public void testPublishWithCountdownLatch() {
        ZiggyMessenger.setStoreMessages(true);

        CountDownLatch latch = new CountDownLatch(1);
        ZiggyMessenger.publish(new Message1("signed"), latch);
        assertTrue(TestEventDetector.detectTestEvent(1000L,
            () -> (ZiggyMessenger.getMessagesFromOutgoingQueue().size() > 0)));
        assertTrue(TestEventDetector.detectTestEvent(1000L, () -> (latch.getCount() == 0)));
        assertTrue(ZiggyMessenger.getOutgoingMessageQueue().isEmpty());
        assertEquals(1, ZiggyMessenger.getMessagesFromOutgoingQueue().size());
        assertTrue(ZiggyMessenger.getMessagesFromOutgoingQueue().get(0) instanceof Message1);
    }

    private static final Logger log = LoggerFactory.getLogger(ZiggyMessengerTest.class);

    @Test
    public void testTakeAction() {
        log.info("Start");
        ZiggyMessenger.setStoreMessages(true);

        ZiggyMessenger.subscribe(Message1.class, message -> {
            stringsFromMessages.add(message.getPayload());
        });
        ZiggyMessenger.subscribe(Message1.class, message -> {
            stringsFromMessages.add("sealed");
        });
        ZiggyMessenger.subscribe(Message2.class, message -> {
            stringsFromMessages.add(message.getPayload());
        });

        ZiggyMessenger.publish(new Message1("signed"));
        ZiggyMessenger.publish(new Message2("delivered"));
        TestEventDetector.detectTestEvent(1000L, () -> (stringsFromMessages.size() > 2));
        assertEquals(2, ZiggyMessenger.getMessagesFromOutgoingQueue().size());
        assertEquals(3, stringsFromMessages.size());
        assertTrue(stringsFromMessages.contains("signed"));
        assertTrue(stringsFromMessages.contains("sealed"));
        assertTrue(stringsFromMessages.contains("delivered"));
    }
}
