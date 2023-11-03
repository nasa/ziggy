package gov.nasa.ziggy.services.messaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Performs the actions needed within a process that relate to messages and messaging.
 * <p>
 * The {@link ZiggyMessenger} holds the main responsibility for all messages that are sent or
 * received by a process. This is accomplished through two primary methods:
 * <ol>
 * <li>The {@link ZiggyMessenger#subscribe(Class, MessageAction)} allows a user to specify an action
 * that must be taken when a message of a specified class is received.
 * <li>The {@link ZiggyMessenger#publish(PipelineMessage) causes a {@link PipelineMessage} to be
 * dispatched. If the process hosting the {@link ZiggyMessenger} has an initialized instance of
 * {@link ZiggyRmiClient}, the message is sent to all Ziggy RMI clients via the
 * {@link ZiggyRmiClient#send(PipelineMessage)} method; otherwise, the subscriptions in the current
 * process are used to determine if any action needs to be taken locally. Note that messages that
 * are sent via RMI are then received by all clients, so
 * {@link ZiggyMessenger#publish(PipelineMessage)} always works for messages from one activity in a
 * process to another activity in the same process.
 * </ol>
 * <p>
 * The {@link ZiggyMessenger} is created as a singleton instance in each process. Static methods are
 * provided to allow users to send publish or subscribe requests to the singleton.
 *
 * @author PT
 */
public class ZiggyMessenger {

    private static final Logger log = LoggerFactory.getLogger(ZiggyMessenger.class);

    /**
     * The singleton instance for a process.
     */
    private static ZiggyMessenger instance;

    /**
     * Blocking queue for outgoing messages. Messages wait here until the singleton instance is free
     * to deal with them, at which time they get sent from the RMI client to the RMI server.
     */
    private static LinkedBlockingQueue<PipelineMessage> outgoingMessageQueue = new LinkedBlockingQueue<>();

    /**
     * For testing only.
     */
    static List<PipelineMessage> messagesFromOutgoingQueue = new ArrayList<>();

    /**
     * Dedicated thread for pulling messages off the blocking queue and giving them to the
     * {@link ZiggyMessenger#publishMessage(PipelineMessage)} instance method.
     * <p>
     * The issue is that if a message requires an action that dispatches another message, there's
     * potential for a deadlock of the {@link ZiggyMessenger} singleton instance: the
     * {@link #takeAction(PipelineMessage) can't return until after the
     * {@link #publishMessage(PipelineMessage)} call returns, but that call may be blocked from
     * executing because the instance is waiting for the {@link #takeAction(PipelineMessage)} to
     * return before it can do anything else. By making the static {@link #publish(PipelineMessage)}
     * message put the message onto the blocking queue, that call returns immediately, which means
     * that the instance call to {@link #takeAction(PipelineMessage)} can also return.
     */
    private Thread outgoingMessageThread;

    /**
     * Stores the mapping between a message class and the actions that must be taken when such a
     * message arrives.
     */
    private Map<Class<? extends PipelineMessage>, List<MessageAction<?>>> subscriptions = new ConcurrentHashMap<>();

    /**
     * In some cases it may be necessary to get confirmation that a message has actually gone out
     * before some other action is taken. For that reason we have a {@link Map} between messages and
     * instances of {@link CountDownLatch}.
     */
    private Map<PipelineMessage, CountDownLatch> messageCountdownLatches = new HashMap<>();

    /**
     * Determines whether messages are stored when they are sent out from the message queue. For
     * testing only.
     */
    private boolean storeMessages;

    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    private ZiggyMessenger() {
        outgoingMessageThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    PipelineMessage message = outgoingMessageQueue.take();
                    if (storeMessages) {
                        messagesFromOutgoingQueue.add(message);
                    }
                    publishMessage(message);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        outgoingMessageThread.setDaemon(true);
        outgoingMessageThread.start();
    }

    private void publishMessage(PipelineMessage message) {
        CountDownLatch latch = messageCountdownLatches.get(message);
        messageCountdownLatches.remove(message);
        if (ZiggyRmiClient.isInitialized()) {
            log.debug("Sending message of " + message.getClass().toString());
            ZiggyRmiClient.send(message, latch);
        } else {
            takeAction(message);
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends PipelineMessage> void takeAction(T message) {
        List<MessageAction<?>> actions = subscriptions.get(message.getClass());
        if (CollectionUtils.isEmpty(actions)) {
            return;
        }
        for (MessageAction<?> action : actions) {
            log.debug("Applying action for message " + message.getClass().toString());
            ((MessageAction<T>) action).action(message);
        }
    }

    // Package scope for testing.
    static void initializeInstance() {
        if (!isInitialized()) {
            log.info("Initializing ZiggyMessenger singleton");
            instance = new ZiggyMessenger();
        }
    }

    private static boolean isInitialized() {
        return instance != null;
    }

    /**
     * Subscribes to a subclass of {@link PipelineMessage}.
     */
    public static <T extends PipelineMessage> void subscribe(Class<T> messageClass,
        MessageAction<T> action) {
        if (!isInitialized()) {
            initializeInstance();
        }
        instance.addSubscription(messageClass, action);
    }

    private <T extends PipelineMessage> void addSubscription(Class<T> messageClass,
        MessageAction<T> action) {
        if (subscriptions.get(messageClass) == null) {
            subscriptions.put(messageClass, new ArrayList<>());
        }
        subscriptions.get(messageClass).add(action);
    }

    /**
     * Unsubscribes to a subclass of {@link PipelineMessage}.
     */
    public static <T extends PipelineMessage> void unsubscribe(Class<T> messageClass,
        MessageAction<T> action) {
        if (!isInitialized()) {
            return;
        }
        instance.removeSubscription(messageClass, action);
    }

    private <T extends PipelineMessage> void removeSubscription(Class<T> messageClass,
        MessageAction<T> action) {
        if (subscriptions.get(messageClass) == null) {
            return;
        }
        subscriptions.get(messageClass).remove(action);
    }

    /**
     * Publishes a message via the {@link ZiggyMessenger} singleton.
     *
     * @throws InterruptedException
     */
    public static void publish(PipelineMessage message) {
        publish(message, null);
    }

    /**
     * Publishes a message via the {@link ZiggyMessenger} singleton, and holds onto a
     * {@link CountDownLatch} for the message.
     */
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public static void publish(PipelineMessage message, CountDownLatch latch) {
        if (!isInitialized()) {
            initializeInstance();
        }
        try {
            if (latch != null) {
                instance.messageCountdownLatches.put(message, latch);
            }
            outgoingMessageQueue.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Acts on a message via the {@link ZiggyMessenger} singleton. If the singleton has not been
     * initialized, a {@link PipelineException} will be thrown. Package scoped because only
     * {@link ZiggyRmiClient} should use this method.
     *
     * @param message
     */
    static void actOnMessage(PipelineMessage message) {
        if (!isInitialized()) {
            throw new PipelineException("Unable to act on message of "
                + message.getClass().toString() + " due to absence of ZiggyMessenger instance");
        }
        log.debug("Taking action on message of " + message.getClass().toString());
        instance.takeAction(message);
    }

    /** For testing only. */
    static void reset() {
        instance.outgoingMessageThread.interrupt();
        instance = null;
        messagesFromOutgoingQueue.clear();
    }

    /** For testing only. */
    static Map<Class<? extends PipelineMessage>, List<MessageAction<?>>> getSubscriptions() {
        return instance.subscriptions;
    }

    /** For testing only. */
    static LinkedBlockingQueue<PipelineMessage> getOutgoingMessageQueue() {
        return ZiggyMessenger.outgoingMessageQueue;
    }

    /** For testing only. */
    static void setStoreMessages(boolean storeMessages) {
        instance.storeMessages = storeMessages;
    }
}
