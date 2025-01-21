package gov.nasa.ziggy.services.messaging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <li>The {@link ZiggyMessenger#publish(PipelineMessage)} causes a {@link PipelineMessage} to be
 * dispatched. If the process hosting the {@link ZiggyMessenger} has an initialized instance of
 * {@link ZiggyRmiClient}, the message is sent to all Ziggy RMI clients via the
 * {@link ZiggyRmiClient#send(PipelineMessage, CountDownLatch)} method; otherwise, the subscriptions
 * in the current process are used to determine if any action needs to be taken locally. Note that
 * messages that are sent via RMI are then received by all clients, so
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
    private static ZiggyMessenger instance = new ZiggyMessenger();

    /**
     * Blocking queue for outgoing messages. Messages wait here until the singleton instance is free
     * to deal with them, at which time they get sent from the RMI client to the RMI server.
     */
    private LinkedBlockingQueue<Message> outgoingMessageQueue = new LinkedBlockingQueue<>();

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
    private Map<Class<? extends PipelineMessage>, Collection<MessageAction<?>>> subscriptions = new ConcurrentHashMap<>();

    /**
     * In some cases it may be necessary to get confirmation that a message has actually gone out
     * before some other action is taken. For that reason we have a {@link Map} between messages and
     * instances of {@link CountDownLatch}.
     */
    private Map<PipelineMessage, CountDownLatch> messageCountdownLatches = new ConcurrentHashMap<>();

    /**
     * Determines whether messages are stored when they are sent out from the message queue. Use
     * {@link #getMessagesFromOutgoingQueue()} to retrieve them. For testing only.
     */
    private boolean storeMessages;

    /** For testing only. */
    private List<PipelineMessage> messagesFromOutgoingQueue = new ArrayList<>();

    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    private ZiggyMessenger() {
        startOutgoingMessageThread();
    }

    private void startOutgoingMessageThread() {
        outgoingMessageThread = new Thread(() -> {
            Thread.currentThread().setName("Message Publisher");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    log.debug("Waiting for message to send");
                    Message message = outgoingMessageQueue.take();
                    if (storeMessages) {
                        messagesFromOutgoingQueue.add(message.getPipelineMessage());
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

    private void publishMessage(Message message) {
        CountDownLatch latch = messageCountdownLatches.remove(message.getPipelineMessage());
        if (ZiggyRmiClient.isInitialized() && message.isBroadcastOverRmi()) {
            log.debug("Sending message {}", message);
            ZiggyRmiClient.send(message.getPipelineMessage(), latch);
            log.debug("Sending message {}...done", message);
        } else {
            takeAction(message.getPipelineMessage());
        }
        if (latch != null) {
            latch.countDown();
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends PipelineMessage> void takeAction(T message) {
        Collection<MessageAction<?>> actions = subscriptions.get(message.getClass());
        if (CollectionUtils.isEmpty(actions)) {
            log.debug("No subscribers for {}", message);
            return;
        }
        for (MessageAction<?> action : actions) {
            log.debug("Dispatching {} to {}", message, action.getClass().getSimpleName());
            ((MessageAction<T>) action).action(message);
            log.debug("Dispatching {} to {}...done", message, action.getClass().getSimpleName());
        }
    }

    /**
     * Subscribes to a subclass of {@link PipelineMessage}.
     */
    public static <T extends PipelineMessage> void subscribe(Class<T> messageClass,
        MessageAction<T> action) {
        instance.addSubscription(messageClass, action);
    }

    private <T extends PipelineMessage> void addSubscription(Class<T> messageClass,
        MessageAction<T> action) {
        if (subscriptions.get(messageClass) == null) {
            subscriptions.put(messageClass, new ConcurrentLinkedQueue<>());
        }
        subscriptions.get(messageClass).add(action);
    }

    /**
     * Unsubscribes to a subclass of {@link PipelineMessage}.
     */
    public static <T extends PipelineMessage> void unsubscribe(Class<T> messageClass,
        MessageAction<T> action) {
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
     */
    public static void publish(PipelineMessage message) {
        publish(message, true, null);
    }

    public static void publish(PipelineMessage message, boolean broadcastOverRmi) {
        publish(message, broadcastOverRmi, null);
    }

    public static void publish(PipelineMessage message, CountDownLatch latch) {
        publish(message, true, latch);
    }

    /**
     * Publishes a message via the {@link ZiggyMessenger} singleton, and holds onto a
     * {@link CountDownLatch} for the message. The latch is quietly ignored if null.
     */
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public static void publish(PipelineMessage message, boolean broadcastOverRmi,
        CountDownLatch latch) {

        try {
            if (latch != null) {
                instance.messageCountdownLatches.put(message, latch);
            }
            instance.outgoingMessageQueue.put(new Message(message, broadcastOverRmi));
            log.debug("Added {} to outgoing queue ({} messages)", message,
                instance.outgoingMessageQueue.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Acts on a message via the {@link ZiggyMessenger} singleton. Package scoped because only
     * {@link ZiggyRmiClient} should use this method.
     */
    static void actOnMessage(PipelineMessage message) {
        instance.takeAction(message);
    }

    /** For testing only. */
    static void reset() {
        instance.outgoingMessageThread.interrupt();
        getOutgoingMessageQueue().clear();
        getSubscriptions().clear();
        setStoreMessages(false);
        instance.messagesFromOutgoingQueue.clear();
        instance.messageCountdownLatches.clear();
        instance.startOutgoingMessageThread();
    }

    /** For testing only. */
    static Map<Class<? extends PipelineMessage>, Collection<MessageAction<?>>> getSubscriptions() {
        return instance.subscriptions;
    }

    /** For testing only. */
    static LinkedBlockingQueue<Message> getOutgoingMessageQueue() {
        return instance.outgoingMessageQueue;
    }

    /** For testing only. */
    static void setStoreMessages(boolean storeMessages) {
        instance.storeMessages = storeMessages;
    }

    /** For testing only. */
    static List<PipelineMessage> getMessagesFromOutgoingQueue() {
        return instance.messagesFromOutgoingQueue;
    }

    private static class Message {

        private final PipelineMessage pipelineMessage;
        private final boolean broadcastOverRmi;

        public Message(PipelineMessage pipelineMessage, boolean broadcastOverRmi) {
            this.pipelineMessage = pipelineMessage;
            this.broadcastOverRmi = broadcastOverRmi;
        }

        public PipelineMessage getPipelineMessage() {
            return pipelineMessage;
        }

        public boolean isBroadcastOverRmi() {
            return broadcastOverRmi;
        }

        @Override
        public String toString() {
            return pipelineMessage.toString();
        }
    }
}
