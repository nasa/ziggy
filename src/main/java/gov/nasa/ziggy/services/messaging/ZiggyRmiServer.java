package gov.nasa.ziggy.services.messaging;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.SystemProxy;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Server component of the hub-and-spoke communication between Ziggy processes. The server
 * establishes the RMI registry used for the communication, if it does not yet exist. It also
 * provides a method that broadcasts messages to all clients, each of which supplies a
 * {@link ZiggyRmiClientService} stub when it initializes its own {@link ZiggyRmiClient} instance.
 * <p>
 * The singleton {@link ZiggyRmiServer} is created when the {@link PipelineSupervisor} is created.
 * At the same time a new {@link Thread} is created that periodically broadcasts instances of the
 * {@link HeartbeatMessage} to all clients. Both that thread and the {@link ZiggyRmiServer} instance
 * are destroyed when the {@link PipelineSupervisor} exits. The shutdown will also attempt to shut
 * down the RMI registry (see below).
 * <p>
 * In the event that the {@link PipelineSupervisor} crashes, a new instance of the latter will be
 * created, along with new instances of the {@link ZiggyRmiServer} and the heartbeat thread. In this
 * case, since the RMI registry is (probably) still running, the new instance of
 * {@link ZiggyRmiServer} will attempt to connect to the registry and update the contents of same.
 * In this event, shutting down the {@link ZiggyRmiServer} will not shut down the RMI registry; it
 * is impossible to shut down a registry if the process that created it has been lost. Fortunately,
 * shutting down the supervisor will eventually cause the registry to shut down as well.
 * <p>
 * All of the fields in {@link ZiggyRmiServer} are either final or collections that support
 * concurrency, hence the {@link ZiggyRmiServer} instance is thread-safe.
 *
 * @author PT
 */
public class ZiggyRmiServer implements ZiggyRmiServerService {

    private static Logger log = LoggerFactory.getLogger(ZiggyRmiServer.class);
    public static final int RMI_PORT_DEFAULT = 1099;
    public static final long MAX_WAIT_FOR_QUEUE_CLEARANCE_DURING_SHUTDOWN_MILLIS = 1000L;

    /**
     * Singleton instance of {@link ZiggyRmiServer} so all threads in the supervisor process can
     * make use of the initialized communicator.
     */
    private static ZiggyRmiServer instance;

    // This Map is used for test purposes. It allows the unit tests to determine how many
    // messages showed up, and of which classes.
    private static Map<Class<? extends PipelineMessage>, Integer> messagesReceived = new ConcurrentHashMap<>();

    /**
     * Collection of client stubs. Each stub is provided with a {@link Thread} that is used for
     * outgoing messages.
     */
    private Set<RmiClientThread> clientThreads = Collections
        .newSetFromMap(new ConcurrentHashMap<>());

    private final Registry registry;
    private final boolean registryCreated;
    private final Thread broadcastThread;

    /** Queue for messages that are received from clients and awaiting broadcast to all clients. */
    private LinkedBlockingQueue<PipelineMessage> messageQueue = new LinkedBlockingQueue<>();

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private ZiggyRmiServer() throws RemoteException {

        // Try to create the registry. If that fails due to ExportException, then this
        // is a new SupervisorCommunicator started on a system where the supervisor crashed and
        // left the registry running; in that case, we need the registry so that we can
        // clean up services installed by the prior instance and then rebind new ones
        // to the registry.
        boolean constructorCreatedRegistry = false;
        Registry createdOrRetrievedRegistry = null;
        try {
            log.info("Creating RMI registry");
            createdOrRetrievedRegistry = LocateRegistry.createRegistry(rmiPort());
            constructorCreatedRegistry = true;
        } catch (ExportException ignored) {
            // This just means that the registry already exists, but there's no
            // way to know that other than trying to create it and failing.
            log.info("Retrieving registry on localhost");
            createdOrRetrievedRegistry = LocateRegistry.getRegistry("localhost", rmiPort());
        }
        registry = createdOrRetrievedRegistry;
        registryCreated = constructorCreatedRegistry;
        if (!registryCreated) {
            cleanUpRegistry();
        }

        // Start the background thread that pulls messages off the queue and broadcasts them.
        broadcastThread = new Thread(() -> {
            broadcastNextMessage();
        });
        broadcastThread.setDaemon(true);
        broadcastThread.start();
    }

    /**
     * Constructs an instance of the class and assigns it as the singleton instance. This includes
     * construction of the RMI registry.
     * <P>
     * The {@link ZiggyRmiServer} needs to provide two capabilities to client instances:
     * <ol>
     * <li>The clients must be able to submit themselves, as stubs, to the server, such that the
     * server can send messages to the clients via the
     * {@link ZiggyRmiClientService#takeMessageActionInClient(PipelineMessage)} method. This is
     * accomplished by the {@link #addClientStub(ZiggyRmiClientService)} method.
     * <li>The clients must be able to send messages to the server, which the server then transmits
     * back to all clients. This is accomplished by the {@link #transmitToServer(PipelineMessage)}
     * method.
     * </ol>
     * Both of the capabilities described above are achieved by exporting the server, as an instance
     * of {@link ZiggyRmiServerService}, to the RMI registry.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void start() {

        if (instance != null) {
            log.info("ZiggyRmiServer instance already available, skipping instantiation");
            return;
        }

        log.info("Starting RMI communications server with registry on port {}", rmiPort());

        try {
            ZiggyRmiServer serverInstance = new ZiggyRmiServer();

            log.info("Exporting and binding objects into registry");
            ZiggyRmiServerService commStub = (ZiggyRmiServerService) UnicastRemoteObject
                .exportObject(serverInstance, 0);
            serverInstance.registry.rebind(ZiggyRmiServerService.SERVICE_NAME, commStub);

            ZiggyShutdownHook.addShutdownHook(() -> {
                log.info("SHUTDOWN: ZiggyRmiServer...");
                ZiggyRmiServer.shutdown(true);
                log.info("SHUTDOWN: ZiggyRmiServer...done");
            });
            instance = serverInstance;
        } catch (RemoteException e) {
            throw new PipelineException(
                "Exception occurred when attempting to initialize ZiggyRmiServer", e);
        }

        log.info("Starting RMI communications server with registry on port {}...done", rmiPort());
    }

    /**
     * Remove obsolete bound and/or exported instances of the supervisor communicator and message
     * handler services. This is only necessary when a new SupervisorCommunicator is being
     * constructed to replace one that crashed, or during shutdown.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void cleanUpRegistry() {
        try {
            log.info("Cleaning up bound / exported registry instances");
            List<String> registryServiceNames = Arrays.asList(registry.list());
            if (registryServiceNames.contains(ZiggyRmiServerService.SERVICE_NAME)) {
                registry.unbind(ZiggyRmiServerService.SERVICE_NAME);
            }
        } catch (RemoteException | NotBoundException e) {
            throw new PipelineException("Unable to unbind services from RMI registry", e);
        }
    }

    /**
     * Adds a new {@link ZiggyRmiClientService} stub from a client to the set of same. When a client
     * initializes its {@link ZiggyRmiClient} instance, that instance uses an RMI call that causes
     * the {@link ZiggyRmiServer} to add its stub to the set. It also sends an immediate heartbeat
     * message to the client so that the client can confirm that communication has been started
     * correctly.
     */
    @Override
    public void addClientStub(ZiggyRmiClientService clientStub) throws RemoteException {
        log.info("Adding RMI client {}", clientStub.clientName());
        RmiClientThread clientInformation = new RmiClientThread(clientStub,
            clientStub.clientName());
        clientInformation.start();
        clientInformation.addMessage(new HeartbeatMessage());
        clientThreads.add(clientInformation);
    }

    /**
     * Accepts a message from one of the {@link ZiggyRmiClient} instances and adds it to the queue
     * of messages for broadcast.
     */
    @Override
    public void transmitToServer(PipelineMessage message) throws RemoteException {
        if (!messagesReceived.containsKey(message.getClass())) {
            messagesReceived.put(message.getClass(), 0);
        }
        int messageCount = messagesReceived.get(message.getClass());
        messagesReceived.put(message.getClass(), ++messageCount);
        addToBroadcastQueue(message);
    }

    /**
     * Adds a single message to the broadcast queue.
     */
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public synchronized static void addToBroadcastQueue(PipelineMessage message) {
        try {
            instance.messageQueue.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Takes the next message off the message queue and broadcasts it to all clients.
     * <p>
     * The message queue is a {@link LinkedBlockingQueue}, such that the act of taking the next
     * message off the queue blocks if the queue is empty. In consequence, the
     * {@link ZiggyRmiServer} requires a dedicated {@link Thread} to take the messages off the queue
     * and broadcast them.
     * <p>
     * The broadcast element of the method is accomplished by looping over the stubs from the
     * {@link ZiggyRmiClient} instances and attempting to use the {@link MessageAction} instances in
     * each client to perform any action required by the message. If a client lacks an appropriate
     * {@link MessageAction} for a given message class, the message is ignored.
     * <p>
     * The {@link #broadcastNextMessage()} method also prunes any obsolete message stubs from its
     * collection of same. This is made possible by the fact that any client stub that connects to a
     * client that has shut down results in a {@link RemoteException}. Any stub that causes a
     * {@link RemoteException} is recorded; at the end of the loop over stubs, any such stub is
     * removed from the collection.
     */
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void broadcastNextMessage() {
        while (true) {
            PipelineMessage message;
            try {
                message = messageQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            log.debug("Broadcasting {} to {} clients", message.getClass().getName(),
                clientThreads.size());
            Set<RmiClientThread> obsoleteStubs = new HashSet<>();
            for (RmiClientThread clientService : clientThreads) {
                if (clientService.isClientDisconnected()) {
                    obsoleteStubs.add(clientService);
                    continue;
                }
                clientService.addMessage(message);
            }

            for (RmiClientThread stub : obsoleteStubs) {
                log.debug("Removing RMI client {} ({} remain)", stub.getClientName(),
                    clientThreads.size() - 1);
                clientThreads.remove(stub);
            }
        }
    }

    /**
     * Nullifies the singleton instance of the communicator.
     */
    public static void reset() {
        instance = null;
        messagesReceived.clear();
    }

    public static boolean isInitialized() {
        return instance != null;
    }

    public static void shutdown() {
        shutdown(true);
    }

    /**
     * Performs the most orderly shutdown possible of the server and the registry.
     * <p>
     * In all circumstances, the singleton instance of the server will be nullified, the services
     * will be unbound from the registry, and exported objects will be unexported. In addition, if
     * this is the instance of {@link ZiggyRmiServerService} that created the registry, the registry
     * will be unexported. Unfortunately, if the registry was created by a now-crashed instance of
     * the server, it will be necessary for the JVM to shut it down when it destroys all daemon
     * threads.
     */
    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public static synchronized void shutdown(boolean force) {
        try {
            if (instance != null) {
                // Wait for up to 1 second for the message queues to empty and the
                // broadcast threads to block while waiting for a new message.
                // This ensures that the clients can receive the shutdown message.
                long startWaitTime = SystemProxy.currentTimeMillis();
                while (SystemProxy.currentTimeMillis()
                    - startWaitTime < MAX_WAIT_FOR_QUEUE_CLEARANCE_DURING_SHUTDOWN_MILLIS) {
                    if (isAllMessagingComplete()) {
                        break;
                    }
                }
                log.info("SHUTDOWN: Unbinding / unexporting services");
                instance.cleanUpRegistry();

                // Unexport the ZiggyRmiServer singleton so that the registry will be empty
                UnicastRemoteObject.unexportObject(instance, force);

                // Attempt to unexport the registry if and only if this is the instance of
                // ZiggyRmiServer that created it
                if (instance.registryCreated) {
                    log.info("SHUTDOWN: Unexporting registry");
                    UnicastRemoteObject.unexportObject(instance.registry, force);
                }
            }
        } catch (RemoteException e) {
            // Since this method is called in the process of an orderly shutdown, there's not
            // actually anything we can do if exceptions are thrown.
        }
        reset();
        log.info("SHUTDOWN: ZiggyRmiServer shutdown complete");
    }

    /**
     * Returns the RMI registry. For testing only.
     *
     * @return
     */
    static Registry getRegistry() {
        return instance.registry;
    }

    /**
     * Returns the client service stubs. For testing only.
     */
    static Set<RmiClientThread> getClientServiceStubs() {
        return instance.clientThreads;
    }

    /**
     * Returns the singleton instance. For testing only.
     */
    static ZiggyRmiServer serverInstance() {
        return instance;
    }

    /**
     * Returns statistics on the messages received and rebroadcast. For testing only.
     */
    static Map<Class<? extends PipelineMessage>, Integer> messagesReceived() {
        return messagesReceived;
    }

    /**
     * Checks the state of the broadcast queues and messaging threads.
     */
    static boolean isAllMessagingComplete() {
        boolean messagingComplete = instance.messageQueue.isEmpty()
            && instance.broadcastThread.getState().equals(Thread.State.WAITING);
        if (!messagingComplete) {
            return false;
        }
        for (RmiClientThread client : instance.clientThreads) {
            messagingComplete = messagingComplete && client.getOutgoingMessageQueue().isEmpty()
                && client.outgoingMessageThreadState().equals(Thread.State.WAITING);
            if (!messagingComplete) {
                return false;
            }
        }
        return true;
    }

    static int rmiPort() {
        return ZiggyConfiguration.getInstance()
            .getInt(PropertyName.SUPERVISOR_PORT.property(), ZiggyRmiServer.RMI_PORT_DEFAULT);
    }

    /**
     * Provides a separate {@link Thread} for each client to use for outgoing messages. This ensures
     * that if one client freezes or is waiting to time out, it will not block messages that go out
     * to other clients.
     * <p>
     * The class provides a {@link LinkedBlockingQueue} for messages, which the broadcast system
     * populates with new messages that must go to the client. The thread that executes the
     * message's action in the client blocks until a message arrives in the queue, at which time it
     * attempts to execute that message's action.
     * <p>
     * The class provides methods that allow users to determine the state of the queue and of the
     * thread, which can be used to determine whether the client is actively sending or responding
     * to a message. It also provides a method that indicates whether a prior attempt at sending a
     * message resulted in a {@link RemoteException}, which indicates that the client's process has
     * terminated.
     *
     * @author PT
     */
    public static class RmiClientThread extends Thread {

        private final ZiggyRmiClientService serviceStub;
        private final String clientName;
        private final LinkedBlockingQueue<PipelineMessage> outgoingMessageQueue = new LinkedBlockingQueue<>();
        boolean clientDisconnected;

        public RmiClientThread(ZiggyRmiClientService serviceStub, String clientName) {
            this.serviceStub = serviceStub;
            this.clientName = clientName;
            setDaemon(true);
        }

        @Override
        public void run() {
            PipelineMessage message = null;
            try {
                while (true) {
                    message = outgoingMessageQueue.take();
                    serviceStub.takeMessageActionInClient(message);
                }
            } catch (RemoteException | PipelineException e) {
                log.info("Caught {}{} sending {} to {}", e.getClass().getSimpleName(),
                    e.getCause() != null ? " caused by " + e.getCause().getClass().getSimpleName()
                        : "",
                    message.getClass().getSimpleName(), clientName);
                clientDisconnected = true;
                Thread.currentThread().interrupt();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        public void addMessage(PipelineMessage message) {
            try {
                outgoingMessageQueue.put(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        public String getClientName() {
            return clientName;
        }

        public boolean isClientDisconnected() {
            return clientDisconnected;
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientName);
        }

        private LinkedBlockingQueue<PipelineMessage> getOutgoingMessageQueue() {
            return outgoingMessageQueue;
        }

        private Thread.State outgoingMessageThreadState() {
            return getState();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RmiClientThread other = (RmiClientThread) obj;
            return Objects.equals(clientName, other.clientName);
        }
    }
}
