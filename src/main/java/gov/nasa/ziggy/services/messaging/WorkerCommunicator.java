package gov.nasa.ziggy.services.messaging;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messages.WorkerHeartbeatMessage;
import gov.nasa.ziggy.services.messages.WorkerShutdownMessage;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.worker.WorkerPipelineProcess;

/**
 * Worker end of two-way communication between the worker process and clients. The worker end plays
 * the role of the server in that it establishes the RMI registry used for the communication. It
 * also provides a method that broadcasts messages to all clients, each of which supplies a
 * {@link MessageHandlerService} stub when it initializes its own {@link UiCommunicator} instance.
 * <p>
 * The singleton {@link WorkerCommunicator} is created when the {@link WorkerPipelineProcess} is
 * created. At the same time a new {@link Thread} is created that periodically broadcasts instances
 * of the {@link WorkerHeartbeatMessage} to all clients. Both that thread and the
 * {@link WorkerCommunicator} instance are destroyed when the {@link WorkerPipelineProcess} exits.
 * The shutdown will also attempt to shut down the RMI registry (see below).
 * <p>
 * In the event that the {@link WorkerPipelineProcess} crashes, a new instance of the latter will be
 * created, along with new instances of the {@link WorkerCommunicator} and the heartbeat thread. In
 * this case, since the RMI registry is (probably) still running, the new instance of
 * {@link WorkerCommunicator} will attempt to connect to the registry and update the contents of
 * same. In this event, shutting down the {@link WorkerCommunicator} will not shut down the RMI
 * registry; it is impossible to shut down a registry if the process that created it has been lost.
 * Fortunately, shutting down the worker will eventually cause the registry to shut down as well.
 *
 * @author PT
 */
public class WorkerCommunicator implements WorkerCommunicatorService {

    private static Logger log = LoggerFactory.getLogger(WorkerCommunicator.class);

    /**
     * Singleton instance of {@link WorkerCommunicator} so all threads in the worker process can
     * make use of the initialized communicator.
     */
    private static WorkerCommunicator instance;

    private static ScheduledThreadPoolExecutor heartbeatExecutor;

    private Set<MessageHandlerService> clientMessageServiceStubs = new HashSet<>();
    private MessageHandlerService messageHandler;
    private Registry registry;
    private boolean registryCreated = false;

    private WorkerCommunicator(MessageHandlerService messageHandler) throws RemoteException {
        this.messageHandler = messageHandler;
    }

    /**
     * Constructs an instance of the class and assigns it as the singleton instance. This includes
     * construction of the RMI registry.
     * <P>
     * The {@link WorkerCommunicator} needs to provide two stubs in the registry, corresponding to
     * the two kinds of actions that clients need to be able to invoke:
     * <ol>
     * <li>Worker handling of client messages, which requires an instance of {@link MessageHandler}
     * with appropriate members for server-side execution.
     * <li>Addition of client-side {@link MessageHandler} stubs so that the worker can invoke
     * actions in all clients simultaneously; this requires that the singleton instance of
     * {@link WorkerCommunicator} be available to the clients as a stub.
     * </ol>
     */
    public static synchronized void initializeInstance(MessageHandlerService messageHandler,
        int rmiPort) {

        if (instance != null) {
            log.info("Worker communicator instance already available, skipping instantiation");
            return;
        }

        try {
            log.info("Starting new WorkerCommunicator instance");
            instance = new WorkerCommunicator(messageHandler);

            // Try to create the registry. If that fails due to ExportException, then this
            // is a new WorkerCommunicator started on a system where the worker crashed and
            // left the registry running; in that case, we need the registry so that we can
            // clean up services installed by the prior instance and then rebind new ones
            // to the registry.
            try {
                log.info("Creating RMI registry");
                instance.registry = LocateRegistry.createRegistry(rmiPort);
                instance.registryCreated = true;
            } catch (ExportException e) {
                log.info("Retrieving registry on localhost");
                instance.registry = LocateRegistry.getRegistry("localhost", rmiPort);
                instance.cleanUpRegistry();
            }
            log.info("Exporting and binding objects into registry");
            MessageHandlerService msgStub = (MessageHandlerService) UnicastRemoteObject
                .exportObject(messageHandler, 0);
            instance.registry.rebind(MessageHandlerService.SERVICE_NAME, msgStub);
            WorkerCommunicatorService commStub = (WorkerCommunicatorService) UnicastRemoteObject
                .exportObject(instance, 0);
            instance.registry.rebind(WorkerCommunicatorService.SERVICE_NAME, commStub);

            // Start the heartbeat messages
            log.info("Starting worker-UI heartbeat generator");
            heartbeatExecutor = new ScheduledThreadPoolExecutor(1);
            long heartbeatIntervalMillis = WorkerHeartbeatMessage.heartbeatIntervalMillis();
            if (heartbeatIntervalMillis > 0) {
                heartbeatExecutor.scheduleAtFixedRate(
                    () -> WorkerCommunicator.broadcast(new WorkerHeartbeatMessage()), 0,
                    WorkerHeartbeatMessage.heartbeatIntervalMillis(), TimeUnit.MILLISECONDS);
            }
            ZiggyShutdownHook.addShutdownHook(() -> {
                if (heartbeatExecutor != null) {
                    log.info("SHUTDOWN: Stopping worker communicator");
                    WorkerCommunicator.shutdown(true);
                    log.info("SHUTDOWN: worker communicator shutdown complete");
                }
            });
            log.info("WorkerCommunicator construction complete");
        } catch (

        RemoteException e) {
            throw new PipelineException(
                "Exception occurred when attempting to initialize WorkerCommunicator", e);
        }
    }

    private synchronized void cleanUpRegistry() {
        cleanUpRegistry(true);
    }

    /**
     * Remove obsolete bound and/or exported instances of the worker communicator and message
     * handler services. This is only necessary when a new WorkerCommunicator is being constructed
     * to replace one that crashed, or during shutdown.
     */
    private synchronized void cleanUpRegistry(boolean force) {
        try {
            log.info("Cleaning up bound / exported registry instances");
            registry.unbind(MessageHandlerService.SERVICE_NAME);
            registry.unbind(WorkerCommunicatorService.SERVICE_NAME);
            UnicastRemoteObject.unexportObject(messageHandler, force);
        } catch (RemoteException | NotBoundException e) {
            throw new PipelineException("Unable to unbind services from RMI registry", e);
        }
    }

    /**
     * Adds a new {@link MessageHandlerService} stub from a client to the set of same. When a client
     * initializes its {@link UiCommunicator} instance, that instance uses an RMI call that causes
     * the {@link WorkerCommunicator} to add its stub to the set. It also sends an immediate
     * heartbeat message to the UI so that the UI can confirm that communication has been started
     * correctly.
     */
    @Override
    public synchronized void addClientMessageHandlerStub(MessageHandlerService clientMessageStub) {
        log.info("Adding client message handler");
        clientMessageServiceStubs.add(clientMessageStub);
        try {
            clientMessageStub.handleMessage(new WorkerHeartbeatMessage());
        } catch (RemoteException e) {
            throw new PipelineException("Unable to send heartbeat message to new UI", e);
        }
    }

    static synchronized void stopHeartbeatExecutor() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
            heartbeatExecutor = null;
        }
    }

    /**
     * Triggers the handleMessage method of all client message handler stubs with a given message.
     * This causes all client instances to execute the appropriate handler action for that message.
     * <p>
     * In the event that there are {@link MessageHandlerService} stubs that belong to defunct UI
     * instances, the attempt to cause remote execution of their handleMessage() methods will
     * trigger a {@link RemoteException}. This will cause the offending stub to be removed from the
     * set of all client UI stubs.
     */
    public static synchronized void broadcast(PipelineMessage message) {

        log.debug("Broadcasting " + message.getClass().getName() + " message to "
            + getClientMessageServiceStubs().size() + "UI clients");
        Set<MessageHandlerService> obsoleteStubs = new HashSet<>();
        for (MessageHandlerService msgService : instance.clientMessageServiceStubs) {
            try {
                msgService.handleMessage(message);
            } catch (RemoteException e) {
                obsoleteStubs.add(msgService);
            }
        }
        if (!obsoleteStubs.isEmpty()) {
            log.info(
                "Removing " + obsoleteStubs.size() + " client message handlers from cache, keeping "
                    + (instance.clientMessageServiceStubs.size() - obsoleteStubs.size()));
        }
        instance.clientMessageServiceStubs.removeAll(obsoleteStubs);
    }

    public static synchronized Set<MessageHandlerService> getClientMessageServiceStubs() {
        return instance.clientMessageServiceStubs;
    }

    public static synchronized MessageHandlerService getMessageHandler() {
        return instance.messageHandler;
    }

    public static synchronized Registry getRegistry() {
        return instance.registry;
    }

    /**
     * Nullifies the singleton instance of the communicator.
     */
    public static synchronized void reset() {
        instance = null;
        heartbeatExecutor = null;
    }

    public static synchronized boolean isInitialized() {
        return instance != null;
    }

    public static synchronized void shutdown() {
        shutdown(true);
    }

    /**
     * Performs the most orderly shutdown possible of the worker communicator and the registry.
     * <p>
     * In all circumstances, the singleton instance of the communicator will be nullified, the
     * services will be unbound from the registry, the heartbeat executor will stop, and exported
     * objects will be unexported. In addition, if this is the instance of WorkerCommunicator that
     * created the registry, the registry will be unexported. Unfortunately, if the registry was
     * created by a now-crashed instance of the communicator, it will be necessary for the JVM to
     * shut it down when it destroys all daemon threads.
     */
    public static synchronized void shutdown(boolean force) {
        log.info("SHUTDOWN: Sending shutdown notification to clients");
        WorkerCommunicator.broadcast(new WorkerShutdownMessage());
        log.info("SHUTDOWN: Shutting down heartbeat thread");
        stopHeartbeatExecutor();
        try {
            if (instance != null) {
                log.info("SHUTDOWN: Unbinding / unexporting services");
                instance.cleanUpRegistry(force);

                // Unexport the WorkerCommunicator so that the registry will be empty
                UnicastRemoteObject.unexportObject(instance, force);

                // Attempt to unexport the registry if and only if this is the instance of
                // WorkerCommunicator that created it
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
        log.info("SHUTDOWN: WorkerCommunicator shutdown complete");
    }

}
