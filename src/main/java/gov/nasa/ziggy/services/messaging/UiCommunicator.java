package gov.nasa.ziggy.services.messaging;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.ui.ZiggyConsole;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager;
import gov.nasa.ziggy.ui.common.ProcessHeartbeatManager.NoHeartbeatException;

/**
 * UI end of two-way communication between the worker and all clients. The clients make use of the
 * existing registry created by the worker. The registry in turn provides a method that allows
 * messages to be sent to the worker and arranges for the worker to have broadcast access to the
 * clients.
 * <p>
 * A {@link UiCommunicator} singleton is created when an instance of {@link ZiggyGuiConsole} or
 * {@link ZiggyConsole} is created via the {@link initializeInstance} method. The singleton will be
 * destroyed and re-created via {@link restart} if contact is lost with the worker. When the
 * creating instance exits, the {@link UiCommunicator} singleton will be destroyed via the
 * {@link reset} method.
 *
 * @author PT
 */
public class UiCommunicator {

    private static final String RMI_REGISTRY_HOST = "localhost";

    /**
     * Singleton instance of {@link UiCommunicator} class. All threads in the UI process can access
     * this instance via the static methods.
     */
    private static UiCommunicator instance;

    private static Logger log = LoggerFactory.getLogger(UiCommunicator.class);
    private static ProcessHeartbeatManager heartbeatManager;

    /**
     * Allows messages to get sent to the server. Technically, this actually allows the
     * UiCommunicator instance to request that the WorkerCommunicator's {@link MessageHandler}
     * instance act on a {@link PipelineMessage} instance constructed by the client, but you get the
     * idea.
     */
    private MessageHandlerService messageHandlerService;

    /**
     * Allows the UiCommunicator instance to add a stub of its {@link MessageHandler} instance to
     * the collection of same in the {@link WorkerCommunicator} instance.
     */
    private WorkerCommunicatorService workerService;

    /**
     * When stubbed and sent to the {@link WorkerCommunicator} instance, this will allow that
     * instance to request the UiCommunicator instance handle a message constructed by server (or if
     * you prefer, allows the server to send a message to this client).
     */
    private MessageHandler messageHandler;

    private Registry registry;
    private int rmiPort;

    private UiCommunicator(MessageHandler messageHandler, int rmiPort)
        throws RemoteException, NotBoundException {
        this.rmiPort = rmiPort;
        this.messageHandler = messageHandler;
    }

    /**
     * Constructs and installs the singleton instance of the class. The worker's message handler is
     * obtained and stored for use in sending messages. The worker's communicator itself is obtained
     * as a stub to allow the client's message handler stub to be added to the worker's collection
     * of same.
     * <p>
     * If the singleton instance already exists, it will be kept and no new instance will be
     * constructed.
     */
    public static synchronized void initializeInstance(MessageHandler messageHandler, int rmiPort) {

        if (instance != null) {
            log.info("UI communicator instance already available, skipping instantiation");
            return;
        }

        try {
            log.info("Starting new UiCommunicator instance");
            instance = new UiCommunicator(messageHandler, rmiPort);
            log.info("Retrieving registry on " + RMI_REGISTRY_HOST);
            instance.registry = LocateRegistry.getRegistry(RMI_REGISTRY_HOST, rmiPort);

            // get the stubs that the server provided for the message handler and the
            // worker communicator
            log.info("Looking up services in registry");
            instance.messageHandlerService = (MessageHandlerService) instance.registry
                .lookup(MessageHandlerService.SERVICE_NAME);
            instance.workerService = (WorkerCommunicatorService) instance.registry
                .lookup(WorkerCommunicatorService.SERVICE_NAME);

            // Construct a stub of this instance's message handler and add that to the server's
            // list of same. Note that we first need to unexport it if it was previously
            // exported.
            try {
                UnicastRemoteObject.unexportObject(messageHandler, true);
            } catch (NoSuchObjectException e) {
            }
            MessageHandlerService exportedMessageHandler = (MessageHandlerService) UnicastRemoteObject
                .exportObject(messageHandler, 0);
            log.info("Adding UI message handler service to worker communicator instance");
            instance.workerService.addClientMessageHandlerStub(exportedMessageHandler);

            // Start the heartbeat manager.
            if (heartbeatManager == null) {
                heartbeatManager = new ProcessHeartbeatManager(messageHandler);
            }
            heartbeatManager.initialize();
            log.info("UiCommunicator construction complete");
        } catch (RemoteException | NotBoundException | NoHeartbeatException e) {
            throw new PipelineException(
                "Exception occurred when attempting to initialize UiCommunicator", e);
        }
    }

    /**
     * Sends a message to the worker. To be pedantic about it, the message isn't "sent," exactly,
     * but instead what happens is that the worker's {@link MessageHandler} instance is made to run
     * its handleMessage method in the worker's process space, and its exact actions are based on
     * the content of the {@link PipelineMessage} provided by this method to the handleMessage
     * method in the worker.
     */
    public static synchronized Object send(PipelineMessage message) {
        try {
            log.debug("Sending " + message.getClass().getName() + " message to worker");
            return instance.messageHandlerService.handleMessage(message);
        } catch (RemoteException e) {
            throw new PipelineException(
                "RemoteException occurred attempting to send " + message.getClass().getName(), e);
        }
    }

    public static synchronized MessageHandlerService getServerMessageHandlerStub() {
        return instance.messageHandlerService;
    }

    public static synchronized WorkerCommunicatorService getWorkerService() {
        return instance.workerService;
    }

    public static synchronized MessageHandler getMessageHandler() {
        return instance.messageHandler;
    }

    public static synchronized Registry getRegistry() {
        return instance.registry;
    }

    public static synchronized int getRmiPort() {
        return instance.rmiPort;
    }

    static synchronized void stopHeartbeatListener() {
        if (heartbeatManager != null && heartbeatManager.getHeartbeatListener() != null) {
            heartbeatManager.getHeartbeatListener().shutdownNow();
        }
        heartbeatManager = null;
    }

    /**
     * Destroys the existing instance and creates a new one. This has to be performed when the UI
     * loses contact with the worker, as it indicates that the worker may have crashed and
     * restarted, in which case it needs to be repopulated with {@link MessageHandlerService} stubs
     * from all UIs.
     */
    public static synchronized void restart() {
        log.info("Attempting restart of UiCommunicator instance");
        MessageHandler messageHandler = UiCommunicator.getMessageHandler();
        messageHandler.resetLastHeartbeatTime();
        int rmiPort = UiCommunicator.getRmiPort();
        instance = null;
        initializeInstance(messageHandler, rmiPort);

    }

    public static synchronized void reset() {
        stopHeartbeatListener();
        instance = null;
    }

    static synchronized void setHeartbeatManager(ProcessHeartbeatManager heartbeatManager) {
        UiCommunicator.heartbeatManager = heartbeatManager;
    }
}
