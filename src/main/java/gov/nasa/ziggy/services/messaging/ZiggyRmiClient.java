package gov.nasa.ziggy.services.messaging;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.messages.PipelineMessage;
import gov.nasa.ziggy.services.messaging.ProcessHeartbeatManager.NoHeartbeatException;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.ui.ZiggyConsole;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.os.ProcessUtils;
import gov.nasa.ziggy.worker.PipelineWorker;

/**
 * Client end of two-way communication between the server and all clients. The clients make use of
 * the existing registry created by the server. The registry in turn provides a method that allows
 * messages to be sent to the server and arranges for the server to have broadcast access to the
 * clients.
 * <p>
 * A {@link ZiggyRmiClient} singleton is created is created via the {@link initializeInstance}
 * method. The {@link PipelineSupervisor} has a client, as does every {@link PipelineWorker} and
 * {@link ZiggyConsole}. The singleton will be destroyed and re-created via {@link restart} if
 * contact is lost with the server. When the creating instance exits, the {@link ZiggyRmiClient}
 * singleton will be destroyed via the {@link reset} method.
 * <p>
 * The {@link ZiggyRmiClient} is used by Ziggy console, supervisor, and worker processes. Each
 * instance is created with an appropriate collection of {@link MessageAction} instances that can
 * process all messages the client must process.
 * <p>
 * The {@link ZiggyRmiClient} instance is immutable, hence thread-safe.
 *
 * @author PT
 */
public class ZiggyRmiClient implements ZiggyRmiClientService {

    private static final String RMI_REGISTRY_HOST = "localhost";

    /**
     * Singleton instance of {@link ZiggyRmiClient} class. All threads in the UI process can access
     * this instance via the static methods.
     */
    private static ZiggyRmiClient instance;

    private static Logger log = LoggerFactory.getLogger(ZiggyRmiClient.class);

    // This Map is used for test purposes. It allows the unit tests to examine the messages
    // detected by the singleton instance.
    private static Map<Class<? extends PipelineMessage>, List<PipelineMessage>> messagesDetected = new HashMap<>();

    // This is used in test to disable forwarding of received messages to the ZiggyMessenger.
    private boolean useMessenger = true;

    private final ZiggyRmiServerService ziggyRmiServerService;
    private final Registry registry;
    private final int rmiPort;

    // Stores the type of client (worker, supervisor, console).
    private final String clientType;

    private ZiggyRmiClient(int rmiPort, String clientType)
        throws RemoteException, NotBoundException {
        this.clientType = clientType;
        this.rmiPort = rmiPort;
        log.info("Retrieving registry on {}", RMI_REGISTRY_HOST);
        registry = LocateRegistry.getRegistry(RMI_REGISTRY_HOST, rmiPort);

        // get the stub that the server provided.
        log.info("Looking up services in registry");
        ziggyRmiServerService = (ZiggyRmiServerService) registry
            .lookup(ZiggyRmiServerService.SERVICE_NAME);
    }

    /**
     * Constructs and installs the singleton instance of the class. The server's stub is obtained
     * and stored for use in sending messages. The same stub is used to add the stub from the client
     * singleton instance to the server.
     * <p>
     * If the singleton instance already exists, it will be kept and no new instance will be
     * constructed.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static synchronized void initializeInstance(int rmiPort, String clientType) {

        if (isInitialized()) {
            log.info("ZiggyRmiClient instance already available, skipping instantiation");
            return;
        }

        try {
            log.info("Starting new ZiggyRmiClient instance");
            instance = new ZiggyRmiClient(rmiPort, clientType);

            // Construct a stub of this instance and add that to the server's
            // list of same. Note that we first need to unexport it if it was previously
            // exported.
            try {
                UnicastRemoteObject.unexportObject(instance, true);
            } catch (NoSuchObjectException ignored) {
                // In this case, we don't want execution to halt, because nothing bad will
                // happen if the object we want to "unexport" is already gone. Swallow this
                // exception.
            }
            ZiggyRmiClientService exportedClient = (ZiggyRmiClientService) UnicastRemoteObject
                .exportObject(instance, 0);
            log.info("Adding client service stub to server instance");
            instance.ziggyRmiServerService.addClientStub(exportedClient);
            log.info("ZiggyRmiClient construction complete");
        } catch (RemoteException | NotBoundException | NoHeartbeatException e) {
            throw new PipelineException(
                "Exception occurred when attempting to initialize UiCommunicator", e);
        }
    }

    public static boolean isInitialized() {
        return instance != null;
    }

    /**
     * Sends a message to the server. To be pedantic about it, the message isn't "sent," exactly,
     * but instead what happens is that the server is made to run its
     * {@link ZiggyRmiServerService#transmitToServer(PipelineMessage)} method in the server's
     * process space. Package private because only the {@link ZiggyMessenger} should call it, other
     * users should go by way of {@link ZiggyMessenger}.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    static synchronized void send(PipelineMessage message, CountDownLatch latch) {
        log.debug("Sending {}", message.getClass().getName());
        try {
            instance.ziggyRmiServerService.transmitToServer(message);
        } catch (RemoteException e) {
            throw new PipelineException(e);
        } finally {
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    /**
     * Handles messages sent from the server to the client. This is one of the methods exported to
     * RMI via the service interface.
     */
    @Override
    public void takeMessageActionInClient(PipelineMessage message) throws RemoteException {
        if (useMessenger) {
            ZiggyMessenger.actOnMessage(message);
        } else {
            recordMessage(message);
        }
    }

    /**
     * Updates the count of the number of detected or acted-upon messages of a given class.
     */
    private void recordMessage(PipelineMessage message) {

        Class<? extends PipelineMessage> messageClass = message.getClass();
        if (!messagesDetected.containsKey(messageClass)) {
            messagesDetected.put(messageClass, new ArrayList<>());
        }
        messagesDetected.get(messageClass).add(message);
    }

    @Override
    public String clientName() throws RemoteException {
        return ProcessUtils.getPid() + ":" + clientType;
    }

    public static Registry getRegistry() {
        return instance.registry;
    }

    public static int getRmiPort() {
        return instance.rmiPort;
    }

    public static String getClientType() {
        return instance.clientType;
    }

    /**
     * Destroys the existing instance and creates a new one. This has to be performed when the
     * client loses contact with the server, as it indicates that the server may have crashed and
     * restarted, in which case it needs to be repopulated with {@link ZiggyRmiClientService} stubs
     * from all clients.
     */
    public static synchronized void restart() {
        log.info("Attempting restart of ZiggyRmiClient instance");
        ProcessHeartbeatManager.resetHeartbeatTime();
        int rmiPort = ZiggyRmiClient.getRmiPort();
        String clientType = ZiggyRmiClient.getClientType();
        instance = null;
        initializeInstance(rmiPort, clientType);
    }

    public static synchronized void reset() {
        instance = null;
    }

    /** For testing only. */
    static ZiggyRmiServerService ziggyRmiServerService() {
        return instance.ziggyRmiServerService;
    }

    /** For testing only. */
    static Map<Class<? extends PipelineMessage>, List<PipelineMessage>> messagesDetected() {
        return messagesDetected;
    }

    /** For testing only. */
    static void clearDetectedMessages() {
        messagesDetected.clear();
    }

    /** For testing only. */
    static void setUseMessenger(boolean useMessenger) {
        instance.useMessenger = useMessenger;
    }
}
