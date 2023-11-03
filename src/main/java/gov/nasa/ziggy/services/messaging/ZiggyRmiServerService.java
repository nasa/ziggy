package gov.nasa.ziggy.services.messaging;

import java.rmi.Remote;
import java.rmi.RemoteException;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Defines the RMI service provided by an instance of {@link ZiggyRmiServer}.
 * <p>
 * The {@link ZiggyRmiServer} provides two capabilities that get exported to the RMI registry:
 * <ol>
 * <li>Instances of {@link ZiggyRmiClientService} can submit a stub that allows the server to send
 * messages to that client. The stubs are stored in the server and removed when the corresponding
 * client ceases to function.
 * <li>Instances of {@link ZiggyRmiClientService} can submit a {@link PipelineMessage} to the
 * {@link ZiggyRmiServerService}, which will then re-transmit that message to all clients.
 * </ol>
 * <p>
 * Note that RMI requires all exported capabilities to be represented by an interface that extends
 * {@link Remote}, which is why we have this interface class at all.
 *
 * @author PT
 */
public interface ZiggyRmiServerService extends Remote {

    String SERVICE_NAME = "ZiggyRmiServerService";

    // Note: it may seem that the implementations of these methods
    // don't have anything in them that throws RemoteException, hence
    // that these throws declarations can be removed. THEY CANNOT! RMI requires
    // that all exported methods throw RemoteException, no matter what
    // the underlying code does. If these throws declarations are removed,
    // Ziggy's RMI system will fail at runtime.
    void transmitToServer(PipelineMessage message) throws RemoteException;

    void addClientStub(ZiggyRmiClientService client) throws RemoteException;
}
