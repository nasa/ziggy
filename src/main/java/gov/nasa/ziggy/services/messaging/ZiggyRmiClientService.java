package gov.nasa.ziggy.services.messaging;

import java.rmi.Remote;
import java.rmi.RemoteException;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Defines the RMI service provided by an instance of {@link ZiggyRmiClient}. The provided service
 * is a stub from the client that is exported, via RMI, to the {@link ZiggyRmiServerService}. This
 * stub allows the server to send a message to the client. All messages received by the server are
 * then re-sent to all clients.
 * <p>
 * Note that RMI requires all exported capabilities to be represented by an interface that extends
 * {@link Remote}, which is why we have this interface class at all.
 *
 * @author PT
 */
public interface ZiggyRmiClientService extends Remote {

    String SERVICE_NAME = "ZiggyRmiClientService";

    // Note: it may seem that the implementations of these methods
    // don't have anything in them that throws RemoteException, hence
    // that these throws declarations can be removed. THEY CANNOT! RMI requires
    // that all exported methods throw RemoteException, no matter what
    // the underlying code does. If these throws declarations are removed,
    // Ziggy's RMI system will fail at runtime.
    void takeMessageActionInClient(PipelineMessage message) throws RemoteException;

    String clientName() throws RemoteException;
}
