package gov.nasa.ziggy.services.messaging;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Service formulation of the WorkerCommunicator class. This is necessary because the clients need
 * to be able to add a stub MessageHandlerService to the server, such that the server can use RMI to
 * invoke actions at the client side.
 *
 * @author PT
 */
public interface WorkerCommunicatorService extends Remote {

    String SERVICE_NAME = "workerCommunicatorService";

    void addClientMessageHandlerStub(MessageHandlerService clientMessageStub)
        throws RemoteException;
}
