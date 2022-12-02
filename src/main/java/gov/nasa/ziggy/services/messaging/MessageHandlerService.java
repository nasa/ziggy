package gov.nasa.ziggy.services.messaging;

import java.rmi.Remote;
import java.rmi.RemoteException;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Service that must be implemented by the {@link MessageHandler} class. The use of an interface is
 * necessary for the Remote Method Invocation (RMI) API, which is used for communication between
 * Ziggy components.
 *
 * @author PT
 */
public interface MessageHandlerService extends Remote {

    String SERVICE_NAME = "messageHandlerService";

    Object handleMessage(PipelineMessage message) throws RemoteException;
}
