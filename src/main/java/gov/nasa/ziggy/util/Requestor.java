package gov.nasa.ziggy.util;

import java.util.UUID;

/**
 * An object that makes a request and needs to identify the response (i.e., it needs to see whether
 * the response was intended for it or for another {@link Requestor} instance. This is accomplished
 * by providing a unique identifier, which is obtained by the {@link #requestorIdentifier()} method.
 * The response will be another instance of {@link Requestor} that has the same identifier. This
 * allows the {@link #isDestination(Requestor)} method to determine, er, whether this object is the
 * intended destination.
 * <p>
 * Note that while the requestor identifier can be any {@link Object} of any class, each
 * {@link Requestor} must have one. In use cases wherein the identifier is required to be unique, it
 * is recommended that users implement {@link #requestorIdentifier()} with classes that are
 * inherently and automatically unique, such as {@link UUID#randomUuid()}.
 *
 * @author PT
 */
public interface Requestor {

    Object requestorIdentifier();

    default boolean isDestination(Requestor requestor) {
        return requestor == null || requestorIdentifier() == null ? false
            : requestorIdentifier().equals(requestor.requestorIdentifier());
    }
}
