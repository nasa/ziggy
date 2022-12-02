package gov.nasa.ziggy.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utilities related to host names.
 *
 * @author PT
 */
public class HostNameUtils {

    /**
     * The full host name including domain.
     *
     * @throws UnknownHostException
     */
    public static String hostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    /**
     * The host name, truncated to remove the domain.
     *
     * @throws UnknownHostException
     */
    public static String truncatedHostName() throws UnknownHostException {
        String hostName = hostName();
        int dotIdx = hostName.indexOf(".");
        return dotIdx != -1 ? hostName.substring(0, dotIdx) : hostName;
    }

    /**
     * Compares a {@link String} containing a truncated host name with the truncated host name of
     * the current system. If they match, "localhost" is returned, otherwise the caller-supplied
     * host name is returned. If the system's host name cannot be determined, resulting in an
     * {@link UnknownHostException}, the caller-supplied host name is returned.
     */
    public static String callerHostNameOrLocalhost(String callerHostName) {
        checkNotNull(callerHostName, "caller host name");
        try {
            return callerHostName.equals(truncatedHostName()) ? "localhost" : callerHostName;
        } catch (UnknownHostException e) {
            return callerHostName;
        }
    }
}
