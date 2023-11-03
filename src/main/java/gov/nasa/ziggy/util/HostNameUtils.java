package gov.nasa.ziggy.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.UnknownHostException;

import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Utilities related to host names.
 *
 * @author PT
 */
public class HostNameUtils {

    /**
     * The full host name including domain.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static String hostName() {
        return ExternalProcess.commandOutput("hostname", (String[]) null).get(0);
    }

    /**
     * The host name, truncated to remove the domain.
     */
    public static String shortHostName() {
        return ExternalProcess.commandOutput("hostname -s", (String[]) null).get(0);
    }

    public static String shortHostNameFromHostName(String fullHostName) {
        return fullHostName.split("\\.")[0];
    }

    /**
     * Compares a {@link String} containing a host name with the host name of the current system. If
     * they match, "localhost" is returned, otherwise the short version of the caller-supplied host
     * name is returned. If the system's host name cannot be determined, resulting in an
     * {@link UnknownHostException}, the short version of the caller-supplied host name is returned.
     */
    public static String callerHostNameOrLocalhost(String callerHostName) {
        checkNotNull(callerHostName, "caller host name");
        return callerHostName.equals(hostName()) ? "localhost"
            : shortHostNameFromHostName(callerHostName);
    }
}
