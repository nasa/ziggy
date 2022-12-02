package gov.nasa.ziggy.util.os;

import java.io.IOException;

/**
 * @author Forrest Girouard
 * @author PT
 */
public class LinuxCpuInfo extends AbstractSysInfo implements CpuInfo {
    private static final String CORES_PER_SOCKET_KEY = "Core(s) per socket";
    private static final String SOCKETS_KEY = "Socket(s)";
    private static final String COMMAND = "/usr/bin/lscpu";

    public LinuxCpuInfo() throws IOException {
        super(commandOutput(COMMAND));
    }

    @Override
    public int getNumCores() {
        return Integer.parseInt(get(CORES_PER_SOCKET_KEY)) * Integer.parseInt(get(SOCKETS_KEY));
    }

    @Override
    public String getNumCoresKey() {
        return CORES_PER_SOCKET_KEY + " * " + SOCKETS_KEY;
    }
}
