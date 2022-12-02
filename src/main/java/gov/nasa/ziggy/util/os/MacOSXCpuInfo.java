package gov.nasa.ziggy.util.os;

import java.io.IOException;

/**
 * Determines the number of CPU cores for the current hardware at runtime under the Mac OS X
 * operating system.
 *
 * @author Forrest Girouard
 * @author PT
 */
public class MacOSXCpuInfo extends AbstractSysInfo implements CpuInfo {
    private static final String NUM_CORES_KEY = "hw.physicalcpu";

    public MacOSXCpuInfo() throws IOException {
        super(commandOutput("/usr/sbin/sysctl -a", "hw."));
    }

    @Override
    public int getNumCores() {
        return Integer.parseInt(get(NUM_CORES_KEY.toLowerCase()));
    }

    @Override
    public String getNumCoresKey() {
        return NUM_CORES_KEY;
    }
}
