package gov.nasa.ziggy.util.os;

/**
 * Determines the total memory for the current hardware at runtime under the Linux operating system.
 *
 * @author Forrest Girouard
 * @author PT
 */
public class LinuxMemInfo extends AbstractMemInfo {
    private static final String MEMINFO_COMMAND = "/usr/bin/more /proc/meminfo";

    /*
     * @formatter:off
        MemTotal: Total amount of physical RAM, in kilobytes.
        MemFree: The amount of physical RAM, in kilobytes, left unused by the system.
        Buffers: The amount of physical RAM, in kilobytes, used for file buffers.
        Cached: The amount of physical RAM, in kilobytes, used as cache memory.
        SwapCached: The amount of swap, in kilobytes, used as cache memory.
        SwapTotal: The total amount of swap available, in kilobytes.
        SwapFree: The total amount of swap free, in kilobytes.

        MemTotal:      4061040 kB
        MemFree:        587856 kB
        Buffers:         21848 kB
        Cached:         331860 kB
        SwapCached:      73708 kB
        SwapTotal:     2031608 kB
        SwapFree:      1884796 kB
     * @formatter:on
     */

    private static final String TOTAL_MEMORY_KEY = "MemTotal";
    private static final String FREE_MEMORY_KEY = "MemFree";
    private static final String BUFFERS_KEY = "Buffers";
    private static final String CACHED_KEY = "Cached";
    private static final String SWAP_CACHED_KEY = "SwapCached";
    private static final String SWAP_TOTAL_KEY = "SwapTotal";
    private static final String SWAP_FREE_KEY = "SwapFree";

    public LinuxMemInfo() {
        super(commandOutput(MEMINFO_COMMAND));
    }

    @Override
    public String getTotalMemoryKey() {
        return TOTAL_MEMORY_KEY;
    }

    @Override
    public String getBuffersKey() {
        return BUFFERS_KEY;
    }

    @Override
    public String getCachedKey() {
        return CACHED_KEY;
    }

    @Override
    public String getCachedSwapKey() {
        return SWAP_CACHED_KEY;
    }

    @Override
    public String getFreeMemoryKey() {
        return FREE_MEMORY_KEY;
    }

    @Override
    public String getFreeSwapKey() {
        return SWAP_FREE_KEY;
    }

    @Override
    public String getTotalSwapKey() {
        return SWAP_TOTAL_KEY;
    }
}
