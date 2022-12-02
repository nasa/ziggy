package gov.nasa.ziggy.util.os;

/**
 * Provides access to system memory information at runtime.
 *
 * @author Forrest Girouard
 */
public interface MemInfo extends SysInfo {
    /**
     * Total amount of physical RAM, in kilobytes.
     */
    long getTotalMemoryKB();

    /**
     * The amount of physical RAM, in kilobytes, left unused by the system.
     */
    long getFreeMemoryKB();

    /**
     * The amount of physical RAM, in kilobytes, used for file buffers.
     */
    long getBuffersKB();

    /**
     * The amount of physical RAM, in kilobytes, used as cache memory.
     */
    long getCachedKB();

    /**
     * The amount of swap, in kilobytes, used as cache memory.
     */
    long getCachedSwapedKB();

    /**
     * The total amount of swap available, in kilobytes.
     */
    long getTotalSwapKB();

    /**
     * The total amount of swap free, in kilobytes.
     */
    long getFreeSwapKB();
}
