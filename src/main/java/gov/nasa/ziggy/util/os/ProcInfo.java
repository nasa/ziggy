package gov.nasa.ziggy.util.os;

import java.util.List;

/**
 * @author Forrest Girouard
 */
public interface ProcInfo extends SysInfo {
    /**
     * Return a List of PIDs for all child processes.
     */
    List<Long> getChildPids();

    /**
     * Return a List of PIDs for child processes that match the specified name.
     */
    List<Long> getChildPids(String name);

    /**
     * Return the parent PID.
     */
    long getParentPid();

    long getPid();

    /**
     * Return the maximum number of open files for this process.
     *
     * @return -1 for unlimited.
     */
    int getOpenFileLimit();

    /**
     * Return the maximum process id value.
     */
    long getMaximumPid();

    /**
     * Returns the memory used by the given process, in bytes, or -1 if unable to obtain memory
     * usage from the OS.
     */
    long getMemoryBytes();

    /** Returns the memory used by the process and any descendant processes, as {@link String}s. */
    List<String> getProcessMemoryUsage();
}
