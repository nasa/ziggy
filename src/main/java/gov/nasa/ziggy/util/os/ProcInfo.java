package gov.nasa.ziggy.util.os;

import java.io.IOException;
import java.util.List;

/**
 * @author Forrest Girouard
 */
public interface ProcInfo extends SysInfo {
    /**
     * Return a List of PIDs for all child processes.
     */
    List<Long> getChildPids() throws IOException;

    /**
     * Return a List of PIDs for child processes that match the specified name.
     */
    List<Long> getChildPids(String name) throws IOException;

    /**
     * Return the parent PID.
     */
    long getParentPid() throws Exception;

    /**
     * Return the PID.
     */
    long getPid();

    /**
     * Return the maximum number of open files for this process.
     *
     * @return -1 for unlimited.
     */
    int getOpenFileLimit() throws IOException;

    /**
     * Return the maximum process id value.
     */
    long getMaximumPid();
}
