package gov.nasa.ziggy.util.os;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class parses output from ps(1) on a MacOSX box and provides the contents as a {@link Map}.
 *
 * @author Forrest Girouard
 */
public class MacOSXProcInfo extends AbstractSysInfo implements ProcInfo {
    private static final Logger log = LoggerFactory.getLogger(LinuxProcInfo.class);

    private static final String PS_COMMAND = "/bin/ps -p %d -o pid=,ppid=,command=";
    private static final String PS_LIST_COMMAND = "/bin/ps -xA -o pid=,ppid=,command=";
    private static final String ULIMIT_COMMAND = "/bin/sh ulimit -n";
    private static final int MAX_PID_VALUE = 99999;

    private final long pid;

    public MacOSXProcInfo(long pid) {
        super(commandOutput(String.format(PS_COMMAND, pid)));
        this.pid = pid;
    }

    public MacOSXProcInfo() {
        super(
            commandOutput(String.format(PS_COMMAND, gov.nasa.ziggy.util.os.ProcessUtils.getPid())));
        pid = gov.nasa.ziggy.util.os.ProcessUtils.getPid();
    }

    @Override
    protected void parse(Collection<String> commandOutput) {
        for (String line : commandOutput) {
            log.debug("line={}", line);

            if (line != null && line.trim().length() > 0) {
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length < 2) {
                    log.debug("Ignoring line with two few tokens: {}", line);
                    continue;
                }
                put("Pid", tokens[0]);
                put("PPid", tokens[1]);
                put("Name", tokens[2]);
            }
        }
    }

    @Override
    public List<Long> getChildPids() {
        return getChildPids(null);
    }

    @Override
    public List<Long> getChildPids(String name) {
        long currentPid = Long.parseLong(get("Pid"));
        List<String> commandOutput = commandOutput(PS_LIST_COMMAND);
        List<Long> childPids = new LinkedList<>();

        for (String line : commandOutput) {
            log.debug("line={}", line);

            if (line != null && line.trim().length() > 0) {
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length < 2) {
                    log.debug("Ignoring line with two few tokens: {}", line);
                    continue;
                }
                if (Long.parseLong(tokens[1]) == currentPid
                    && (name == null || tokens[2].endsWith(name))) {
                    // found a match
                    long pid = Long.parseLong(tokens[0]);
                    log.info("Found child process, pid={}, name={}", pid, tokens[2]);
                    childPids.add(pid);
                }
            }
        }

        return childPids;
    }

    @Override
    public long getParentPid() {
        return Long.parseLong(get("PPid"));
    }

    @Override
    public long getPid() {
        return pid;
    }

    /**
     * Return the maximum number of open files for this process.
     *
     * @return -1 for unlimited.
     */
    @Override
    public int getOpenFileLimit() {
        List<String> commandOutput = commandOutput(ULIMIT_COMMAND);
        return Integer.parseInt(commandOutput.get(0));
    }

    /**
     * Return the maximum process id value.
     */
    @Override
    public long getMaximumPid() {
        return MAX_PID_VALUE;
    }
}
