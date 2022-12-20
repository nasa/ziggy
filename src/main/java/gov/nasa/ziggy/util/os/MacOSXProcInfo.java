package gov.nasa.ziggy.util.os;

import java.io.IOException;
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

    public MacOSXProcInfo(long pid) throws IOException {
        super(commandOutput(String.format(PS_COMMAND, pid)));
        this.pid = pid;
    }

    public MacOSXProcInfo() throws IOException {
        super(
            commandOutput(String.format(PS_COMMAND, gov.nasa.ziggy.util.os.ProcessUtils.getPid())));
        pid = gov.nasa.ziggy.util.os.ProcessUtils.getPid();
    }

    @Override
    protected void parse(Collection<String> commandOutput) throws IOException {
        for (String line : commandOutput) {
            log.debug("line = " + line);

            if (line != null && line.trim().length() > 0) {
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length < 2) {
                    log.debug("ignoring line with two few tokens: " + line);
                    continue;
                }
                put("Pid", tokens[0]);
                put("PPid", tokens[1]);
                put("Name", tokens[2]);
            }
        }
    }

    @Override
    public List<Long> getChildPids() throws IOException {
        return getChildPids(null);
    }

    @Override
    public List<Long> getChildPids(String name) throws IOException {
        long currentPid = Long.parseLong(get("Pid"));
        List<String> commandOutput = commandOutput(PS_LIST_COMMAND);
        List<Long> childPids = new LinkedList<>();

        for (String line : commandOutput) {
            log.debug("line = " + line);

            if (line != null && line.trim().length() > 0) {
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length < 2) {
                    log.debug("ignoring line with two few tokens: " + line);
                    continue;
                }
                if (Long.valueOf(tokens[1]) == currentPid
                    && (name == null || tokens[2].endsWith(name))) {
                    // found a match
                    long pid = Long.parseLong(tokens[0]);
                    log.info("Found child process, pid=" + pid + ", name=" + tokens[2]);
                    childPids.add(pid);
                }
            }
        }

        return childPids;
    }

    @Override
    public long getParentPid() throws Exception {
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
    public int getOpenFileLimit() throws IOException {
        try {
            List<String> commandOutput = commandOutput(ULIMIT_COMMAND);
            return Integer.parseInt(commandOutput.get(0));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Return the maximum process id value.
     */
    @Override
    public long getMaximumPid() {
        return MAX_PID_VALUE;
    }
}
