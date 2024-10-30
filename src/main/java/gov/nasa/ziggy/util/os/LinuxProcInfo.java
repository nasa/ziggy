package gov.nasa.ziggy.util.os;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * This class parses /proc/PID/status on a Linux box and provides the contents as a {@link Map}.
 *
 * @author Todd Klaus
 * @author Forrest Girouard
 * @author PT
 */
public class LinuxProcInfo extends AbstractSysInfo implements ProcInfo {
    private static final Logger log = LoggerFactory.getLogger(LinuxProcInfo.class);

    private static final String PROC_STATUS_FILE = "/usr/bin/more /proc/%d/status";
    private static final String PROC_LIMITS_FILE = "/usr/bin/more /proc/%d/limits";

    private static final String MAX_OPEN_FILES = "Max open files";

    private final long pid;

    public LinuxProcInfo(long pid) {
        super(commandOutput(String.format(PROC_STATUS_FILE, pid)));
        this.pid = pid;
    }

    public LinuxProcInfo() {
        super(commandOutput(
            String.format(PROC_STATUS_FILE, gov.nasa.ziggy.util.os.ProcessUtils.getPid())));
        pid = gov.nasa.ziggy.util.os.ProcessUtils.getPid();
    }

    @Override
    public List<Long> getChildPids() {
        return getChildPids(null);
    }

    @Override
    public List<Long> getChildPids(String name) {
        int currentPid = Integer.parseInt(get("Pid"));
        File procDir = new File("/proc");
        File[] procFiles = procDir.listFiles();
        List<Long> childPids = new LinkedList<>();

        for (File procFile : procFiles) {
            try {
                long pid = Long.parseLong(procFile.getName());
                LinuxProcInfo procInfo = new LinuxProcInfo(pid);
                String processName = procInfo.get("Name");
                long ppid = Long.parseLong(procInfo.get("PPid"));

                if (ppid == currentPid && (name == null || name.equals(processName))) {
                    // found a match
                    log.info("Found child process, pid={}, name={}", pid, processName);
                    childPids.add(pid);
                }
            } catch (Exception e) {
                // ignore files that are not a number (PID) or can't be read
            }
        }

        return childPids;
    }

    @Override
    public long getParentPid() {
        return Integer.parseInt(get("PPid"));
    }

    @Override
    public long getPid() {
        return pid;
    }

    /**
     * Return the maximum number of open files for this process.
     *
     * @return -1 for unlimited
     */
    @Override
    public int getOpenFileLimit() {
        int openFileLimit = -1;

        List<String> limitsFileOutput = commandOutput(
            String.format(PROC_LIMITS_FILE, Integer.valueOf(get("Pid"))));
        for (String line : limitsFileOutput) {
            if (!line.startsWith(MAX_OPEN_FILES)) {
                continue;
            }
            String[] parts = line.split("\\s+");

            openFileLimit = Integer.parseInt(parts[parts.length - 3]);
        }
        return openFileLimit;
    }

    /**
     * Return the maximum process id value.
     */
    @Override
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public long getMaximumPid() {
        Path pidMaxFile = Paths.get("/proc/sys/kernel/pid_max");
        try {
            String pidMaxLine = Files.readAllLines(pidMaxFile, ZiggyFileUtils.ZIGGY_CHARSET).get(0);
            if (pidMaxLine.endsWith("\n")) {
                pidMaxLine = pidMaxLine.substring(0, pidMaxLine.length() - 1);
            }
            return Integer.parseInt(pidMaxLine);
        } catch (IOException e) {
            throw new UncheckedIOException("Read of pid file " + pidMaxFile.toString() + " failed",
                e);
        }
    }
}
