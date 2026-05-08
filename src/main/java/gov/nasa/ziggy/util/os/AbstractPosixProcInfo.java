package gov.nasa.ziggy.util.os;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.process.ExternalProcess;

public abstract class AbstractPosixProcInfo extends AbstractSysInfo implements ProcInfo {

    private static final Logger log = LoggerFactory.getLogger(AbstractPosixProcInfo.class);

    public AbstractPosixProcInfo(Collection<String> sysInfo) {
        super(sysInfo);
    }

    private long pid;
    private List<String> processMemoryUsage;

    @Override
    public long getMemoryBytes() {
        ExternalProcess memoryConsumptionExternalProcess = memoryConsumptionExternalProcess();
        memoryConsumptionExternalProcess.execute();
        String memory = memoryConsumptionExternalProcess.stdout().get(0);
        if (StringUtils.isBlank(memory)) {
            log.debug("No memory samples");
            processMemoryUsage = null;
            return -1;
        }
        processMemoryUsage = memoryConsumptionExternalProcess.stdout();
        long totalMemory = 0;
        log.debug("Memory (KB) {}", memoryConsumptionExternalProcess.stdout().toString());
        for (String processMemory : processMemoryUsage) {
            log.debug("Value of processMemory {}", processMemory);
            totalMemory += Long.parseLong(processMemory.strip());
            log.debug("Value of totalMemory {}", totalMemory);
        }
        return totalMemory * BYTES_PER_KIB;
    }

    /**
     * Returns the {@link ExternalProcess} that measures memory consumption for this process and its
     * descendants.
     */
    ExternalProcess memoryConsumptionExternalProcess() {
        Set<Long> processIds = ProcessUtils.descendantProcessIds(pid);
        processIds.add(pid);
        log.debug("Process ids {}", processIds.toString());
        ExternalProcess memoryConsumptionExternalProcess = ExternalProcess
            .simpleExternalProcess(psRssCommand(processIds));
        memoryConsumptionExternalProcess.writeStdOut(true);

        return memoryConsumptionExternalProcess;
    }

    protected abstract String psRssCommand();

    protected abstract String pidFormat();

    private String psRssCommand(Set<Long> pids) {
        int pidCount = pids.size();
        StringBuilder rssCommandBuilder = new StringBuilder(psRssCommand());
        for (int pidCounter = 0; pidCounter < pidCount; pidCounter++) {
            rssCommandBuilder.append(pidFormat());
        }
        return String.format(rssCommandBuilder.toString(), pids.toArray());
    }

    @Override
    public long getPid() {
        return pid;
    }

    protected void setPid(long pid) {
        this.pid = pid;
    }

    @Override
    public List<String> getProcessMemoryUsage() {
        return processMemoryUsage;
    }
}
