package gov.nasa.ziggy.util.os;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.process.ExternalProcess;

/**
 * Reads records (lines) from a specified {@code Reader} containing a colon-delimited label and
 * value providing access to the contents as a {@link Map} of labels to values.
 *
 * @author Forrest Girouard
 * @author PT
 */
public abstract class AbstractSysInfo implements SysInfo {
    private static final Logger log = LoggerFactory.getLogger(LinuxMemInfo.class);

    private final Map<String, String> sysInfoMap = new HashMap<>();

    protected static final List<String> commandOutput(String command) {
        return commandOutput(command, (String[]) null);
    }

    /**
     * Executes a command as an external process and returns the result as a {@link List} of
     * {@link String}s. The results are filtered to only include strings that themselves contain one
     * or more target strings.
     */
    protected static final List<String> commandOutput(String command, String... targets) {
        ExternalProcess commandProcess = ExternalProcess.simpleExternalProcess(command);
        commandProcess.writeStdErr(true);
        commandProcess.writeStdOut(true);
        commandProcess.run(true, 0);
        return commandProcess.stdout(targets);
    }

    public AbstractSysInfo(Collection<String> sysInfo) throws IOException {
        parse(sysInfo);
    }

    @Override
    public String get(String key) {
        return sysInfoMap.get(key.toLowerCase());
    }

    @Override
    public void put(String key, String value) {
        sysInfoMap.put(key.toLowerCase(), value);
    }

    protected void parse(Collection<String> sysInfo) throws IOException {
        for (String line : sysInfo) {
            log.debug("line = " + line);

            if (line != null && line.trim().length() > 0) {
                String[] tokens = line.split(":");
                if (tokens.length < 2) {
                    log.debug("ignoring line with two few tokens: " + line);
                    continue;
                }
                put(tokens[0].trim(), tokens[1].trim());
            }
        }
    }

}
