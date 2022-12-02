package gov.nasa.ziggy.util.os;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines the total memory for the current hardware at runtime under the Mac OS X operating
 * system.
 *
 * @author Forrest Girouard
 * @author PT
 */
public class MacOSXMemInfo extends AbstractMemInfo {
    private static final Logger log = LoggerFactory.getLogger(LinuxMemInfo.class);

    private static final String TOP_COMMAND = "/usr/bin/top -F -l 1 -n 0 -S";

    private static final String TOTAL_MEMORY = "TotalMemory";
    private static final String FREE_MEMORY = "FreeMemory";
    private static final String TOTAL_SWAP = "TotalSwap";
    private static final String FREE_SWAP = "FreeSwap";

    private static final String UNAVAILABLE = "Unavailable";

    public MacOSXMemInfo() throws IOException {
        super(commandOutput(TOP_COMMAND));
    }

    @Override
    protected void parse(Collection<String> topOutput) throws IOException {
        for (String line : topOutput) {
            log.debug("line = " + line);

            if (line != null && line.trim().length() > 0) {
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length < 2) {
                    log.debug("ignoring line with two few tokens: " + line);
                    continue;
                }
                String field = tokens[0].trim().toLowerCase();
                if (field.startsWith("physmem")) {
                    put(TOTAL_MEMORY, valueOf(tokens[1]) + valueOf(tokens[5]));
                    put(FREE_MEMORY, valueOf(tokens[5]));
                } else if (field.startsWith("swap")) {
                    put(TOTAL_SWAP, valueOf(tokens[1]) + valueOf(tokens[3]));
                    put(FREE_SWAP, valueOf(tokens[3]));
                }
            }
        }
    }

    private static int valueOf(String intString) {
        int value = 0;
        if (intString.endsWith("B")) {
            value += Integer.valueOf(intString.substring(0, intString.length() - 1)) / 1024;
        } else if (intString.endsWith("K")) {
            value += Integer.valueOf(intString.substring(0, intString.length() - 1));
        } else if (intString.endsWith("M")) {
            value += Integer.valueOf(intString.substring(0, intString.length() - 1)) * 1024;
        } else if (intString.endsWith("G")) {
            value += Integer.valueOf(intString.substring(0, intString.length() - 1)) * 1024 * 1024;
        } else {
            value += Integer.valueOf(intString);
        }
        return value;
    }

    private void put(String key, int value) {
        put(key.toLowerCase(), String.format("%d KB", value));
    }

    @Override
    public String getBuffersKey() {
        return UNAVAILABLE;
    }

    @Override
    public String getCachedKey() {
        return UNAVAILABLE;
    }

    @Override
    public String getCachedSwapKey() {
        return UNAVAILABLE;
    }

    @Override
    public String getFreeMemoryKey() {
        return FREE_MEMORY;
    }

    @Override
    public String getFreeSwapKey() {
        return FREE_SWAP;
    }

    @Override
    public String getTotalMemoryKey() {
        return TOTAL_MEMORY;
    }

    @Override
    public String getTotalSwapKey() {
        return TOTAL_SWAP;
    }
}
