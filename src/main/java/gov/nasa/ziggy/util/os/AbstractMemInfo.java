package gov.nasa.ziggy.util.os;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Forrest Girouard
 * @author PT
 */
public abstract class AbstractMemInfo extends AbstractSysInfo implements MemInfo {
    private static final Logger log = LoggerFactory.getLogger(AbstractMemInfo.class);

    public static final long BYTES_PER_KB = 1024L;

    public AbstractMemInfo(Collection<String> commandOutput) throws IOException {
        super(commandOutput);
    }

    public abstract String getTotalMemoryKey();

    public abstract String getFreeMemoryKey();

    public abstract String getBuffersKey();

    public abstract String getCachedKey();

    public abstract String getCachedSwapKey();

    public abstract String getTotalSwapKey();

    public abstract String getFreeSwapKey();

    @Override
    public long getTotalMemoryKB() {
        return getValueInKb(getTotalMemoryKey());
    }

    @Override
    public long getFreeMemoryKB() {
        return getValueInKb(getFreeMemoryKey());
    }

    @Override
    public long getBuffersKB() {
        return getValueInKb(getBuffersKey());
    }

    @Override
    public long getCachedKB() {
        return getValueInKb(getCachedKey());
    }

    @Override
    public long getCachedSwapedKB() {
        return getValueInKb(getCachedSwapKey());
    }

    @Override
    public long getTotalSwapKB() {
        return getValueInKb(getTotalSwapKey());
    }

    @Override
    public long getFreeSwapKB() {
        return getValueInKb(getFreeSwapKey());
    }

    private long getValueInKb(String key) {
        if (key == null) {
            return -1L;
        }

        long longValue = 0L;
        String value = get(key.toLowerCase());
        if (value != null && value.length() > 0) {
            String[] parts = value.split(" ");
            if (parts.length > 0) {
                try {
                    longValue = Long.parseLong(parts[0]);
                } catch (NumberFormatException ignore) {
                }
                if (parts.length > 1) {
                    if (parts[1].equalsIgnoreCase("GB")) {
                        longValue *= BYTES_PER_KB * BYTES_PER_KB;
                    } else if (parts[1].equalsIgnoreCase("MB") || parts[1].equalsIgnoreCase("M")) {
                        longValue *= BYTES_PER_KB;
                    }
                }
            }
        }

        log.debug(String.format("%s: %,d KB", key, longValue));

        return longValue;
    }
}
