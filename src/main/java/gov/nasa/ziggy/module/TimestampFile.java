package gov.nasa.ziggy.module;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * @author Todd Klaus
 */
public abstract class TimestampFile {
    private static final Logger log = LoggerFactory.getLogger(TimestampFile.class);

    public enum Event {
        ARRIVE_COMPUTE_NODES, QUEUED, START, FINISH, SUBTASK_START, SUBTASK_FINISH
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public static boolean create(File directory, Event name, long timestamp) {
        if (!delete(directory, name)) {
            return false;
        }
        String filename = String.format("%s.%d", name.toString(), timestamp);
        File f = new File(directory, filename);
        try {
            boolean result = f.createNewFile();
            f.setReadable(true, false);
            return result;
        } catch (IOException e) {
            log.warn(
                String.format("failed to create timestamp file, dir=%s, file=%s, caught e = %s",
                    directory, filename, e),
                e);
            return false;
        }
    }

    public static boolean exists(File directory, Event name) {
        return !find(directory, name).isEmpty();
    }

    public static Set<Path> find(File directory, Event name) {
        return ZiggyFileUtils.listFiles(directory.toPath(), pattern(name));
    }

    private static String pattern(Event name) {
        return name.toString() + "\\.[0-9]+";
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public static boolean delete(File directory, Event name) {
        Set<Path> files = find(directory, name);
        for (Path file : files) {
            try {
                boolean deleted = Files.deleteIfExists(file);
                if (!deleted) {
                    log.warn("Failed to delete existing timestamp file, dir={}, file=}", directory,
                        file);
                    return false;
                }
            } catch (IOException e) {
                log.error("Exception occurred when deleting {}", file.toString(), e);
                return false;
            }
        }
        return true;
    }

    public static boolean create(File directory, Event name) {
        return create(directory, name, System.currentTimeMillis());
    }

    public static long timestamp(File directory, final Event name) {
        return timestamp(directory, name, true);
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public static long timestamp(File directory, final Event name, boolean errorIfMissing) {
        File[] files = directory
            .listFiles((FileFilter) f -> f.getName().startsWith(name.toString()) && f.isFile());

        if (files.length == 0) {
            if (!errorIfMissing) {
                log.warn("Unable to find {} timestamp file in directory {}", name.toString(),
                    directory.toString());
                return 0;
            }
            throw new PipelineException("Found zero files that match event: " + name);
        }

        if (files.length > 1) {
            throw new PipelineException("Found more than one files that match event: " + name);
        }

        String filename = files[0].getName();
        String[] elements = filename.split("\\.");

        if (elements.length != 2) {
            throw new PipelineException("Unable to parse timestamp file: " + filename
                + ", numElements = " + elements.length);
        }

        String timeString = elements[1];
        long timeMillis = -1;
        try {
            timeMillis = Long.parseLong(timeString);
        } catch (NumberFormatException e) {
            // This can never occur. By construction, the timestamp files are written with
            // valid long integer values.
            throw new AssertionError(e);
        }

        return timeMillis;
    }

    /**
     * Returns the elapsed time between the specified events. Assumes that the specified directory
     * contains timestamp files for the specified events.
     *
     * @param directory
     * @param startEvent
     * @param finishEvent
     * @return
     */
    public static long elapsedTimeMillis(File directory, final Event startEvent,
        final Event finishEvent) {
        long startTime = timestamp(directory, startEvent);
        long finishTime = timestamp(directory, finishEvent);

        if (startTime == -1 || finishTime == -1) {
            // At least one of the events was missing or unparsable.
            log.warn("Missing or invalid timestamp files, startTime={}, finishTime={}", startTime,
                finishTime);
            return 0;
        }
        return finishTime - startTime;
    }
}
