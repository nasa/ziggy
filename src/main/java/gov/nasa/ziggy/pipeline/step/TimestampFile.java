package gov.nasa.ziggy.pipeline.step;

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
import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.SystemProxy;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * @author Todd Klaus
 */
public abstract class TimestampFile {
    private static final Logger log = LoggerFactory.getLogger(TimestampFile.class);

    public enum Event {
        ARRIVE_COMPUTE_NODES, QUEUED, START, FINISH, SUBTASK_START {
            @Override
            public boolean isTaskLevelTimestamp() {
                return false;
            }
        },
        SUBTASK_FINISH {
            @Override
            public boolean isTaskLevelTimestamp() {
                return false;
            }
        };

        public boolean isTaskLevelTimestamp() {
            return true;
        }
    }

    public static boolean create(File directory, Event name) {
        return create(directory, name, SystemProxy.currentTimeMillis());
    }

    public static boolean createIfAbsent(File directory, Event name) {
        return exists(directory, name) ? true : create(directory, name);
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    static boolean create(File directory, Event name, long timestamp) {
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

    public static boolean deleteAllTaskLevelTimestamps(File directory) {
        boolean allTimestampStatus = true;
        for (Event event : Event.values()) {
            if (!event.isTaskLevelTimestamp()) {
                continue;
            }
            allTimestampStatus = allTimestampStatus && delete(directory, event);
        }
        return allTimestampStatus;
    }

    private static File eventFile(File directory, final Event name) {
        File[] files = directory
            .listFiles((FileFilter) f -> f.getName().startsWith(name.toString()) && f.isFile());

        if (files.length != 1) {
            log.warn("Unable to find unique {} timestamp file in directory {}", name.toString(),
                directory.toString());
            return null;
        }
        return files[0];
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    private static long timestampValue(File timestampFile) {
        String[] elements = timestampFile.getName().split("\\.");

        if (elements.length != 2) {
            throw new PipelineException("Unable to parse timestamp file: " + timestampFile.getName()
                + ", numElements = " + elements.length);
        }
        try {
            return Long.parseLong(elements[1]);
        } catch (NumberFormatException e) {
            // This can never occur. By construction, the timestamp files are written with
            // valid long integer values.
            throw new AssertionError(e);
        }
    }

    /**
     * Returns elapsed time between two events. If the start event is missing, an elapsed time of -1
     * is returned. If the finish event is missing, the current system time is used for the finish
     * time.
     */
    public static long elapsedTimeMillis(File directory, final Event startEvent,
        final Event finishEvent) {
        long fileStartTime = eventTimeMillis(directory, startEvent);
        if (fileStartTime < 0) {
            return -1;
        }
        long fileEndTime = eventTimeMillis(directory, finishEvent);
        long endTime = fileEndTime > 0 ? fileEndTime : SystemProxy.currentTimeMillis();
        return endTime - fileStartTime;
    }

    public static boolean elapsedTimeValid(File directory, final Event startEvent,
        final Event finishEvent) {
        return exists(directory, startEvent) && exists(directory, finishEvent);
    }

    public static long eventTimeMillis(File directory, final Event event) {
        File eventFile = eventFile(directory, event);
        return eventFile != null ? timestampValue(eventFile) : -1L;
    }
}
