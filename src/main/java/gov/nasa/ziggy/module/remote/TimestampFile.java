package gov.nasa.ziggy.module.remote;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;

/**
 * @author Todd Klaus
 */
public abstract class TimestampFile {
    private static final Logger log = LoggerFactory.getLogger(TimestampFile.class);

    public enum Event {
        ARRIVE_PFE, QUEUED_PBS, PBS_JOB_START, PBS_JOB_FINISH, SUB_TASK_START, SUB_TASK_FINISH
    }

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

    public static boolean delete(File directory, Event name) {
        // delete any existing files with this prefix
        String prefix = name.toString();

        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.getName().startsWith(prefix)) {
                boolean deleted = file.delete();
                if (!deleted) {
                    log.warn(
                        String.format("failed to delete existing timestamp file, dir=%s, file=%s",
                            directory, file));
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean create(File directory, Event name) {
        return create(directory, name, System.currentTimeMillis());
    }

    public static long timestamp(File directory, final Event name) {
        File[] files = directory
            .listFiles((FileFilter) f -> f.getName().startsWith(name.toString()) && f.isFile());

        if (files.length == 0) {
            throw new PipelineException("Found zero files that match event:" + name);
        }

        if (files.length > 1) {
            throw new PipelineException("Found more than one files that match event:" + name);
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
            throw new PipelineException(
                "Unable to parse timestamp file: " + filename + ", timeString = " + timeString);
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
            // at least one of the events was missing or unparsable
            log.warn(
                String.format("Missing or invalid timestamp files, startTime=%s, finishTime=%s",
                    startTime, finishTime));
            return 0;
        }
        return finishTime - startTime;
    }
}
