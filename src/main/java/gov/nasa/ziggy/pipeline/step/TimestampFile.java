package gov.nasa.ziggy.pipeline.step;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * @author Todd Klaus
 */
public abstract class TimestampFile {
    private static final Logger log = LoggerFactory.getLogger(TimestampFile.class);

    private static PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    public enum Event {
        ARRIVE_COMPUTE_NODES {
            @Override
            public long timestampFromFile(File directory, boolean errorIfMissing) {
                return minTimestamp(directory, this, errorIfMissing);
            }

            @Override
            public long defaultValue(File directory, PipelineTask pipelineTask) {
                return pipelineTaskDataOperations().startTimestamp(pipelineTask);
            }
        },
        QUEUED {
            @Override
            public long timestampFromFile(File directory, boolean errorIfMissing) {
                return minTimestamp(directory, this, errorIfMissing);
            }

            @Override
            public long defaultValue(File directory, PipelineTask pipelineTask) {
                return pipelineTaskDataOperations().startTimestamp(pipelineTask);
            }
        },
        START {
            @Override
            public long timestampFromFile(File directory, boolean errorIfMissing) {
                return minTimestamp(directory, this, errorIfMissing);
            }

            @Override
            public long defaultValue(File directory, PipelineTask pipelineTask) {
                return pipelineTaskDataOperations().startTimestamp(pipelineTask);
            }
        },
        FINISH {
            @Override
            public long timestampFromFile(File directory, boolean errorIfMissing) {
                return maxTimestamp(directory, this, errorIfMissing);
            }

            @Override
            public long defaultValue(File directory, PipelineTask pipelineTask) {
                return pipelineTaskDataOperations().endTimestamp(pipelineTask);
            }
        },
        SUBTASK_START {
            @Override
            public long timestampFromFile(File directory, boolean errorIfMissing) {
                return minTimestamp(directory, this, errorIfMissing);
            }

            @Override
            public long defaultValue(File directory, PipelineTask pipelineTask) {
                return pipelineTaskDataOperations().startTimestamp(pipelineTask);
            }
        },
        SUBTASK_FINISH {
            @Override
            public long timestampFromFile(File directory, boolean errorIfMissing) {
                return maxTimestamp(directory, this, errorIfMissing);
            }

            @Override
            public long defaultValue(File directory, PipelineTask pipelineTask) {
                return pipelineTaskDataOperations().endTimestamp(pipelineTask);
            }
        };

        /**
         * Returns a time if there is one timestamp file, or more than one. In the event that there
         * are no timestamp files, this can either throw an exception or return 0.
         */
        public abstract long timestampFromFile(File directory, boolean errorIfMissing);

        /** Returns a value from the {@link PipelineTask}. */
        public abstract long defaultValue(File directory, PipelineTask pipelineTask);
    }

    public static boolean create(File directory, Event name) {
        return create(directory, name, System.currentTimeMillis());
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

    // For testing only.
    static long timestamp(File directory, final Event name) {
        return timestamp(directory, name, true);
    }

    private static long timestamp(File directory, final Event name, boolean errorIfMissing) {
        File[] files = eventFiles(directory, name, errorIfMissing);

        if (files == null) {
            return 0;
        }

        if (files.length > 1) {
            throw new PipelineException("Found more than one files that match event: " + name);
        }
        return timestampValue(files[0]);
    }

    private static File[] eventFiles(File directory, final Event name, boolean errorIfMissing) {
        File[] files = directory
            .listFiles((FileFilter) f -> f.getName().startsWith(name.toString()) && f.isFile());

        if (files.length == 0) {
            if (!errorIfMissing) {
                log.warn("Unable to find {} timestamp file in directory {}", name.toString(),
                    directory.toString());
                return null;
            }
            throw new PipelineException("Found zero files that match event: " + name);
        }

        return files;
    }

    private static long maxTimestamp(File directory, final Event name, boolean errorIfMissing) {
        File[] files = eventFiles(directory, name, errorIfMissing);

        if (files == null) {
            return 0;
        }
        return Collections.max(timestampValues(files));
    }

    private static long minTimestamp(File directory, final Event name, boolean errorIfMissing) {
        File[] files = eventFiles(directory, name, errorIfMissing);

        if (files == null) {
            return 0;
        }
        return Collections.min(timestampValues(files));
    }

    private static List<Long> timestampValues(File[] timestampFiles) {
        List<Long> timestampValues = new ArrayList<>();
        for (File timestampFile : timestampFiles) {
            timestampValues.add(timestampValue(timestampFile));
        }
        return timestampValues;
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
     * Returns elapsed time between two events. If an event file is missing, the appropriate default
     * replacement value is used.
     */
    public static long elapsedTimeMillis(File directory, final Event startEvent,
        final Event finishEvent, PipelineTask pipelineTask) {
        return eventTimeMillis(directory, finishEvent, pipelineTask)
            - eventTimeMillis(directory, startEvent, pipelineTask);
    }

    public static long eventTimeMillis(File directory, final Event event,
        PipelineTask pipelineTask) {
        return TimestampFile.exists(directory, event) ? event.timestampFromFile(directory, false)
            : event.defaultValue(directory, pipelineTask);
    }

    private static PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    // For testing only.
    static void setPipelineTaskDataOperations(PipelineTaskDataOperations dataOperations) {
        pipelineTaskDataOperations = dataOperations;
    }
}
