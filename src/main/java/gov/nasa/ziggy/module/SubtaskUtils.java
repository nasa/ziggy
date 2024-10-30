package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * @author PT
 * @author Todd Klaus
 */
public class SubtaskUtils {
    private static final Logger log = LoggerFactory.getLogger(SubtaskUtils.class);

    public static final String SUBTASK_DIR_PREFIX = "st-";
    public static final String SUBTASK_DIR_REGEXP = SUBTASK_DIR_PREFIX + "(\\d+)";
    public static final Pattern SUBTASK_DIR_PATTERN = Pattern.compile(SUBTASK_DIR_REGEXP);

    public static Path subtaskDirectory(Path taskWorkingDir, int subtaskIndex) {
        return taskWorkingDir.resolve(subtaskDirName(subtaskIndex));
    }

    public static String subtaskDirName(int subtaskIndex) {
        return SUBTASK_DIR_PREFIX + subtaskIndex;
    }

    /**
     * Sets a thread-specific string that will be included in log messages. Specifically, the
     * subtask directory, enclosed in parentheses (i.e., "(st-123)") is set as the thread-specific
     * value of the logStreamIdentifier key, so all log messages generated in the thread have the
     * subtask directory in the message. This allows users to examine an algorithm log (which can
     * contain messages from more than one subtask) and determine which subtasks generated which
     * messages.
     */
    public static void putLogStreamIdentifier(File workingDir) {
        putLogStreamIdentifier(workingDir.getName());
    }

    /**
     * Sets a thread-specific string that will be included in log messages. Specifically, the
     * subtask directory, enclosed in parentheses (i.e., "(st-123)") is set as the thread-specific
     * value of the logStreamIdentifier key, so all log messages generated in the thread have the
     * subtask directory in the message. This allows users to examine an algorithm log (which can
     * contain messages from more than one subtask) and determine which subtasks generated which
     * messages.
     */
    public static void putLogStreamIdentifier(String logStreamIdentifier) {
        if (StringUtils.isBlank(logStreamIdentifier)) {
            MDC.remove("logStreamIdentifier");
        } else {
            MDC.put("logStreamIdentifier", "(" + logStreamIdentifier + ")");
        }
    }

    public static void clearStaleAlgorithmStates(File taskDir) {
        log.info("Removing stale PROCESSING state from task directory");
        new AlgorithmStateFiles(taskDir).clearStaleState();
        log.info("Finding and clearing stale PROCESSING or FAILED subtask states");
        SubtaskDirectoryIterator it = new SubtaskDirectoryIterator(taskDir);
        while (it.hasNext()) {
            File subtaskDir = it.next().getSubtaskDir();
            new AlgorithmStateFiles(subtaskDir).clearStaleState();
        }
    }

    /**
     * Returns the subtask index for the current subtask. Assumes that the working directory is the
     * subtask directory.
     */
    public static int subtaskIndex() {
        Matcher m = SUBTASK_DIR_PATTERN
            .matcher(DirectoryProperties.workingDir().getFileName().toString());
        if (m.matches()) {
            return Integer.parseInt(m.group(1));
        }
        throw new PipelineException("Directory " + DirectoryProperties.workingDir().toString()
            + " not a subtask directory");
    }

    /** Returns a list of subtask directories in a task dir. */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static List<Path> subtaskDirectories(Path taskDir) {
        try (Stream<Path> dirStream = Files.list(taskDir)) {
            return dirStream.filter(Files::isDirectory)
                .filter(s -> SUBTASK_DIR_PATTERN.matcher(s.getFileName().toString()).matches())
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Creates a subtask for a given task directory and subtask index. */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static Path createSubtaskDirectory(Path taskWorkingDir, int subtaskIndex) {
        try {
            return Files.createDirectories(subtaskDirectory(taskWorkingDir, subtaskIndex));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
