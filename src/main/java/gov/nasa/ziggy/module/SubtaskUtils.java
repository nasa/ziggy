package gov.nasa.ziggy.module;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author PT
 * @author Todd Klaus
 */
public class SubtaskUtils {
    private static final Logger log = LoggerFactory.getLogger(SubtaskUtils.class);

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
        if (logStreamIdentifier == null || logStreamIdentifier.isEmpty()) {
            MDC.remove("logStreamIdentifier");
        } else {
            MDC.put("logStreamIdentifier", "(" + logStreamIdentifier + ")");
        }
    }

    public static void clearStaleAlgorithmStates(File taskDir) {
        log.info("Finding and clearing stale PROCESSING or FAILED subtask states");
        SubtaskDirectoryIterator it = new SubtaskDirectoryIterator(taskDir);
        while (it.hasNext()) {
            File subtaskDir = it.next().getSubtaskDir();
            new AlgorithmStateFiles(subtaskDir).clearStaleState();
        }
    }

}
