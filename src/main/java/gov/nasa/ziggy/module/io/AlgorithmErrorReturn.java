package gov.nasa.ziggy.module.io;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a Persistable class that mirrors the structure of the MATLAB lasterror struct. It
 * is used by all algorithms, regardless of language, in order to read and display the stack trace
 * from algorithms in the log.
 *
 * @author Todd Klaus
 * @author PT
 */
public class AlgorithmErrorReturn implements Persistable {

    @ProxyIgnore
    private static final Logger log = LoggerFactory.getLogger(AlgorithmErrorReturn.class);

    private String message;
    private String identifier;
    private List<AlgorithmStack> stack;

    public AlgorithmErrorReturn() {
    }

    public void logStackTrace() {
        log.error("Algorithm stack trace for msg={}, id={}", message, identifier);

        for (AlgorithmStack stackFrame : stack) {
            stackFrame.logStackTrace();
        }
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<AlgorithmStack> getStack() {
        return stack;
    }

    public void setStack(List<AlgorithmStack> stack) {
        this.stack = stack;
    }

    /** Models a single entry in an algorithm stack trace. */
    private static class AlgorithmStack implements Persistable {

        private String file;
        private String name;
        private int line;

        public void logStackTrace() {
            log.error("    Algorithm stack trace:    file={}, name={}, line={}", file, name, line);
        }
    }
}
