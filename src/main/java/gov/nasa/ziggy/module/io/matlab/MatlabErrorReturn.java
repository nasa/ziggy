package gov.nasa.ziggy.module.io.matlab;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.module.io.ProxyIgnore;

/**
 * This class is a Persistable class that mirrors the structure of the MATLAB lasterror struct.
 * <p>
 * WARNING: Do not change the names of any of these fields, they must match the names in the MATLAB
 * lasterror struct.
 *
 * @author Todd Klaus
 */
public class MatlabErrorReturn implements Persistable {
    @ProxyIgnore
    private static final Logger log = LoggerFactory.getLogger(MatlabErrorReturn.class);

    private String message;
    private String identifier;
    private List<MatlabStack> stack;

    public MatlabErrorReturn() {
    }

    public void logStackTrace() {
        log.error("MATLAB Stack Trace: msg=" + message + ", id=" + identifier);

        for (MatlabStack stackFrame : stack) {
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

    public List<MatlabStack> getStack() {
        return stack;
    }

    public void setStack(List<MatlabStack> stack) {
        this.stack = stack;
    }
}
