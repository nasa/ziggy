package gov.nasa.ziggy.module.io.matlab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.module.io.ProxyIgnore;

/**
 * This class is a Persistable class that mirrors the structure of the MATLAB lasterror.stack
 * struct.
 * <p>
 * WARNING: Do not change the names of any of these fields, they must match the names in the MATLAB
 * lasterror.stack struct.
 *
 * @author Todd Klaus
 */
public class MatlabStack implements Persistable {
    @ProxyIgnore
    private static final Logger log = LoggerFactory.getLogger(MatlabStack.class);

    private String file;
    private String name;
    private int line;

    public MatlabStack() {
    }

    public void logStackTrace() {
        log.error("MATLAB Stack Trace:    file=" + file + ", name=" + name + ", line=" + line);
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public int getLine() {
        return line;
    }

    public void setLine(int line) {
        this.line = line;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
