package gov.nasa.ziggy.module.remote;

import java.util.List;

/**
 * Subclass of QueueCommandManager that can be used for unit tests. Its qstat() and qdel() methods
 * do nothing (and certainly don't attempt to call a queue management application, which may or may
 * not be available).
 *
 * @author PT
 */
public class QueueCommandManagerForUnitTests extends QueueCommandManager {

    @Override
    protected List<String> qstat(String commandString, String... strings) {
        return null;
    }

    @Override
    protected void qdel(String commandString) {
    }

    @Override
    public String user() {
        return "user";
    }

    @Override
    public String hostname() {
        return "host";
    }
}
