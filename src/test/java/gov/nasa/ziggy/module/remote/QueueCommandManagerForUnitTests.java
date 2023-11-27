package gov.nasa.ziggy.module.remote;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.module.StateFile;

/**
 * Subclass of QueueCommandManager that can be used for unit tests. Its qstat() and qdel() methods
 * do nothing (and certainly don't attempt to call a queue management application, which may or may
 * not be available).
 *
 * @author PT
 */
public class QueueCommandManagerForUnitTests extends QueueCommandManager {

    private Map<Long, QueueDeleteCommand> queueDeleteCommands = new HashMap<>();

    @Override
    protected List<String> qstat(String commandString, String... strings) {
        return null;
    }

    @Override
    protected int qdel(String commandString) {
        return 0;
    }

    @Override
    public String user() {
        return "user";
    }

    @Override
    public String hostname() {
        return "host";
    }

    // Store information about a call for a task deletion, and return an appropriate return code.
    // For task IDs between 101 and 200, return code is 1; all other cases, 0.
    @Override
    public int deleteJobsForStateFile(StateFile stateFile) {
        long taskId = stateFile.getPipelineTaskId();
        int returnCode = taskId > 100 && taskId <= 200 ? 1 : 0;
        queueDeleteCommands.put(taskId, new QueueDeleteCommand(stateFile, returnCode));
        return returnCode;
    }

    public Map<Long, QueueDeleteCommand> getQueueDeleteCommands() {
        return queueDeleteCommands;
    }

    public static class QueueDeleteCommand {

        private final StateFile stateFile;
        private final int returnCode;

        public QueueDeleteCommand(StateFile stateFile, int returnCode) {
            this.stateFile = stateFile;
            this.returnCode = returnCode;
        }

        public StateFile getStateFile() {
            return stateFile;
        }

        public int getReturnCode() {
            return returnCode;
        }
    }
}
