package gov.nasa.ziggy.module.remote;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Subclass of QueueCommandManager that can be used for unit tests. Its qstat() and qdel() methods
 * do nothing (and certainly don't attempt to call a queue management application, which may or may
 * not be available).
 *
 * @author PT
 */
public class QueueCommandManagerForUnitTests extends QueueCommandManager {

    private List<QueueDeleteCommand> queueDeleteCommands = new ArrayList<>();

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
    // When all job IDS are between 100 and 200, return code is 0; all other cases, 1.
    @Override
    public int deleteJobsByJobId(Collection<Long> jobIds) {
        super.deleteJobsByJobId(jobIds);
        for (long jobId : jobIds) {
            if (jobId < 100 || jobId > 200) {
                queueDeleteCommands.add(new QueueDeleteCommand(jobIds, 1));
                return 1;
            }
        }
        queueDeleteCommands.add(new QueueDeleteCommand(jobIds, 0));
        return 0;
    }

    public List<QueueDeleteCommand> getQueueDeleteCommands() {
        return queueDeleteCommands;
    }

    public static class QueueDeleteCommand {

        private final Collection<Long> jobIds;
        private final int returnCode;

        public QueueDeleteCommand(Collection<Long> jobIds, int returnCode) {
            this.jobIds = jobIds;
            this.returnCode = returnCode;
        }

        public Collection<Long> getJobIds() {
            return jobIds;
        }

        public int getReturnCode() {
            return returnCode;
        }
    }
}
