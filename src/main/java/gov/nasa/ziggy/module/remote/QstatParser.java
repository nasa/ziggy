package gov.nasa.ziggy.module.remote;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;

/**
 * Provides parsing capabilities for the outputs from a qstat command.
 *
 * @author PT
 */
public class QstatParser {

    private String owner;
    private String serverName;
    private QueueCommandManager cmdManager;

    /** Constructor for general use. */
    public QstatParser() {
        this(QueueCommandManager.newInstance());
    }

    /**
     * Constructor for test.
     */
    QstatParser(QueueCommandManager cmdManager) {
        this(cmdManager.user(), cmdManager.hostname(), cmdManager);
    }

    private QstatParser(String owner, String serverName, QueueCommandManager cmdManager) {
        this.owner = owner;
        this.serverName = serverName;
        if (cmdManager == null) {
            this.cmdManager = QueueCommandManager.newInstance();
        } else {
            this.cmdManager = cmdManager;
        }
    }

    /** Populates the Job IDs in a collection of {@link RemoteJobInformation} instances. */
    public void populateJobIds(PipelineTask pipelineTask,
        List<RemoteJobInformation> remoteJobsInformation) {
        Map<String, Long> jobIdByName = jobIdByName(pipelineTask.taskBaseName());
        for (RemoteJobInformation remoteJobInformation : remoteJobsInformation) {
            remoteJobInformation.setJobId(jobIdByName.get(remoteJobInformation.getJobName()));
        }
    }

    public RemoteJobInformation remoteJobInformation(RemoteJob remoteJob) {
        return cmdManager.remoteJobInformation(remoteJob);
    }

    /**
     * Uses the output from qstat to generate a {@link Map} from the job name to the job ID. The
     * process eliminates any jobs that have the wrong username or hostname, since those are
     * obviously somebody else's jobs.
     */
    public Map<String, Long> jobIdByName(String taskName) {

        Map<String, Long> jobIdByName = new HashMap<>();

        // get all the qstat entries, if any, for the specified task name
        List<String> qstatLines = cmdManager.getQstatInfoByTaskName(owner, taskName);
        if (qstatLines.isEmpty() || qstatLines.size() == 1 && qstatLines.get(0).isBlank()) {
            return jobIdByName;
        }

        // generate a QstatEntry for each string
        List<QstatEntry> qstatEntries = qstatLines.stream()
            .map(QstatEntry::new)
            .collect(Collectors.toList());

        // get the server names for all the jobs
        Map<Long, String> jobIdToServerMap = cmdManager.serverNames(
            qstatEntries.stream().map(QstatEntry::getId).sorted().collect(Collectors.toList()));

        // strip out the jobs with the wrong server names
        List<Long> jobsWithCorrectServer = jobIdToServerMap.keySet()
            .stream()
            .filter(s -> jobIdToServerMap.get(s).equals(serverName))
            .collect(Collectors.toList());

        // if the list is empty, return a null
        if (jobsWithCorrectServer.isEmpty()) {
            return jobIdByName;
        }

        qstatEntries = qstatEntries.stream()
            .filter(s -> jobsWithCorrectServer.contains(s.getId()))
            .collect(Collectors.toList());

        for (QstatEntry qstatEntry : qstatEntries) {
            jobIdByName.put(qstatEntry.name, qstatEntry.id);
        }
        return jobIdByName;
    }

    private static class QstatEntry {

        private final long id;
        private final String name;

        /**
         * Constructs a QstatEntry instance from a line out of a qstat command.
         *
         * @param qstatLine Line from the output of the qstat command
         */
        public QstatEntry(String qstatLine) {

            String[] qstatLineParts = qstatLine.split("\\s+");

            // The job ID has an ID # followed by ".<name>".
            String jobId = qstatLineParts[QueueCommandManager.ID_INDEX];
            String[] jobIdParts = jobId.split("\\.");
            id = Long.parseLong(jobIdParts[0]);

            // The name, owner, and status are simple strings.
            name = qstatLineParts[QueueCommandManager.NAME_INDEX];
        }

        public long getId() {
            return id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            QstatEntry other = (QstatEntry) obj;
            if (id != other.id) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return id + "";
        }
    }
}
