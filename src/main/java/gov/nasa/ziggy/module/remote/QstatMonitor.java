package gov.nasa.ziggy.module.remote;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.JobMonitor;
import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Interrogates the state of running / completed / failed jobs on a remote system via the qstat
 * command line in order to identify issues with those jobs that would not otherwise be made
 * apparent to pipeline operators, and also to support use of the console or picli to delete jobs
 * that are queued or running.
 *
 * @author PT
 */
public class QstatMonitor implements JobMonitor {

    private String owner;
    private String serverName;
    private QueueCommandManager cmdManager;

    private static final Logger log = LoggerFactory.getLogger(QstatMonitor.class);

    // Maps from a given pipeline task name to the corresponding QstatEntry
    private Map<String, Set<QstatEntry>> taskNameQstatEntryMap = null;

    /**
     * Constructor. Forces the QstatMonitor to get its own instance of QueueCommandManager.
     */
    public QstatMonitor(String owner, String serverName) {
        this(owner, serverName, null);
    }

    /**
     * Constructor. Uses a supplied instance of the QueueCommandManager to supply the username,
     * hostname, and queue command management for this instance.
     */
    public QstatMonitor(QueueCommandManager cmdManager) {
        this(cmdManager.user(), cmdManager.hostname(), cmdManager);
    }

    private QstatMonitor(String owner, String serverName, QueueCommandManager cmdManager) {
        this.owner = owner;
        this.serverName = serverName;
        if (cmdManager == null) {
            this.cmdManager = QueueCommandManager.newInstance();
        } else {
            this.cmdManager = cmdManager;
        }
        taskNameQstatEntryMap = new HashMap<>();
    }

    /**
     * Start monitoring the job that corresponds to a particular state file.
     */
    @Override
    public void addToMonitoring(StateFile stateFile) {
        addToMonitoring(stateFile.taskBaseName());
    }

    @Override
    public void addToMonitoring(PipelineTask pipelineTask) {
        addToMonitoring(pipelineTask.taskBaseName());
    }

    private void addToMonitoring(String taskName) {

        // if the task is already in the map, we don't need to do anything else
        if (!taskNameQstatEntryMap.containsKey(taskName)) {
            log.info("Adding PBS job " + taskName + " to qstat monitor");
            taskNameQstatEntryMap.put(taskName, Collections.emptySet());
        }
    }

    /**
     * Terminate monitoring of a job that corresponds to a particular state file.
     */
    @Override
    public void endMonitoring(StateFile stateFile) {
        log.info("Removing PBS job " + stateFile.taskBaseName() + " from qstart monitor");
        taskNameQstatEntryMap.remove(stateFile.taskBaseName());
    }

    /**
     * Updates the contents of the monitor. Tasks that had not yet been assigned jobs are checked to
     * see whether jobs are available. All task status values and elapsed times are updated.
     */
    @Override
    public void update() {

        // update the values of any jobs that already have them
        updateQstatEntries();

        // for any task that doesn't yet have a job, check for new job assignments
        for (String taskName : taskNameQstatEntryMap.keySet()) {
            if (taskNameQstatEntryMap.get(taskName).isEmpty()) {
                Set<QstatEntry> newEntries = qstatEntriesForTask(taskName);
                if (!newEntries.isEmpty()) {
                    log.info(
                        "Task " + taskName + " matched with PBS jobs " + newEntries.toString());
                }
                taskNameQstatEntryMap.replace(taskName, newEntries);
            }
        }
    }

    @Override
    public Set<Long> allIncompleteJobIds(PipelineTask pipelineTask) {
        return allIncompleteJobIds(pipelineTask.taskBaseName());
    }

    @Override
    public Set<Long> allIncompleteJobIds(StateFile stateFile) {
        return allIncompleteJobIds(stateFile.taskBaseName());
    }

    public Set<Long> allIncompleteJobIds(String taskName) {
        Set<QstatEntry> qstatEntries = taskNameQstatEntryMap.get(taskName);
        if (qstatEntries.isEmpty()) {
            return new HashSet<>();
        }
        Set<Long> ids = new TreeSet<>();
        for (QstatEntry entry : qstatEntries) {
            ids.add(entry.getId());
        }
        return ids;
    }

    /**
     * Constructs QstatEntry instances for a given job based on its name.
     *
     * @param taskName Name of the pipeline task.
     * @return QstatEntry instance for the given job. Note that in addition to the job name, the job
     * owner and the server from which it was started must match the values in the QstatMonitor. The
     * return can also be null under certain circumstances, in particular the case of a job that has
     * been submitted via qsub but which has not yet completed the process of queueing. + * @return
     * a non-null {@link Set}.
     */
    private Set<QstatEntry> qstatEntriesForTask(String taskName) {

        // get all the qstat entries, if any, for the specified task name
        List<String> qstatLines = cmdManager.getQstatInfoByTaskName(owner, taskName);
        if (qstatLines.isEmpty() || qstatLines.size() == 1 && qstatLines.get(0).isBlank()) {
            return Collections.emptySet();
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
            return Collections.emptySet();
        }

        qstatEntries = qstatEntries.stream()
            .filter(s -> jobsWithCorrectServer.contains(s.getId()))
            .collect(Collectors.toList());

        // Generate a list of unique job names
        List<String> uniqueJobNames = qstatEntries.stream()
            .map(QstatEntry::getName)
            .distinct()
            .collect(Collectors.toList());

        Set<Long> jobIds = new HashSet<>();
        for (String name : uniqueJobNames) {
            // Because of restarts, it is possible for there to be > 1 job with a given
            // name, owner, and server. In this case, find the one with the max value
            // of the job ID
            long jobId = qstatEntries.stream()
                .filter(s -> s.getName().equals(name))
                .map(QstatEntry::getId)
                .max(Long::compare)
                .get();
            jobIds.add(jobId);
        }

        return qstatEntries.stream()
            .filter(s -> jobIds.contains(s.getId()))
            .collect(Collectors.toSet());
    }

    private void updateQstatEntries() {

        // construct a list of job IDs
        List<Long> jobIds = new ArrayList<>();
        Set<QstatEntry> allQstatEntries = new HashSet<>();
        for (String taskName : taskNameQstatEntryMap.keySet()) {
            Set<QstatEntry> qstatEntries = taskNameQstatEntryMap.get(taskName);
            if (!qstatEntries.isEmpty()) {
                jobIds.addAll(
                    qstatEntries.stream().map(QstatEntry::getId).collect(Collectors.toList()));
                allQstatEntries.addAll(qstatEntries);
            }
        }

        if (jobIds.isEmpty()) {
            return;
        }

        jobIds = jobIds.stream().sorted(Long::compare).collect(Collectors.toList());
        // get the updates
        Map<String, String> jobNameQstatStringMap = cmdManager.getQstatInfoByJobNameMap(jobIds);

        // apply the updates
        for (QstatEntry entry : allQstatEntries) {
            String qstatLine = jobNameQstatStringMap.get(entry.getName());
            if (!StringUtils.isBlank(qstatLine)) {
                entry.updateFromQstatString(qstatLine);
            }
        }
    }

    /**
     * Determines whether a particular task is finished based on the status of its jobs in qstat
     *
     * @param stateFile StateFile for the task
     * @return true all jobs have status "F" indicating completion or "E" indicating exiting, false
     * otherwise
     */
    @Override
    public boolean isFinished(StateFile stateFile) {
        boolean isFinished = true;
        Set<QstatEntry> entries = taskNameQstatEntryMap.get(stateFile.taskBaseName());
        if (entries.isEmpty()) {
            return false;
        }
        for (QstatEntry entry : entries) {
            String stat = entry.getStatus();
            boolean jobFinished = stat.equals("E") || stat.equals("F");
            isFinished = isFinished && jobFinished;
        }
        return isFinished;
    }

    /**
     * Retrieves the exit status for a task's jobs given its state file
     *
     * @param stateFile state file for the task of interest
     */
    @Override
    public Map<Long, Integer> exitStatus(StateFile stateFile) {
        Map<Long, Integer> jobIdExitStatusMap = new HashMap<>();
        Collection<Long> jobIds = idFromStateFile(stateFile);
        for (long jobId : jobIds) {
            Integer exitStatus = cmdManager.exitStatus(jobId);
            if (exitStatus != null) {
                jobIdExitStatusMap.put(jobId, exitStatus);
            }
        }
        return jobIdExitStatusMap;
    }

    /**
     * Retrieves the exit comment for a job given its state file.
     *
     * @param stateFile state file for the task of interest.
     */
    @Override
    public Map<Long, String> exitComment(StateFile stateFile) {
        Map<Long, String> jobIdExitCommentMap = new HashMap<>();
        Collection<Long> jobIds = idFromStateFile(stateFile);
        for (long jobId : jobIds) {
            String exitComment = cmdManager.exitComment(jobId);
            if (!StringUtils.isBlank(exitComment)) {
                jobIdExitCommentMap.put(jobId, exitComment);
            }
        }
        return jobIdExitCommentMap;
    }

    private Set<Long> idFromStateFile(StateFile stateFile) {
        String taskName = stateFile.taskBaseName();
        Set<QstatEntry> qstatEntries = taskNameQstatEntryMap.get(taskName);
        return qstatEntries.stream().map(QstatEntry::getId).collect(Collectors.toSet());
    }

    // getters
    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public String getServerName() {
        return serverName;
    }

    @Override
    public QueueCommandManager getQstatCommandManager() {
        return cmdManager;
    }

    @Override
    public Set<String> getJobsInMonitor() {
        return taskNameQstatEntryMap.keySet();
    }

    // =======================================================================================

    /**
     * Utility class that tracks useful information for a single job out of a qstat output.
     *
     * @author PT
     */
    private static class QstatEntry {

        private final long id;
        private final String name;
        private String status;

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
            status = qstatLineParts[QueueCommandManager.STATUS_INDEX];
            name = qstatLineParts[QueueCommandManager.NAME_INDEX];
        }

        /**
         * Updates the status and the elapsed time from the output of a qstat command.
         *
         * @param qstatString Output of the qstat command.
         */
        public void updateFromQstatString(String qstatString) {
            String[] qstatLineParts = qstatString.split("\\s+");
            status = qstatLineParts[QueueCommandManager.STATUS_INDEX];
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getStatus() {
            return status;
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
