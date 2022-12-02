package gov.nasa.ziggy.module.remote;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.RemoteJob.RemoteJobQstatInfo;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Executes batch commands and parses the text output from those commands.
 * <p>
 * Subclasses of the {@link QueueCommandManager} class support the {@link QstatMonitor} class by
 * obtaining job information that is only available in text output from the qstat shell command. The
 * command is executed, its text results captured, and the text is parsed to obtain the desired
 * information for {@link QstatMonitor}. Information such as job IDs, job exit status, etc., can
 * thus be obtained.
 * <p>
 * Additionally, class instances can execute the qdel shell command to delete queued or running jobs
 * from the batch system. This allows the operator to terminate jobs via the pipeline user interface
 * rather than via the cumbersome qdel command itself.
 * <p>
 * Concrete implementations of {@link QueueCommandManager} must provide implementation of the
 * abstract qstat and qdel methods. The correct subclass of {@link QueueCommandManager} is provided
 * at runtime by {@link newInstance()}.
 *
 * @author PT
 */
public abstract class QueueCommandManager {

    private static final Logger log = LoggerFactory.getLogger(QueueCommandManager.class);

    // Indices for the various components of a qstat output line
    public static final int ID_INDEX = 0;
    public static final int OWNER_INDEX = 1;
    public static final int QUEUE_INDEX = 2;
    public static final int NAME_INDEX = 3;
    public static final int TSK_INDEX = 4;
    public static final int NODES_INDEX = 5;
    public static final int REQUESTED_TIME_INDEX = 6;
    public static final int STATUS_INDEX = 7;
    public static final int ELAPSED_TIME_INDEX = 8;
    public static final int EFF_INDEX = 9;

    private static final Pattern EXIT_STATUS_PATTERN = Pattern
        .compile("\\s*Exit_status = (-?[0-9]+)");
    private static final Pattern EXIT_COMMENT_PATTERN = Pattern.compile("\\s*comment = (.+)");
    public static final String DEFAULT_LOCAL_CLASS = "gov.nasa.ziggy.module.remote.QueueLocalCommandManager";
    public static final String SELECT = "Resource_List.select";
    public static final String WALLTIME = "resources_used.walltime";
    public static final Pattern SELECT_PATTERN = Pattern
        .compile("\\s*" + SELECT + " = ([0-9]+):model=(\\S+)");
    public static final Pattern WALLTIME_PATTERN = Pattern.compile("\\s*" + WALLTIME + " = (\\S+)");

    public static QueueCommandManager newInstance() {
        Configuration config = ZiggyConfiguration.getInstance();
        String queueCommandClassName;
        queueCommandClassName = config.getString(PropertyNames.QUEUE_COMMAND_CLASS_PROP_NAME,
            DEFAULT_LOCAL_CLASS);

        QueueCommandManager cmdManager;
        try {
            cmdManager = (QueueCommandManager) Class.forName(queueCommandClassName)
                .getConstructor()
                .newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException
            | ClassNotFoundException e) {
            throw new PipelineException("Unable to instantiate class " + queueCommandClassName, e);
        }
        return cmdManager;
    }

    /**
     * Gets the qstat information for all jobs with a given name.
     *
     * @param taskName Name of tasks.
     * @param owner Owner of jobs.
     * @return Qstat information for all jobs with the given name.
     */
    public List<String> getQstatInfoByTaskName(String owner, String taskName) {

        String commandString = "-u " + owner;
        return qstat(commandString, taskName);
    }

    private String qstatArgsWithJobIds(String firstArg, Collection<Long> jobIds) {
        StringBuilder qstatArgs = new StringBuilder(firstArg);
        qstatArgs.append(" ");
        for (long jobId : jobIds) {
            qstatArgs.append(jobId).append(" ");
        }
        qstatArgs.setLength(qstatArgs.length() - 1);
        return qstatArgs.toString();
    }

    /**
     * Gets the server names for a collection of jobs using their job IDs.
     *
     * @param jobIds IDs of jobs.
     * @return Map from the job IDs to the server used to submit each job.
     */
    public Map<Long, String> serverNames(Collection<Long> jobIds) {

        Map<Long, String> jobIdServerNameMap = new HashMap<>();

        // construct the qstat command line arguments
        String qstatArgs = qstatArgsWithJobIds("-xf", jobIds);

        // run the command and grep for the job and owner
        List<String> qstatOutput = qstat(qstatArgs, "Job:", "Job_Owner");

        // parse the output: there are 2 lines per job, the first has the ID and the
        // second has the server name, though both need to be chopped up
        String line = null;
        for (int qstatOutputIndex = 0; qstatOutputIndex < qstatOutput
            .size(); qstatOutputIndex += 2) {
            line = qstatOutput.get(qstatOutputIndex);
            Long jobIdString = Long.valueOf(StringUtils.stripStart(line.split("\\.")[0], "Job: "));
            line = qstatOutput.get(qstatOutputIndex + 1);
            String serverName = line.split("\\@")[1].split("\\.")[0];
            jobIdServerNameMap.put(jobIdString, serverName);
        }
        return jobIdServerNameMap;
    }

    /**
     * Gets the qstat output line for a collection of jobs using their job IDs.
     *
     * @param jobIds IDs of jobs.
     * @return Map from the job names (not IDs) to the qstat line for each job.
     */
    public Map<String, String> getQstatInfoByJobNameMap(Collection<Long> jobIds) {

        Map<String, String> qstatInfoByJobName = new HashMap<>();

        // construct the qstat arguments
        String qstatArgs = qstatArgsWithJobIds("-x", jobIds);

        // run the command
        List<String> qstatOutput = qstat(qstatArgs);

        // skip the first 3 lines of the output and then parse 1 line per job
        for (int i = 3; i < qstatOutput.size(); i++) {
            String line = qstatOutput.get(i);
            String jobName = line.split("\\s+")[NAME_INDEX];
            qstatInfoByJobName.put(jobName, line);
        }

        return qstatInfoByJobName;
    }

    /**
     * Gets the exit status for a job.
     *
     * @param jobId ID number of the job
     * @return exit status integer, can be null if no job with the specified ID can be found or the
     * job has not yet completed.
     */
    public Integer exitStatus(long jobId) {
        String qstatArgs = "-xf " + jobId;
        List<String> qstatOutput = qstat(qstatArgs, "Exit_status");
        if (qstatOutput != null && !qstatOutput.isEmpty()) {
            String exitStatusString = qstatOutput.get(0);
            if (exitStatusString != null && !exitStatusString.isEmpty()) {
                return Integer.valueOf(matchAndReturn(EXIT_STATUS_PATTERN, exitStatusString, 1));
            }
        }
        return null;
    }

    /**
     * Gets remote job information (node count, model, and wall time) for a job.
     *
     * @return a {@link RemoteJobQstatInfo} instance, never null.
     */
    public RemoteJob.RemoteJobQstatInfo remoteJobQstatInfo(long jobId) {
        RemoteJobQstatInfo remoteJobQstatInfo = new RemoteJobQstatInfo();
        String qstatArgs = "-xf " + jobId;
        List<String> qstatOutput = qstat(qstatArgs, SELECT, WALLTIME);
        log.debug("job " + jobId + "qstat lines: " + qstatOutput.toString());
        String selectLine = null;
        String walltimeLine = null;

        // Note that the select is the only line when the job isn't running,
        // but it's the 2nd line when the job is running
        if (qstatOutput.size() == 1) {
            selectLine = qstatOutput.get(0);
        }
        if (qstatOutput.size() == 2) {
            walltimeLine = qstatOutput.get(0);
            selectLine = qstatOutput.get(1);
        }
        if (selectLine != null) {
            Matcher m = SELECT_PATTERN.matcher(selectLine);
            if (m.matches()) {
                remoteJobQstatInfo.setNodes(Integer.parseInt(m.group(1)));
                remoteJobQstatInfo.setModel(m.group(2));
            }
        }
        if (walltimeLine != null) {
            Matcher m = WALLTIME_PATTERN.matcher(walltimeLine);
            if (m.matches()) {
                remoteJobQstatInfo.setWallTime(m.group(1));
            }
        }
        return remoteJobQstatInfo;
    }

    /**
     * Matches a string to a pattern and returns a single specified group.
     *
     * @return specified group from a match, assuming successful match and sufficient groups were
     * found, null otherwise.
     */
    private String matchAndReturn(Pattern pattern, String qstatOutputString, int groupNumber) {
        Matcher matcher = pattern.matcher(qstatOutputString);
        boolean matches = matcher.matches();
        if (matches && matcher.groupCount() >= groupNumber) {
            return matcher.group(groupNumber);
        } else {
            return null;
        }
    }

    /**
     * Gets the exit comment for a job.
     *
     * @param jobId ID number for the job
     * @return exit comment, can be null if no job with the specified ID can be found
     */
    public String exitComment(long jobId) {
        String qstatArgs = "-xf " + jobId;
        List<String> qstatOutput = qstat(qstatArgs, "comment");
        if (qstatOutput != null && !qstatOutput.isEmpty()) {
            String exitCommentString = qstatOutput.get(0);
            if (exitCommentString != null && !exitCommentString.isEmpty()) {
                return matchAndReturn(EXIT_COMMENT_PATTERN, exitCommentString, 1);
            }
        }
        return null;
    }

    /**
     * Issues the delete command for all selected pipeline tasks.
     *
     * @param pipelineTasks selected pipeline tasks.
     */
    public void deleteJobsForPipelineTasks(List<PipelineTask> pipelineTasks) {
        String qdelArgs = qdelArgsForPipelineTasks(pipelineTasks);
        qdel(qdelArgs);
    }

    public void deleteJobsByJobId(Collection<Long> jobIds) {
        String qdelArgs = qdelArgsForJobIds(jobIds);
        qdel(qdelArgs);
    }

    /**
     * Generates the qdel command string.
     *
     * @param pipelineTasks pipeline tasks selected for deletion.
     * @return Command string for deletion using qdel.
     */
    private String qdelArgsForPipelineTasks(List<PipelineTask> pipelineTasks) {

        // obtain the job IDs for each task. This is accomplished via the
        // QstatMonitor
        QstatMonitor monitor = new QstatMonitor(this);
        for (PipelineTask task : pipelineTasks) {
            monitor.addToMonitoring(task);
        }
        monitor.update();

        // Get the job IDs.
        Set<Long> allJobIds = new HashSet<>();
        for (PipelineTask task : pipelineTasks) {
            Set<Long> jobIds = monitor.allIncompleteJobIds(task);
            if (jobIds != null) {
                allJobIds.addAll(jobIds);
            }
        }
        return qdelArgsForJobIds(allJobIds);
    }

    private String qdelArgsForJobIds(Collection<Long> jobIds) {
        if (jobIds == null || jobIds.isEmpty()) {
            return null;
        }
        StringBuilder qdelCommand = new StringBuilder();
        for (Long jobId : jobIds) {
            qdelCommand.append(jobId).append(" ");
        }
        return qdelCommand.toString();
    }

    private List<String> qstat(String commandOptions) {
        return qstat(commandOptions, (String[]) null);
    }

    /**
     * Executes the qstat command string and returns the result as a List&lt;String&gt;, one list
     * entry per line, filtering to return only those lines that contain one or more target strings.
     */
    protected abstract List<String> qstat(String commandString, String... targets);

    /**
     * Executes the qdel command string.
     *
     * @param commandString
     */
    protected abstract void qdel(String commandString);

    // In normal operations these return the hostname and username, which are used
    // to eliminate duplicates from the queue listings. For test purposes they can
    // be mocked to produce known values.
    protected String hostname() {
        return System.getenv("HOST");
    }

    protected String user() {
        return System.getenv("USER");
    }

}
