package gov.nasa.ziggy.module;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.Iso8601Formatter;
import gov.nasa.ziggy.util.io.LockManager;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * This class models a file whose name contains the state of a pipeline task executing on a remote
 * cluster.
 * <p>
 * The file also contains additional properties of the remote job.
 * <p>
 * Each state file represents a single unit of work from the perspective of the pipeline module.
 *
 * <pre>
 * Filename Format:
 *
 * ziggy-PIID.PTID.EXENAME.STATE_TOTAL-COMPLETE-FAILED
 *
 * PIID: Pipeline Instance ID
 * PTID: Pipeline Task ID
 * EXENAME: Name of the MATLAB executable
 * STATE: enum(SUBMITTED,PROCESSING,ERRORS_RUNNING,FAILED,COMPLETE)
 * TOTAL-COMPLETE-FAILED): Number of jobs in each category
 * </pre>
 *
 * The file contains the properties that reflect the properties in this class, including:
 * <dl>
 * <dt>timeoutSecs</dt>
 * <dd>Timeout for the MATLAB process.</dd>
 * <dt>gigsPerCore</dt>
 * <dd>Required memory per core used. Used to calculate coresPerNode based on architecture.</dd>
 * <dt>tasksPerCore</dt>
 * <dd>Number of tasks to allocate to each available core.</dd>
 * <dt>remoteNodeArchitecture</dt>
 * <dd>Architecture to use.</dd>
 * <dt>remoteGroup</dt>
 * <dd>Group name used for the qsub command on the remote node.</dd>
 * <dt>queueName</dt>
 * <dd>Queue name used for the qsub command on the remote node.</dd>
 * <dt>reRunnable</dt>
 * <dd>Whether this task is re-runnable.</dd>
 * <dt>localBinToMatEnabled</dt>
 * <dd>If true, don't generate .mat files on the remote node.</dd>
 * <dt>requestedWallTime</dt>
 * <dd>Requested wall time for the PBS qsub command.</dd>
 * <dt>symlinksEnabled</dt>
 * <dd>Determines whether symlinks are created between sub-task directories and files in the
 * top-level task directory. This should be enabled for modules that store files common to all
 * sub-tasks in the top-level task directory.</dd>
 * <dt>pbsSubmitTimeMillis</dt>
 * <dd>The time in milliseconds that the job was submitted to the Pleiades scheduler, the Portable
 * Batch System (PBS).</dd>
 * <dt>pfeArrivalTimeMillis</dt>
 * <dd>This seems to be the same as pbsSubmitTimeMillis in most cases.</dd>
 * </dl>
 *
 * @author Bill Wohler
 * @author Todd Klaus
 */
public class StateFile implements Comparable<StateFile>, Serializable {
    private static final long serialVersionUID = 20230511L;

    private static final Logger log = LoggerFactory.getLogger(StateFile.class);

    public static final Pattern TASK_DIR_PATTERN = Pattern.compile("([0-9]+)-([0-9]+)-(\\S+)");
    public static final int TASK_DIR_INSTANCE_ID_GROUP_NUMBER = 1;
    public static final int TASK_DIR_TASK_ID_GROUP_NUMBER = 2;
    public static final int TASK_DIR_MODULE_NAME_GROUP_NUMBER = 3;

    public static final String PREFIX_BARE = "ziggy";
    public static final String PREFIX = PREFIX_BARE + ".";
    public static final String PREFIX_WITH_BACKSLASHES = PREFIX_BARE + "\\.";

    public static final String DEFAULT_REMOTE_NODE_ARCHITECTURE = "none";
    public static final String DEFAULT_WALL_TIME = "24:00:00";
    public static final String INVALID_STRING = "none";
    public static final int INVALID_VALUE = -1;

    public static final String LOCK_FILE_NAME = ".state-file.lock";

    private static final String REMOTE_NODE_ARCHITECTURE_PROP_NAME = "remoteNodeArchitecture";
    private static final String MIN_CORES_PER_NODE_PROP_NAME = "minCoresPerNode";
    private static final String MIN_GIGS_PER_NODE_PROP_NAME = "minGigsPerNode";
    private static final String REMOTE_GROUP_PROP_NAME = "remoteGroup";
    private static final String QUEUE_NAME_PROP_NAME = "queueName";
    private static final String REQUESTED_WALLTIME_PROP_NAME = "requestedWallTime";
    private static final String REQUESTED_NODE_COUNT_PROP_NAME = "requestedNodeCount";
    private static final String ACTIVE_CORES_PER_NODE_PROP_NAME = "activeCoresPerNode";
    private static final String GIGS_PER_SUBTASK_PROP_NAME = "gigsPerSubtask";
    private static final String EXECUTABLE_NAME_PROP_NAME = "executableName";

    private static final String PBS_SUBMIT_PROP_NAME = "pbsSubmitTimeMillis";
    private static final String PFE_ARRIVAL_PROP_NAME = "pfeArrivalTimeMillis";

    public enum State {
        /** Task has been initialized. */
        INITIALIZED,

        /**
         * Task has been submitted by the worker, but not yet picked up by the remote cluster.
         */
        SUBMITTED,

        /** Task is waiting for compute nodes to become available. */
        QUEUED,

        /** Task is running on the compute nodes. */
        PROCESSING,

        /**
         * Task has finished running on the compute nodes, either with or without subtask errors.
         */
        COMPLETE,

        /** The final state for this task has been acknowledged by the worker. */
        CLOSED;

    }

    private static String statesPatternElement;

    // Concatenate all of the states into a single String for use in the state file
    // name pattern.
    static {
        StringBuilder sb = new StringBuilder();
        for (State state : State.values()) {
            sb.append(state.toString());
            sb.append("|");
        }
        sb.setLength(sb.length() - 1);
        statesPatternElement = sb.toString();
    }

    // Pattern and regex for a state file name
    private static final String STATE_FILE_NAME_REGEX = PREFIX_WITH_BACKSLASHES
        + "([0-9]+)\\.([0-9]+)\\.(\\S+)\\." + "(" + statesPatternElement + ")"
        + "_([0-9]+)-([0-9]+)-([0-9]+)";
    public static final Pattern STATE_FILE_NAME_PATTERN = Pattern.compile(STATE_FILE_NAME_REGEX);
    private static final int STATE_FILE_NAME_INSTANCE_ID_GROUP_NUMBER = 1;
    private static final int STATE_FILE_NAME_TASK_ID_GROUP_NUMBER = 2;
    private static final int STATE_FILE_NAME_MODULE_GROUP_NUMBER = 3;
    private static final int STATE_FILE_NAME_STATE_GROUP_NUMBER = 4;
    private static final int STATE_FILE_NAME_TOTAL_SUBTASKS_GROUP_NUMBER = 5;
    private static final int STATE_FILE_NAME_COMPLETE_SUBTASKS_GROUP_NUMBER = 6;
    private static final int STATE_FILE_NAME_FAILED_SUBTASKS_GROUP_NUMBER = 7;

    // This is a slightly more informative explanation of the file name format,
    // used in log files so the user knows exactly what was expected.
    private static final String FORMAT = PREFIX + "PIID.PTID.MODNAME.STATE_TOTAL-COMPLETE-FAILED";

    // Fields in the file name.
    private long pipelineInstanceId = 0;
    private long pipelineTaskId = 0;
    private String moduleName;
    private State state = State.INITIALIZED;
    private int numTotal = 0;
    private int numComplete = 0;
    private int numFailed = 0;

    /** Contains all properties from the file. */
    private transient PropertiesConfiguration props = new PropertiesConfiguration();

    public StateFile() {
    }

    /**
     * Constructs a StateFile containing only the invariant part.
     *
     * @param moduleName the name of the pipeline module for the task
     * @param pipelineInstanceId the pipeline instance ID
     * @param pipelineTaskId the pipeline task ID
     */
    public StateFile(String moduleName, long pipelineInstanceId, long pipelineTaskId) {
        this.moduleName = moduleName;
        this.pipelineInstanceId = pipelineInstanceId;
        this.pipelineTaskId = pipelineTaskId;
    }

    /**
     * Constructs a {@link StateFile} instance from a task directory. The task directory name is
     * parsed to obtain the module name, instance ID, and task ID components.
     */
    public static StateFile of(Path taskDir) {
        String taskDirName = taskDir.getFileName().toString();
        Matcher matcher = TASK_DIR_PATTERN.matcher(taskDirName);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Task dir name " + taskDirName + " does not match convention for task dir names");
        }
        return new StateFile(matcher.group(TASK_DIR_MODULE_NAME_GROUP_NUMBER),
            Long.parseLong(matcher.group(TASK_DIR_INSTANCE_ID_GROUP_NUMBER)),
            Long.parseLong(matcher.group(TASK_DIR_TASK_ID_GROUP_NUMBER)));
    }

    /**
     * Constructs a StateFile from an existing name.
     */
    public StateFile(String name) {
        parse(name);
    }

    /**
     * Parses a string of the form: PREFIX + MODNAME.PIID.PTID.STATE_TOTAL-COMPLETE-FAILED)
     */
    private void parse(String name) {
        Matcher matcher = STATE_FILE_NAME_PATTERN.matcher(name);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(name + " does not match expected format: " + FORMAT);
        }

        pipelineInstanceId = Long
            .parseLong(matcher.group(STATE_FILE_NAME_INSTANCE_ID_GROUP_NUMBER));
        pipelineTaskId = Long.parseLong(matcher.group(STATE_FILE_NAME_TASK_ID_GROUP_NUMBER));
        moduleName = matcher.group(STATE_FILE_NAME_MODULE_GROUP_NUMBER);
        state = State.valueOf(matcher.group(STATE_FILE_NAME_STATE_GROUP_NUMBER));
        numTotal = Integer.parseInt(matcher.group(STATE_FILE_NAME_TOTAL_SUBTASKS_GROUP_NUMBER));
        numComplete = Integer
            .parseInt(matcher.group(STATE_FILE_NAME_COMPLETE_SUBTASKS_GROUP_NUMBER));
        numFailed = Integer.parseInt(matcher.group(STATE_FILE_NAME_FAILED_SUBTASKS_GROUP_NUMBER));
    }

    /**
     * Creates a shallow copy of the given object.
     */
    public StateFile(StateFile other) {

        // members

        moduleName = other.getModuleName();
        pipelineInstanceId = other.getPipelineInstanceId();
        pipelineTaskId = other.getPipelineTaskId();
        state = other.getState();
        numTotal = other.getNumTotal();
        numComplete = other.getNumComplete();
        numFailed = other.getNumFailed();

        // properties of the PropertiesConfiguration

        List<String> propertyNames = other.getSortedPropertyNames();
        for (String propertyName : propertyNames) {
            props.setProperty(propertyName, other.props.getProperty(propertyName));
        }
    }

    /**
     * Gets the property names of the StateFile's PropertyConfigurations object, sorted
     * alphabetically
     *
     * @return a List<String> of property names defined for this object.
     */
    private List<String> getSortedPropertyNames() {
        Iterator<String> propertyNamesIterator = props.getKeys();
        List<String> propertyNames = new ArrayList<>();
        while (propertyNamesIterator.hasNext()) {
            propertyNames.add(propertyNamesIterator.next());
        }
        Collections.sort(propertyNames);
        return propertyNames;
    }

    /**
     * Creates a StateFile from the given parameters.
     */
    public static StateFile generateStateFile(PipelineTask pipelineTask,
        PbsParameters pbsParameters, int numSubtasks) {

        StateFile state = new StateFile.Builder().moduleName(pipelineTask.getModuleName())
            .executableName(pipelineTask.getExecutableName())
            .pipelineInstanceId(pipelineTask.getPipelineInstanceId())
            .pipelineTaskId(pipelineTask.getId())
            .numTotal(numSubtasks)
            .numComplete(0)
            .numFailed(0)
            .state(StateFile.State.QUEUED)
            .build();

        if (pbsParameters != null) {
            state.setActiveCoresPerNode(pbsParameters.getActiveCoresPerNode());
            state.setRemoteNodeArchitecture(pbsParameters.getArchitecture().getNodeName());
            state.setMinCoresPerNode(pbsParameters.getMinCoresPerNode());
            state.setMinGigsPerNode(pbsParameters.getMinGigsPerNode());
            state.setRemoteGroup(pbsParameters.getRemoteGroup());
            state.setQueueName(pbsParameters.getQueueName());
            state.setRequestedWallTime(pbsParameters.getRequestedWallTime());
            state.setRequestedNodeCount(pbsParameters.getRequestedNodeCount());
            state.setGigsPerSubtask(pbsParameters.getGigsPerSubtask());
        } else {
            state.setActiveCoresPerNode(1);
            state.setRemoteNodeArchitecture("");
            state.setRemoteGroup("");
            state.setQueueName("");
        }

        return state;
    }

    /**
     * Persists this {@link StateFile} to the state file directory.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public File persist() {
        File directory = DirectoryProperties.stateFilesDir().toFile();
        File file = new File(directory, name());
        try (Writer fw = new OutputStreamWriter(new FileOutputStream(file),
            ZiggyFileUtils.ZIGGY_CHARSET)) {
            props.write(fw);

            // Also, move any old state files that are for the same instance and
            // task as this one.
            moveOldStateFiles(directory);
        } catch (ConfigurationException e) {
            // This can never occur. The construction of the props field is guaranteed
            // to be correct.
            throw new AssertionError(e);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write to file " + file.toString(), e);
        }
        return file;
    }

    private void moveOldStateFiles(File stateFileDir) {

        String stateFileName = name();
        FileFilter fileFilter = new WildcardFileFilter(invariantPart() + "*");
        File[] matches = stateFileDir.listFiles(fileFilter);

        if (matches == null || matches.length == 0) {
            throw new PipelineException(
                "State file \"" + stateFileName + "\" does not exist or there was an I/O error.");
        }

        String iso8601Date = Iso8601Formatter.dateTimeLocalFormatter().format(new Date());
        int fileCounter = 0;

        // For all the matched files that are NOT the current state file, rename
        // the old ones to a new name that removes the "ziggy" at the beginning
        // (replacing it with "old"), and appends a datestamp and index #.

        for (File match : matches) {
            if (!match.getName().equals(stateFileName)) {
                String nameSansPrefix = match.getName().substring(PREFIX.length());
                String newName = "old." + nameSansPrefix + "." + iso8601Date + "."
                    + Integer.toString(fileCounter);
                File newFile = new File(stateFileDir, newName);
                match.renameTo(newFile);
                log.warn("File " + match.getName() + " in directory " + stateFileDir
                    + " renamed to " + newFile.getName());
            }
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static boolean updateStateFile(StateFile oldStateFile, StateFile newStateFile) {

        File stateFileDir = DirectoryProperties.stateFilesDir().toFile();
        if (oldStateFile.equals(newStateFile)) {
            log.debug("Old state file " + oldStateFile.name() + " is the same as new state file "
                + newStateFile.name() + ", not changing");
        }
        // Update the state file.
        log.info("Updating state: " + oldStateFile + " -> " + newStateFile);
        File oldFile = new File(stateFileDir, oldStateFile.name());
        File newFile = new File(stateFileDir, newStateFile.name());

        log.debug("  renaming file: " + oldFile + " -> " + newFile);

        try {
            FileUtils.moveFile(oldFile, newFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to move file " + oldFile.toString(), e);
        }

        return true;
    }

    /**
     * Returns a {@link List} of {@link StateFile} instances from the state files directory that are
     * in the {@link State#PROCESSING} state.
     */
    public static List<StateFile> processingStateFilesFromDisk() {
        File directory = DirectoryProperties.stateFilesDir().toFile();

        String[] filenames = directory.list((dir, name) -> {
            Matcher matcher = STATE_FILE_NAME_PATTERN.matcher(name);
            return matcher.matches() && matcher.group(STATE_FILE_NAME_STATE_GROUP_NUMBER)
                .equals(State.PROCESSING.toString());
        });
        List<StateFile> stateFiles = new ArrayList<>();
        for (String filename : filenames) {
            stateFiles.add(new StateFile(filename));
        }
        return stateFiles;
    }

    /**
     * Constructs a new {@link StateFile} from an existing file.
     *
     * @return a {@link StateFile} object derived from the specified file on disk
     */
    public StateFile newStateFileFromDiskFile() {
        return newStateFileFromDiskFile(false);
    }

    /**
     * Constructs a new {@link StateFile} from an existing file.
     *
     * @param silent when true suppresses the logging message from matching the disk file.
     * @return a StateFile object derived from the specified file on disk
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public StateFile newStateFileFromDiskFile(boolean silent) {

        File stateFilePathToUse = StateFile.getDiskFileFromInvariantNamePart(this);

        if (!silent) {
            log.info("Matched statefile: " + stateFilePathToUse);
        }
        StateFile stateFile = new StateFile(stateFilePathToUse.getName());
        try {
            PropertiesConfiguration props = new Configurations().properties(stateFilePathToUse);
            if (props.isEmpty()) {
                throw new PipelineException("State file contains no properties!");
            }
            stateFile.props = props;
        } catch (ConfigurationException e) {
            // This can never occur. By construction, the props field is guaranteee
            // to be constructed correctly.
            throw new AssertionError(e);
        }

        return stateFile;
    }

    /**
     * Searches a directory for a state file where the name's invariant part matches that provided
     * by the caller.
     *
     * @param oldStateFile existing {@link StateFile} instance
     * @return the file in the directory of the stateFilePath that has a name for which the
     * invariant part matches the invariant part of the stateFilePath name (i.e.,
     * ziggy.pa.6.23.QUEUED.10.0.0 will match ziggy.pa.6.23.* on disk)
     * @exception IllegalArgumentException if the stateFilePath's parent is not a directory
     * @exception IllegalStateException if there isn't only one StateFile that matches the invariant
     * part of stateFilePath
     */
    private static File getDiskFileFromInvariantNamePart(StateFile oldStateFile) {

        // Sadly, this is probably the easiest way to do this.
        String invariantPart = oldStateFile.invariantPart();
        File directory = DirectoryProperties.stateFilesDir().toFile();
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalArgumentException(
                "Specified directory does not exist or is not a directory: " + directory);
        }

        FileFilter fileFilter = new WildcardFileFilter(invariantPart + "*");
        File[] matches = directory.listFiles(fileFilter);
        if (matches == null) {
            throw new IllegalStateException(
                "No state file matching " + invariantPart + "* in directory " + directory);
        }
        if (matches.length > 1) {
            throw new IllegalStateException("More than one state file matches " + invariantPart
                + "* in directory " + directory);
        }

        return matches[0];
    }

    /**
     * Builds the name of the state file based on the elements.
     */
    public String name() {
        return invariantPart() + "." + state + "_" + numTotal + "-" + numComplete + "-" + numFailed;
    }

    /**
     * Returns the invariant part of the state file name. This includes the static PREFIX and the
     * pipeline instance and task ids, plus the module name.
     */
    public String invariantPart() {
        return PREFIX + pipelineInstanceId + "." + pipelineTaskId + "." + moduleName;
    }

    public static String invariantPart(PipelineTask task) {
        return PREFIX + task.getPipelineInstanceId() + "." + task.getId() + "."
            + task.getModuleName();
    }

    public String taskBaseName() {
        return PipelineTask.taskBaseName(pipelineInstanceId, pipelineTaskId, moduleName);
    }

    /**
     * Returns the name of the task dir represented by this StateFile.
     */
    public String taskDirName() {
        return PipelineTask.taskBaseName(getPipelineInstanceId(), getPipelineTaskId(),
            getModuleName());
    }

    public void setStateAndPersist(State state) {
        LockManager.getWriteLockOrBlock(lockFile());
        try {
            StateFile oldState = newStateFileFromDiskFile(true);
            StateFile newState = new StateFile(oldState);
            newState.setState(state);
            StateFile.updateStateFile(oldState, newState);
        } finally {
            LockManager.releaseWriteLock(lockFile());
        }
    }

    public File lockFile() {
        return DirectoryProperties.taskDataDir()
            .resolve(taskDirName())
            .resolve(LOCK_FILE_NAME)
            .toFile();
    }

    public boolean isDone() {
        return state == State.COMPLETE || state == State.CLOSED;
    }

    public boolean isRunning() {
        return state == State.PROCESSING;
    }

    public boolean isQueued() {
        return state == State.QUEUED;
    }

    public boolean isStarted() {
        return isRunning() || isDone();
    }

    @Override
    public int compareTo(StateFile o) {
        return name().compareTo(o.name());
    }

    @Override
    public String toString() {
        return name();
    }

    public long getPipelineInstanceId() {
        return pipelineInstanceId;
    }

    public void setPipelineInstanceId(long pipelineInstanceId) {
        this.pipelineInstanceId = pipelineInstanceId;
    }

    public long getPipelineTaskId() {
        return pipelineTaskId;
    }

    public void setPipelineTaskId(long pipelineTaskId) {
        this.pipelineTaskId = pipelineTaskId;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public int getNumTotal() {
        return numTotal;
    }

    public void setNumTotal(int numTotal) {
        this.numTotal = numTotal;
    }

    public int getNumComplete() {
        return numComplete;
    }

    public void setNumComplete(int numComplete) {
        this.numComplete = numComplete;
    }

    public int getNumFailed() {
        return numFailed;
    }

    public void setNumFailed(int numFailed) {
        this.numFailed = numFailed;
    }

    public String getExecutableName() {
        return props.getProperty(EXECUTABLE_NAME_PROP_NAME) != null
            && !StringUtils.isBlank(props.getString(EXECUTABLE_NAME_PROP_NAME))
                ? props.getString(EXECUTABLE_NAME_PROP_NAME)
                : INVALID_STRING;
    }

    public void setExecutableName(String executableName) {
        props.setProperty(EXECUTABLE_NAME_PROP_NAME, executableName);
    }

    /**
     * Returns the value of the {@value #REMOTE_NODE_ARCHITECTURE_PROP_NAME} property, or
     * {@link #DEFAULT_REMOTE_NODE_ARCHITECTURE} if not present or set.
     */
    public String getRemoteNodeArchitecture() {
        return props.getProperty(REMOTE_NODE_ARCHITECTURE_PROP_NAME) != null
            && !props.getString(REMOTE_NODE_ARCHITECTURE_PROP_NAME).isBlank()
                ? props.getString(REMOTE_NODE_ARCHITECTURE_PROP_NAME)
                : DEFAULT_REMOTE_NODE_ARCHITECTURE;
    }

    public void setRemoteNodeArchitecture(String remoteNodeArchitecture) {
        props.setProperty(REMOTE_NODE_ARCHITECTURE_PROP_NAME, remoteNodeArchitecture);
    }

    /**
     * Returns the value of the {@value #MIN_CORES_PER_NODE_PROP_NAME} property, or 0 if not present
     * or set.
     */
    public int getMinCoresPerNode() {
        return props.getProperty(MIN_CORES_PER_NODE_PROP_NAME) != null
            ? props.getInt(MIN_CORES_PER_NODE_PROP_NAME)
            : INVALID_VALUE;
    }

    public void setMinCoresPerNode(int minCoresPerNode) {
        props.setProperty(MIN_CORES_PER_NODE_PROP_NAME, minCoresPerNode);
    }

    /**
     * Returns the value of the {@value #MIN_GIGS_PER_NODE_PROP_NAME} property, or 0 if not present
     * or set.
     */
    public double getMinGigsPerNode() {
        return props.getProperty(MIN_GIGS_PER_NODE_PROP_NAME) != null
            ? props.getDouble(MIN_GIGS_PER_NODE_PROP_NAME)
            : INVALID_VALUE;
    }

    public void setMinGigsPerNode(double minGigsPerNode) {
        props.setProperty(MIN_GIGS_PER_NODE_PROP_NAME, minGigsPerNode);
    }

    /**
     * Returns the value of the {@value #REQUESTED_WALLTIME_PROP_NAME} property, or
     * {@link #DEFAULT_WALL_TIME} if not present or set.
     */
    public String getRequestedWallTime() {
        return props.getProperty(REQUESTED_WALLTIME_PROP_NAME) != null
            ? props.getString(REQUESTED_WALLTIME_PROP_NAME)
            : DEFAULT_WALL_TIME;
    }

    public void setRequestedWallTime(String requestedWallTime) {
        props.setProperty(REQUESTED_WALLTIME_PROP_NAME, requestedWallTime);
    }

    /**
     * Returns the value of the {@value #REMOTE_GROUP_PROP_NAME} property, or
     * {@link #INVALID_STRING} if not present or set.
     */
    public String getRemoteGroup() {
        return props.getProperty(REMOTE_GROUP_PROP_NAME) != null
            ? props.getString(REMOTE_GROUP_PROP_NAME)
            : INVALID_STRING;
    }

    public void setRemoteGroup(String remoteGroup) {
        props.setProperty(REMOTE_GROUP_PROP_NAME, remoteGroup);
    }

    /**
     * Returns the value of the {@value #QUEUE_NAME_PROP_NAME} property, or {@link #INVALID_STRING}
     * if not present or set.
     */
    public String getQueueName() {
        return props.getProperty(QUEUE_NAME_PROP_NAME) != null
            ? props.getString(QUEUE_NAME_PROP_NAME)
            : INVALID_STRING;
    }

    public void setQueueName(String queueName) {
        props.setProperty(QUEUE_NAME_PROP_NAME, queueName);
    }

    public int getActiveCoresPerNode() {
        return props.getProperty(ACTIVE_CORES_PER_NODE_PROP_NAME) != null
            ? props.getInt(ACTIVE_CORES_PER_NODE_PROP_NAME)
            : INVALID_VALUE;
    }

    public void setActiveCoresPerNode(int activeCoresPerNode) {
        props.setProperty(ACTIVE_CORES_PER_NODE_PROP_NAME, activeCoresPerNode);
    }

    public int getRequestedNodeCount() {
        return props.getProperty(REQUESTED_NODE_COUNT_PROP_NAME) != null
            ? props.getInt(REQUESTED_NODE_COUNT_PROP_NAME)
            : INVALID_VALUE;
    }

    public void setRequestedNodeCount(int requestedNodeCount) {
        props.setProperty(REQUESTED_NODE_COUNT_PROP_NAME, requestedNodeCount);
    }

    public double getGigsPerSubtask() {
        return props.getProperty(GIGS_PER_SUBTASK_PROP_NAME) != null
            ? props.getDouble(GIGS_PER_SUBTASK_PROP_NAME)
            : INVALID_VALUE;
    }

    public void setGigsPerSubtask(double gigsPerSubtask) {
        props.setProperty(GIGS_PER_SUBTASK_PROP_NAME, gigsPerSubtask);
    }

    /**
     * Returns the value of the {@value #PBS_SUBMIT_PROP_NAME} property, or {@link #INVALID_VALUE}
     * if not present or set.
     */
    public long getPbsSubmitTimeMillis() {
        return props.getProperty(PBS_SUBMIT_PROP_NAME) != null ? props.getLong(PBS_SUBMIT_PROP_NAME)
            : INVALID_VALUE;
    }

    public void setPbsSubmitTimeMillis(long pbsSubmitTimeMillis) {
        props.setProperty(PBS_SUBMIT_PROP_NAME, pbsSubmitTimeMillis);
    }

    /**
     * Returns the value of the {@value #PFE_ARRIVAL_PROP_NAME} property, or {@link #INVALID_VALUE}
     * if not present or set.
     */
    public long getPfeArrivalTimeMillis() {
        return props.getProperty(PFE_ARRIVAL_PROP_NAME) != null
            ? props.getLong(PFE_ARRIVAL_PROP_NAME)
            : INVALID_VALUE;
    }

    public void setPfeArrivalTimeMillis(long pfeArrivalTimeMillis) {
        props.setProperty(PFE_ARRIVAL_PROP_NAME, pfeArrivalTimeMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(moduleName, numComplete, numFailed, numTotal, pipelineInstanceId,
            pipelineTaskId, state);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StateFile other = (StateFile) obj;
        if (moduleName == null) {
            if (other.moduleName == null) {
                return false;
            }
        } else if (!moduleName.equals(other.moduleName)) {
            return false;
        }
        if (numComplete != other.numComplete || numFailed != other.numFailed
            || numTotal != other.numTotal || pipelineInstanceId != other.pipelineInstanceId) {
            return false;
        }
        if (pipelineTaskId != other.pipelineTaskId || state != other.state) {
            return false;
        }
        return true;
    }

    /**
     * Used to construct a {@link StateFile} object. To use this class, a {@link StateFile} object
     * is created and then non-null fields are set using the available builder methods. Finally, a
     * {@link StateFile} object is created using the {@code build} method. For example:
     *
     * <pre>
     * StateFile stateFile = new StateFile.Builder().foo(fozar).bar(bazar).build();
     * </pre>
     *
     * This pattern is based upon
     * <a href= "http://developers.sun.com/learning/javaoneonline/2006/coreplatform/TS-1512.pdf" >
     * Josh Bloch's JavaOne 2006 talk, Effective Java Reloaded, TS-1512</a>.
     *
     * @author PT
     */
    public static class Builder {

        private StateFile stateFile = new StateFile();

        public Builder() {
        }

        public Builder moduleName(String moduleName) {
            stateFile.setModuleName(moduleName);
            return this;
        }

        public Builder executableName(String executableName) {
            stateFile.setExecutableName(executableName);
            return this;
        }

        public Builder pipelineInstanceId(long pipelineInstanceId) {
            stateFile.setPipelineInstanceId(pipelineInstanceId);
            return this;
        }

        public Builder pipelineTaskId(long pipelineTaskId) {
            stateFile.setPipelineTaskId(pipelineTaskId);
            return this;
        }

        public Builder state(State state) {
            stateFile.setState(state);
            return this;
        }

        public Builder numTotal(int numTotal) {
            stateFile.setNumTotal(numTotal);
            return this;
        }

        public Builder numComplete(int numComplete) {
            stateFile.setNumComplete(numComplete);
            return this;
        }

        public Builder numFailed(int numFailed) {
            stateFile.setNumFailed(numFailed);
            return this;
        }

        public StateFile build() {
            return new StateFile(stateFile);
        }
    }
}
