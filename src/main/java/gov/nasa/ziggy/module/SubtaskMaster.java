package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Semaphore;

import org.apache.commons.exec.DefaultExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.remote.TimestampFile;
import gov.nasa.ziggy.util.io.LockManager;

/**
 * Manages one algorithm thread for {@link ComputeNodeMaster}. Each {@link ComputeNodeMaster} has
 * one instance of {@link SubtaskMaster} for each active core in that job's compute node. The class
 * performs the following activities:
 * <ol>
 * <li>Uses an instance of {@link SubtaskClient} to get new subtasks to process from the
 * {@link SubtaskServer} until it finds one that isn't locked (i.e., one that's not already being
 * processed by another job).
 * <li>Places a job information file in the subtask directory that contains the identity of the job
 * name, job ID, and compute node that are going to process the subtask.
 * <li>Processes the subtask using the specified processing algorithm via a {@link DefaultExecutor}.
 * <li>Once all subtasks are processed, exit.
 * </ol>
 * <p>
 *
 * @author PT
 */
public class SubtaskMaster implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SubtaskMaster.class);

    public static final String JOB_INFO_PREFIX = ".jobinfo.";

    int threadNumber = -1;
    private final String node;
    private final Semaphore complete;
    private final String binaryName;
    private final String taskDir;
    private final int timeoutSecs;
    private final String homeDir;
    private final String pipelineConfigPath;
    private final String jobId;
    private final String jobName;

    public SubtaskMaster(int threadNumber, String node, Semaphore complete, String binaryName,
        String taskDir, int timeoutSecs, String homeDir, String pipelineConfigPath) {
        this.threadNumber = threadNumber;
        this.node = node;
        this.complete = complete;
        this.binaryName = binaryName;
        this.taskDir = taskDir;
        this.timeoutSecs = timeoutSecs;
        this.homeDir = homeDir;
        this.pipelineConfigPath = pipelineConfigPath;

        String fullJobId = System.getenv("PBS_JOBID");
        if (fullJobId != null && !fullJobId.isEmpty()) {
            jobId = fullJobId.split("\\.")[0];
            jobName = System.getenv("PBS_JOBNAME");
            log.info(
                "job ID: " + jobId + ", job name: " + jobName + ", thread number: " + threadNumber);
        } else {
            jobId = "none";
            jobName = "none";
        }
    }

    @Override
    public void run() {
        try {
            processSubtasks();
            log.info("Node: " + node + "[" + threadNumber
                + "]: No more subtasks to process, thread exiting");
        } catch (InterruptedException i) {
            log.error(
                "Exiting SubtaskMaster on thread " + threadNumber + " due to thread interruption");
        } finally {
            complete.release();
        }
    }

    /**
     * Generate external processes to execute subtask algorithms. This method will block waiting for
     * the {@link SubtaskClient} to receive a reply to its request for the next subtask for
     * processing.
     *
     * @throws InterruptedException if the thread is interrupted.
     */
    private void processSubtasks() throws InterruptedException {

        SubtaskClient subtaskClient = null;
        SubtaskServer.Response response = null;
        int subtaskIndex = -1;
        while (true) {
            try {
                subtaskClient = subtaskClient();

                response = subtaskClient.nextSubtask();

                if (!response.successful()) {
                    break;
                }
                subtaskIndex = response.subtaskIndex;

                log.debug(threadNumber + ": Processing sub-task: " + subtaskIndex);

                File subtaskDir = TaskConfigurationManager.subtaskDirectory(new File(taskDir),
                    subtaskIndex);
                File lockFile = new File(subtaskDir, TaskConfigurationManager.LOCK_FILE_NAME);
                if (getWriteLockWithoutBlocking(lockFile)) {

                    // Note: The SubtaskMaster must not exit due to an exception caused
                    // during algorithm execution, since the SubtaskMaster instances persist
                    // for the duration of their parent ComputeNodeMaster. Hence we use
                    // a literal catch-all block that catches all Exceptions, writes an
                    // error message, and moves on to try and process the next subtask.
                    try {
                        SubtaskUtils.putLogStreamIdentifier(subtaskDir);
                        if (!checkSubtaskState(subtaskDir)) {
                            executeSubtask(subtaskDir, threadNumber, subtaskIndex);
                            subtaskClient.reportSubtaskComplete(subtaskIndex);
                        }
                    } catch (Exception e) {
                        logException(subtaskIndex, e);
                    } finally {
                        SubtaskUtils.putLogStreamIdentifier((String) null);
                        releaseWriteLock(lockFile);
                    }
                } else {
                    subtaskClient.reportSubtaskLocked(subtaskIndex);
                }

            } catch (IOException e) {

                // If an IOException has occurred, it's possible that
                // only the current subtask has failed and that other subtasks can still be
                // processed, so don't halt the search for new subtasks.
                logException(subtaskIndex, e);

            }
        }
    }

    /**
     * Checks for the existence of an {@link AlgorithmStateFiles} from a previous run and reset the
     * state appropriately:
     *
     * <pre>
     * .COMPLETE   : Do nothing and return true
     * .FAILED     : Write log message and return true (i.e., don't attempt to restart)
     * .PROCESSING : Write log message and return false (i.e., unlocked subtasks with .PROCESSING
     *               can be processed, since the job that was processing exited before completion)
     * No File : do nothing, return false
     * removed by RemoteTaskMaster before this object was even created.
     * </pre>
     *
     * @param subtaskDir
     * @return
     */
    private boolean checkSubtaskState(File subtaskDir) {
        AlgorithmStateFiles previousAlgorithmState = algorithmStateFiles(subtaskDir);

        if (!previousAlgorithmState.subtaskStateExists()) {
            // no previous run exists
            log.info("No previous algorithm state file found in " + subtaskDir.getName()
                + ", executing this subtask");
            return false;
        }

        if (previousAlgorithmState.isComplete()) {
            log.info("subtask algorithm state = COMPLETE, skipping subtask" + subtaskDir.getName());
            return true;
        }

        if (previousAlgorithmState.isFailed()) {
            log.info(".FAILED state detected in directory " + subtaskDir.getName());
            return true;
        }

        if (previousAlgorithmState.isProcessing()) {
            log.info(".PROCESSING state detected in directory " + subtaskDir.getName());
            return true;
        }

        log.info(
            "Unexpected subtask algorithm state = " + previousAlgorithmState.currentSubtaskState()
                + ", restarting subtask " + subtaskDir.getName());
        return false;
    }

    /**
     * Executes the processing algorithm on the current subtask via an external process.
     *
     * @throws IOException if an IO error occurs during the external process.
     */
    private void executeSubtask(File subtaskDir, int threadNumber, int subtaskIndex)
        throws IOException {

        int retCode = 0;

        try {

            // Put in the current correct job info file
            String jobInfoFileName = JOB_INFO_PREFIX + "jobname." + jobName + ".jobid." + jobId
                + ".node." + node;
            new File(subtaskDir, jobInfoFileName).createNewFile();
            TimestampFile.create(subtaskDir, TimestampFile.Event.SUB_TASK_START);

            SubtaskExecutor subtaskExecutor = subtaskExecutor(subtaskIndex);

            log.info("START subtask: " + subtaskIndex + " on " + node + "[" + threadNumber + "]");
            retCode = subtaskExecutor.execAlgorithm();
            log.info("FINISH subtask " + subtaskIndex + " on " + node + ", rc: " + retCode);

        } finally {
            TimestampFile.create(subtaskDir, TimestampFile.Event.SUB_TASK_FINISH);
        }

        if (retCode != 0) {
            throw new ModuleFatalProcessingException(
                "Failed to run: " + binaryName + ", retCode=" + retCode);
        }
    }

    /**
     * Returns a new instance of {@link SubtaskClient}. Implemented as a separate method to support
     * testing.
     */
    SubtaskClient subtaskClient() {
        return new SubtaskClient();
    }

    /**
     * Attempts to acquire a write lock on a lock file, but doesn't block when the lock file is
     * already locked by another user. Implemented as a separate method to support testing.
     */
    boolean getWriteLockWithoutBlocking(File lockFile) throws IOException {
        return LockManager.getWriteLockWithoutBlocking(lockFile);
    }

    /**
     * Releases a write lock held by this object. Implemented as a separate method to support
     * testing.
     */
    void releaseWriteLock(File lockFile) throws IOException {
        LockManager.releaseWriteLock(lockFile);
    }

    /**
     * Returns an {@link AlgorithmStateFiles} instance for the specified subtask directory.
     * Implemented as a separate method to support testing.
     */
    AlgorithmStateFiles algorithmStateFiles(File subtaskDir) {
        return new AlgorithmStateFiles(subtaskDir);
    }

    /**
     * Logs an {@link Exception} that occurs during processing. Implemented as a separate method to
     * support testing.
     */
    void logException(int subtaskIndex, Exception e) {
        log.error("Error occurred during processing of subtask " + subtaskIndex, e);
    }

    /**
     * Constructs a new instance of {@link SubtaskExecutor}. Implemented as a separate method to
     * support testing.
     */
    SubtaskExecutor subtaskExecutor(int subtaskIndex) throws IOException {
        return new SubtaskExecutor.Builder().taskDir(new File(taskDir))
            .binaryName(binaryName)
            .subtaskIndex(subtaskIndex)
            .timeoutSecs(timeoutSecs)
            .pipelineConfigPath(pipelineConfigPath)
            .pipelineHomeDir(homeDir)
            .build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(binaryName, homeDir, jobId, jobName, node, taskDir, threadNumber);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SubtaskMaster other = (SubtaskMaster) obj;
        return Objects.equals(binaryName, other.binaryName)
            && Objects.equals(homeDir, other.homeDir) && Objects.equals(jobId, other.jobId)
            && Objects.equals(jobName, other.jobName) && Objects.equals(node, other.node)
            && Objects.equals(taskDir, other.taskDir) && threadNumber == other.threadNumber;
    }
}
