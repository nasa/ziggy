package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.SubtaskServer.ResponseType;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.module.io.AlgorithmErrorReturn;
import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
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
 *
 * @author PT
 */
public class SubtaskMaster implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SubtaskMaster.class);

    public static final String JOB_INFO_PREFIX = ".jobinfo.";

    int threadNumber = -1;
    private final String node;
    private final CountDownLatch countdownLatch;
    private final String binaryName;
    private final String taskDir;
    private final int timeoutSecs;
    private final String jobId;
    private final String jobName;

    public SubtaskMaster(int threadNumber, String node, CountDownLatch countdownLatch,
        String binaryName, String taskDir, int timeoutSecs) {
        this.threadNumber = threadNumber;
        this.node = node;
        this.countdownLatch = countdownLatch;
        this.binaryName = binaryName;
        this.taskDir = taskDir;
        this.timeoutSecs = timeoutSecs;

        String fullJobId = System.getenv("PBS_JOBID");
        if (!StringUtils.isBlank(fullJobId)) {
            jobId = fullJobId.split("\\.")[0];
            jobName = System.getenv("PBS_JOBNAME");
            log.info("jobId={}, jobName={}, threadNumber={}", jobId, jobName, threadNumber);
        } else {
            jobId = "none";
            jobName = "none";
        }
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_IN_RUNNABLE)
    public void run() {
        try {
            processSubtasks();
            log.info("Node: {}[{}]: No more subtasks to process, thread exiting", node,
                threadNumber);
        } catch (Exception e) {
            log.error("Exception thrown in SubtaskMaster", e);
        } finally {
            countdownLatch.countDown();
        }
    }

    /**
     * Generate external processes to execute subtask algorithms. This method will block waiting for
     * the {@link SubtaskClient} to receive a reply to its request for the next subtask for
     * processing.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void processSubtasks() {

        SubtaskClient subtaskClient = null;
        SubtaskServer.Response response = null;
        int subtaskIndex = -1;
        while (true) {
            subtaskClient = subtaskClient();

            response = subtaskClient.nextSubtask();

            if (response == null) {
                log.error("Null response from SubtaskClient, exiting");
                break;
            }
            if (response.status.equals(ResponseType.NO_MORE)) {
                log.debug("Received no-more message from server");
                break;
            }
            if (!response.successful()) {
                log.error("Unsuccessful response from SubtaskClient, exiting");
                log.error("Response is {}", response.toString());
                break;
            }
            subtaskIndex = response.subtaskIndex;

            log.debug("threadNumber={}, subtaskIndex={}", threadNumber, subtaskIndex);

            File subtaskDir = SubtaskUtils.subtaskDirectory(Paths.get(taskDir), subtaskIndex)
                .toFile();
            File lockFile = new File(subtaskDir, TaskConfiguration.LOCK_FILE_NAME);
            boolean lockFileObtained = false;
            try {
                if (getWriteLockWithoutBlocking(lockFile)) {
                    lockFileObtained = true;
                    SubtaskUtils.putLogStreamIdentifier(subtaskDir);
                    if (!checkSubtaskState(subtaskDir)) {
                        executeSubtask(subtaskDir, threadNumber, subtaskIndex);
                    }
                    subtaskClient.reportSubtaskComplete(subtaskIndex);
                } else {
                    subtaskClient.reportSubtaskLocked(subtaskIndex);
                }
            } catch (Exception e) {
                // We don't want any exception that occurred in processing a subtask
                // (including runtime exceptions) to stop the SubtaskMaster instance
                // from processing further subtasks, consequently we catch Exception
                // here to prevent same. The higher-level monitoring will manage any
                // cases in which a subtask's processing failed.
                logException(subtaskIndex, e);

                // Also, tell the server and allocator not to bother trying again with
                // this subtask.
                subtaskClient.reportSubtaskComplete(subtaskIndex);
            } finally {
                SubtaskUtils.putLogStreamIdentifier((String) null);
                if (lockFileObtained) {
                    releaseWriteLock(lockFile);
                    lockFileObtained = false;
                }
                logAlgorithmStackTrace(subtaskDir, subtaskIndex);
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

        if (!previousAlgorithmState.stateExists()) {
            // no previous run exists
            log.info("No previous algorithm state file found in {}, executing this subtask",
                subtaskDir.getName());
            return false;
        }

        if (previousAlgorithmState.isComplete()) {
            log.info("Subtask algorithm state COMPLETE, skipping subtask{}", subtaskDir.getName());
            return true;
        }

        if (previousAlgorithmState.isFailed()) {
            log.info(".FAILED state detected in directory {}", subtaskDir.getName());
            return true;
        }

        if (previousAlgorithmState.isProcessing()) {
            log.info(".PROCESSING state detected in directory {}", subtaskDir.getName());
            return true;
        }

        log.info("Unexpected subtask algorithm state {}, restarting subtask {}",
            previousAlgorithmState.currentAlgorithmState(), subtaskDir.getName());
        return false;
    }

    /**
     * Executes the processing algorithm on the current subtask via an external process.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void executeSubtask(File subtaskDir, int threadNumber, int subtaskIndex) {

        int retCode = 0;

        // Put in the current correct job info file
        String jobInfoFileName = JOB_INFO_PREFIX + "jobname." + jobName + ".jobid." + jobId
            + ".node." + node;
        try {
            new File(subtaskDir, jobInfoFileName).createNewFile();
            TimestampFile.create(subtaskDir, TimestampFile.Event.SUBTASK_START);

            SubtaskExecutor subtaskExecutor = subtaskExecutor(subtaskIndex);

            log.info("START subtask {} on {}[{}]", subtaskIndex, node, threadNumber);
            retCode = subtaskExecutor.execAlgorithm();
            log.info("FINISH subtask {} on {} (retCode={})", subtaskIndex, node, retCode);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to create file " + new File(subtaskDir, jobInfoFileName).toString(), e);
        } finally {
            TimestampFile.create(subtaskDir, TimestampFile.Event.SUBTASK_FINISH);
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
    boolean getWriteLockWithoutBlocking(File lockFile) {
        return LockManager.getWriteLockWithoutBlocking(lockFile);
    }

    /**
     * Releases a write lock held by this object. Implemented as a separate method to support
     * testing.
     */
    void releaseWriteLock(File lockFile) {
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
        log.error("Error occurred during processing of subtask {}", subtaskIndex, e);
    }

    /** Writes the algorithm stack trace, if any, to the algorithm log. */
    void logAlgorithmStackTrace(File subtaskDir, int subtaskIndex) {
        File errorFile = ModuleInterfaceUtils.errorFile(subtaskDir, binaryName);
        if (errorFile.exists()) {
            AlgorithmErrorReturn algorithmStack = new AlgorithmErrorReturn();
            new Hdf5ModuleInterface().readFile(errorFile, algorithmStack, true);
            algorithmStack.logStackTrace();
        }
    }

    /**
     * Constructs a new instance of {@link SubtaskExecutor}. Implemented as a separate method to
     * support testing.
     */
    SubtaskExecutor subtaskExecutor(int subtaskIndex) {
        return new SubtaskExecutor.Builder().taskDir(new File(taskDir))
            .binaryName(binaryName)
            .subtaskIndex(subtaskIndex)
            .timeoutSecs(timeoutSecs)
            .build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(binaryName, jobId, jobName, node, taskDir, threadNumber);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SubtaskMaster other = (SubtaskMaster) obj;
        return Objects.equals(binaryName, other.binaryName) && Objects.equals(jobId, other.jobId)
            && Objects.equals(jobName, other.jobName) && Objects.equals(node, other.node)
            && Objects.equals(taskDir, other.taskDir) && threadNumber == other.threadNumber;
    }
}
