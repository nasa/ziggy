/*
 * Copyright (C) 2022-2023 United States Government as represented by the Administrator of the
 * National Aeronautics and Space Administration. All Rights Reserved.
 *
 * NASA acknowledges the SETI Institute's primary role in authoring and producing Ziggy, a Pipeline
 * Management System for Data Analysis Pipelines, under Cooperative Agreement Nos. NNX14AH97A,
 * 80NSSC18M0068 & 80NSSC21M0079.
 *
 * This file is available under the terms of the NASA Open Source Agreement (NOSA). You should have
 * received a copy of this agreement with the Ziggy source code; see the file LICENSE.pdf.
 *
 * Disclaimers
 *
 * No Warranty: THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF ANY KIND, EITHER
 * EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY THAT THE SUBJECT
 * SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL BE
 * ERROR FREE, OR ANY WARRANTY THAT DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE SUBJECT
 * SOFTWARE. THIS AGREEMENT DOES NOT, IN ANY MANNER, CONSTITUTE AN ENDORSEMENT BY GOVERNMENT AGENCY
 * OR ANY PRIOR RECIPIENT OF ANY RESULTS, RESULTING DESIGNS, HARDWARE, SOFTWARE PRODUCTS OR ANY
 * OTHER APPLICATIONS RESULTING FROM USE OF THE SUBJECT SOFTWARE. FURTHER, GOVERNMENT AGENCY
 * DISCLAIMS ALL WARRANTIES AND LIABILITIES REGARDING THIRD-PARTY SOFTWARE, IF PRESENT IN THE
 * ORIGINAL SOFTWARE, AND DISTRIBUTES IT "AS IS."
 *
 * Waiver and Indemnity: RECIPIENT AGREES TO WAIVE ANY AND ALL CLAIMS AGAINST THE UNITED STATES
 * GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT. IF RECIPIENT'S
 * USE OF THE SUBJECT SOFTWARE RESULTS IN ANY LIABILITIES, DEMANDS, DAMAGES, EXPENSES OR LOSSES
 * ARISING FROM SUCH USE, INCLUDING ANY DAMAGES FROM PRODUCTS BASED ON, OR RESULTING FROM,
 * RECIPIENT'S USE OF THE SUBJECT SOFTWARE, RECIPIENT SHALL INDEMNIFY AND HOLD HARMLESS THE UNITED
 * STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT, TO THE
 * EXTENT PERMITTED BY LAW. RECIPIENT'S SOLE REMEDY FOR ANY SUCH MATTER SHALL BE THE IMMEDIATE,
 * UNILATERAL TERMINATION OF THIS AGREEMENT.
 */

package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import gov.nasa.ziggy.module.StateFile.State;
import gov.nasa.ziggy.module.remote.TimestampFile;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.HostNameUtils;
import gov.nasa.ziggy.util.TimeFormatter;
import gov.nasa.ziggy.util.io.LockManager;

/**
 * Acts as a controller for a single-node remote job and associated subtasks running on the node.
 * <p>
 * The class starts a set of {@link SubtaskMaster} instances in separate threads, each of which can
 * process one subtask at a time. The class also monitors progress to determine whether the subtasks
 * are all completed and/or the {@link SubtaskMaster}s have all exited. Finally, the class also
 * starts a {@link SubtaskServer} instance to dispatch subtasks to the {@link SubtaskMaster}
 * instances as they complete existing subtasks and are free to start new ones.
 * <p>
 * The {@link ComputeNodeMaster} requires that the environment variable PIPELINE_CONFIG_PATH be set
 * on the computer that runs the {@link ComputeNodeMaster} instance.
 *
 * @author Todd Klaus
 * @author PT
 */
public class ComputeNodeMaster implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ComputeNodeMaster.class);

    private static final long SLEEP_INTERVAL_MILLIS = 10000;

    private final String workingDir;
    private int coresPerNode;

    private final StateFile stateFile;
    private final File taskDir;
    private final File stateFileLockFile;
    private String nodeName = "<unknown>";
    private TaskMonitor monitor;

    private SubtaskServer subtaskServer;
    private Semaphore subtaskMasterSemaphore;
    private CountDownLatch monitoringLatch = new CountDownLatch(1);
    private ExecutorService threadPool;

    private Set<SubtaskMaster> subtaskMasters = new HashSet<>();

    private TaskConfigurationManager inputsHandler;

    public ComputeNodeMaster(String workingDir, TaskLog algorithmLog) {
        this.workingDir = workingDir;

        log.info("RemoteTaskMaster START");
        log.info(" workingDir = " + workingDir);
        log.info(" algorithmLog = " + algorithmLog);

        nodeName = HostNameUtils.shortHostName();

        stateFile = StateFile.of(Paths.get(workingDir)).newStateFileFromDiskFile();
        taskDir = new File(workingDir);
        stateFileLockFile = stateFile.lockFile();
    }

    /**
     * Initializes the {@link ComputeNodeMaster}. Specifically, it locates the file that carries the
     * node name of the node with the {@link SubtaskServer} and starts new threads for the
     * {@link SubtaskMaster} instances. For the node that is going to host the {@link SubtaskServer}
     * instance it also starts the server, updates the {@link StateFile}, creates symlinks, and
     * creates task-start timestamps.
     */
    public void initialize() {

        log.info("Starting ComputeNodeMaster ({})",
            ZiggyConfiguration.getInstance().getString(PropertyName.ZIGGY_VERSION.property()));
        ZiggyConfiguration.logJvmProperties();

        // It's possible that this node isn't starting until all of the subtasks are
        // complete! In that case, it should just exit without doing anything else.
        monitor = new TaskMonitor(getInputsHandler(), stateFile, taskDir);
        monitor.updateState();
        if (monitor.allSubtasksProcessed()) {
            log.info("All subtasks processed, ComputeNodeMaster exiting");
            return;
        }

        coresPerNode = stateFile.getActiveCoresPerNode();

        updateStateFile();
        subtaskServer().start();
        createTimestamps();

        log.info("Starting " + coresPerNode + " subtask masters");
        startSubtaskMasters();
    }

    /**
     * Moves the state file for this task from QUEUED to PROCESSING.
     *
     * @throws IOException if unable to release write lock on state file.
     */
    private void updateStateFile() {

        // NB: If there are multiple jobs associated with a single task, this update only
        // needs to be performed if this job is the first to start
        boolean stateFileLockObtained = getWriteLockWithoutBlocking(stateFileLockFile);
        try {
            StateFile previousStateFile = new StateFile(stateFile);
            if (stateFileLockObtained
                && previousStateFile.getState().equals(StateFile.State.QUEUED)) {
                stateFile.setState(StateFile.State.PROCESSING);
                log.info("Updating state: " + previousStateFile + " -> " + stateFile);

                if (!StateFile.updateStateFile(previousStateFile, stateFile)) {
                    log.error("Failed to update state file: " + previousStateFile);
                }
            } else {
                log.info("State file already moved to PROCESSING state, not modifying state file");
                stateFile.setState(StateFile.State.PROCESSING);
            }
        } finally {
            if (stateFileLockObtained) {
                releaseWriteLock(stateFileLockFile);
            }
        }
    }

    private void createTimestamps() {
        long arriveTime = stateFile.getPfeArrivalTimeMillis() != StateFile.INVALID_VALUE
            ? stateFile.getPfeArrivalTimeMillis()
            : System.currentTimeMillis();
        long submitTime = stateFile.getPbsSubmitTimeMillis();

        TimestampFile.create(taskDir, TimestampFile.Event.ARRIVE_PFE, arriveTime);
        TimestampFile.create(taskDir, TimestampFile.Event.QUEUED_PBS, submitTime);
        TimestampFile.create(taskDir, TimestampFile.Event.PBS_JOB_START);
    }

    /**
     * Starts the {@link SubtaskMaster} instances in the threads of a thread pool, one thread per
     * active cores on this node. A {@link Semaphore} is used to track the number of
     * {@link SubtaskMaster} instances currently running.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    private void startSubtaskMasters() {

        int timeoutSecs = (int) TimeFormatter
            .timeStringHhMmSsToTimeInSeconds(stateFile.getRequestedWallTime());
        subtaskMasterSemaphore = new Semaphore(coresPerNode);
        threadPool = subtaskMasterThreadPool();
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("SubtaskMaster[%d]")
            .build();
        for (int i = 0; i < coresPerNode; i++) {
            try {
                subtaskMasterSemaphore.acquire();
            } catch (InterruptedException e) {
                // This can never occur. The number of permits is equal to the number of threads,
                // thus there is no need to wait for a permit to become available.
                throw new AssertionError(e);
            }
            SubtaskMaster subtaskMaster = new SubtaskMaster(i, nodeName, subtaskMasterSemaphore,
                stateFile.getModuleName(), workingDir, timeoutSecs);
            subtaskMasters.add(subtaskMaster);
            threadPool.submit(subtaskMaster, threadFactory);
        }
    }

    /**
     * Performs periodic checks of subtask processing status. This is accomplished by using a
     * {@link ScheduledThreadPoolExecutor} to check processing at the desired intervals. Execution
     * of the {@link TaskMonitor} thread will block until the monitoring checks determine that
     * processing is completed, at which time the thread resumes execution.
     * <p>
     * The specific conditions under which the monitor will resume execution of the current thread
     * are as follows:
     * <ol>
     * <li>All of the {@link SubtaskMaster} threads have completed.
     * <li>All of the subtasks are either completed or failed.
     * </ol>
     */
    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    public void monitor() {

        log.info("Waiting for subtasks to complete");

        ScheduledThreadPoolExecutor monitoringThreadPool = new ScheduledThreadPoolExecutor(1);
        monitoringThreadPool.scheduleAtFixedRate(this, 0L, SLEEP_INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS);
        try {
            monitoringLatch.await();
        } catch (InterruptedException e) {
            // If the ComputeNodeMaster main thread is interrupted, it means that the entire
            // ComputeNodeMaster is shutting down. We can simply allow it to shut down and don't
            // need to do anything further.
        }
        monitoringThreadPool.shutdownNow();
    }

    @Override
    public void run() {

        if (monitoringLatch.getCount() == 0) {
            return;
        }

        // If the subtask server has failed, then we
        // don't need to do any finalization, just exit and start the process of all subtask
        // master threads stopping.
        if (!subtaskServer().isListenerRunning()) {
            log.error("ComputeNodeMaster: error exit");
            endMonitoring();
            return;
        }

        // Do state checks and updates
        boolean allSubtasksProcessed = monitor.allSubtasksProcessed();
        monitor.updateState();

        // If all the subtasks are either completed or failed, exit monitoring
        // immediately
        if (allSubtasksProcessed) {
            log.info("All subtasks complete");
            endMonitoring();
            return;
        }

        // If all RemoteSubtaskMasters are done we can exit monitoring
        if (allPermitsAvailable()) {
            endMonitoring();
        }
    }

    /**
     * Ends monitoring. This is accomplished by decrementing the {@link CountDownLatch} that the
     * main thread is waiting for.
     */
    private synchronized void endMonitoring() {
        monitoringLatch.countDown();
    }

    /**
     * If monitoring ended with successful completion of the job, create a timestamp for the
     * completion time in the task directory and mark the task's {@link StateFile} as done.
     */
    public void finish() {

        TimestampFile.create(taskDir, TimestampFile.Event.PBS_JOB_FINISH);
        if (monitor.allSubtasksProcessed()) {
            monitor.markStateFileDone();
        }
        log.info("ComputeNodeMaster: Done");
    }

    /**
     * Final cleanup from job execution. The {@link SubtaskMaster} thread pool is shut down and
     * logging is terminated; the {@link SubtaskServer} is shut down.
     */
    public void cleanup() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        subtaskServer().shutdown();
    }

    // The following getter methods are intended for testing purposes only. They do not expose any
    // of the ComputeNodeMaster's private objects to callers. In some cases it is necessary for
    // the methods to be public, as they are used by tests in other packages.
    public long getCountDownLatchCount() {
        return monitoringLatch.getCount();
    }

    public int getSemaphorePermits() {
        if (subtaskMasterSemaphore == null) {
            return -1;
        }
        return subtaskMasterSemaphore.availablePermits();
    }

    int subtaskMastersCount() {
        return subtaskMasters.size();
    }

    State getStateFileState() {
        return stateFile.getState();
    }

    int getStateFileNumComplete() {
        return stateFile.getNumComplete();
    }

    int getStateFileNumFailed() {
        return stateFile.getNumFailed();
    }

    int getStateFileNumTotal() {
        return stateFile.getNumTotal();
    }

    /**
     * Restores the {@link TaskConfigurationHandler} from disk. Package scope so it can be replaced
     * with a mocked instance.
     */
    TaskConfigurationManager getInputsHandler() {
        if (inputsHandler == null) {
            inputsHandler = TaskConfigurationManager.restore(taskDir);
        }
        return inputsHandler;
    }

    /**
     * Attempts to obtain the lock file for the task's state file, but does not block if it cannot
     * obtain it. Broken out as a separate method to support testing.
     *
     * @return true if lock obtained, false otherwise.
     */
    boolean getWriteLockWithoutBlocking(File lockFile) {
        return LockManager.getWriteLockWithoutBlocking(lockFile);
    }

    /**
     * Releases the write lock on a file. Broken out as a separate method to support testing.
     */
    void releaseWriteLock(File lockFile) {
        LockManager.releaseWriteLock(lockFile);
    }

    /**
     * Determines whether all permits are available for the {@link Semaphore} that works with the
     * {@link SubtaskMaster} instances. Broken out as a separate method to support testing.
     */
    boolean allPermitsAvailable() {
        return subtaskMasterSemaphore.availablePermits() == coresPerNode;
    }

    /**
     * Returns a new instance of {@link SubtaskServer}. Broken out as a separate method to support
     * testing.
     */
    SubtaskServer subtaskServer() {
        if (subtaskServer == null) {
            subtaskServer = new SubtaskServer(coresPerNode, getInputsHandler());
        }
        return subtaskServer;
    }

    /**
     * Returns a thread pool with the correct number of threads for the {@link SubtaskMaster}
     * instances. Broken out as a separate method to support testing.
     */
    ExecutorService subtaskMasterThreadPool() {
        return Executors.newFixedThreadPool(coresPerNode);
    }

    @AcceptableCatchBlock(rationale = Rationale.CLEANUP_BEFORE_EXIT)
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("USAGE: ComputeNodeMaster workingDir logFilePath");
            System.exit(-1);
        }

        String workingDir = args[0];
        String logFilePath = args[1];

        // Start up logging
        TaskLog algorithmLog = new TaskLog(logFilePath);
        algorithmLog.startLogging();

        ComputeNodeMaster computeNodeMaster = null;

        // Startup: constructor and initialization
        try {
            computeNodeMaster = new ComputeNodeMaster(workingDir, algorithmLog);
            computeNodeMaster.initialize();
        } catch (Exception e) {

            // Any exception that occurs in the constructor or initialization is
            // fatal, so issue diagnostics and exit.
            log.error("ComputeNodeMaster startup failed", e);
            if (computeNodeMaster != null) {
                computeNodeMaster.cleanup();
            }
            System.exit(1);
        }

        // Monitoring: wait for subtasks to finish, subtask masters to finish, or exceptions
        // to be thrown
        computeNodeMaster.monitor();

        // Wrap-up: finalize and clean up
        computeNodeMaster.finish();
        computeNodeMaster.cleanup();
        System.exit(0);
    }
}
