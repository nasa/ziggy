/*
 * Copyright (C) 2022 United States Government as represented by the Administrator of the National
 * Aeronautics and Space Administration. All Rights Reserved.
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
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import gov.nasa.ziggy.module.remote.TimestampFile;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.util.TimeFormatter;
import gov.nasa.ziggy.util.ZiggyBuild;
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
 *
 * @author Todd Klaus
 * @author PT
 */
public class ComputeNodeMaster implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ComputeNodeMaster.class);

    private static final long SLEEP_INTERVAL_MILLIS = 10000;

    private static SocketException socketException;

    private final String workingDir;
    private final String homeDir;
    private final String pipelineConfigPath;
    private final TaskLog algorithmLog;
    private int coresPerNode;

    private final StateFile stateFile;
    private final File stateFileDir;
    private final File taskDir;
    private final File stateFileLockFile;
    private String nodeName;
    private String nodeFullName;
    private int subtaskServerPort;
    private TaskMonitor monitor;

    private Semaphore subtaskMasters;
    private CountDownLatch monitoringLatch = new CountDownLatch(1);
    private ExecutorService threadPool;

    private TaskConfigurationManager inputsHandler;

    /**
     * @param workingDir
     * @param homeDir
     * @param stateFilePath
     * @param pipelineConfigPath
     * @param algorithmLog
     * @throws Exception
     */
    public ComputeNodeMaster(String workingDir, String homeDir, String stateFilePath,
        String pipelineConfigPath, TaskLog algorithmLog) throws Exception {
        this.workingDir = workingDir;
        this.homeDir = homeDir;
        this.pipelineConfigPath = pipelineConfigPath;
        this.algorithmLog = algorithmLog;

        log.info("RemoteTaskMaster START");
        log.info(" workingDir = " + workingDir);
        log.info(" homeDir = " + homeDir);
        log.info(" stateFilePath = " + stateFilePath);
        log.info(" pipelineConfigPath = " + pipelineConfigPath);
        log.info(" algorithmLog = " + algorithmLog);

        stateFile = StateFile.newStateFileFromDiskFile(new File(stateFilePath), true);
        stateFileDir = new File(stateFilePath).getParentFile();
        taskDir = new File(workingDir);
        stateFileLockFile = stateFile.lockFile();
    }

    /**
     * Initializes the {@link ComputeNodeMaster}. Specifically, it locates the file that carries the
     * node name of the node with the {@link SubtaskServer} and starts new threads for the
     * {@link SubtaskMaster} instances. For the node that is going to host the {@link SubtaskServer}
     * instance it also starts the server, updates the {@link StateFile}, creates symlinks, and
     * creates task-start timestamps.
     *
     * @throws IOException if unable to lock a file in the task directory or determine the host name
     * of the node.
     * @throws InterruptedException if the start of the {@link SubtaskServer} or the
     * {@link SubtaskMaster}s are interrupted.
     * @throws IllegalStateException if thrown while attempting to update the state file.
     * @throws ConfigurationException if thrown while attempting to update the state file.
     */
    public void initialize()
        throws IOException, InterruptedException, ConfigurationException, IllegalStateException {
        ZiggyBuild.logVersionInfo(log);

        log.info("jvm version:");
        log.info("  java.runtime.name=" + SystemUtils.JAVA_RUNTIME_NAME);
        log.info("  sun.boot.library.path=" + System.getProperty("sun.boot.library.path"));
        log.info("  java.vm.version=" + SystemUtils.JAVA_VM_VERSION);

        // It's possible that this node isn't starting until all of the subtasks are
        // complete! In that case, it should just exit without doing anything else.
        monitor = new TaskMonitor(getInputsHandler(), stateFile, stateFileDir, taskDir);
        monitor.updateState();
        if (monitor.allSubtasksProcessed()) {
            log.info("All subtasks processed, ComputeNodeMaster exiting");
            return;
        }

        coresPerNode = stateFile.getActiveCoresPerNode();

        updateStateFile();
        startSubtaskServer();
        createTimestamps();
        setNodeName();

        log.info("Starting " + coresPerNode + " subtask masters");
        startSubtaskMasters();
    }

    /**
     * Sets the state file for this task to PROCESSING, and clears any FAILED or PROCESSING sub-task
     * states. Note that this update to the state file does not update the sub-task counts (how many
     * completed, how many failed, etc). That update does not happen until the RemoteTaskMonitor
     * begins. However, this step is still necessary so that when the RemoteTaskMonitor does start,
     * the overall processing state and the sub-task states are correct.
     *
     * @throws IOException if unable to release write lock on state file.
     */
    private void updateStateFile() throws IOException {

        // NB: If there are multiple jobs associated with a single task, this update only
        // needs to be performed if this job is the first to start
        boolean stateFileLockObtained = LockManager.getWriteLockWithoutBlocking(stateFileLockFile);
        try {
            StateFile previousStateFile = new StateFile(stateFile);
            if (stateFileLockObtained
                && previousStateFile.getState().equals(StateFile.State.QUEUED)) {
                stateFile.setState(StateFile.State.PROCESSING);
                log.info("Updating state: " + previousStateFile + " -> " + stateFile);

                if (!StateFile.updateStateFile(previousStateFile, stateFile, stateFileDir)) {
                    log.error("Failed to update state file: " + previousStateFile);
                }
            } else {
                log.info("State file already moved to PROCESSING state, not modifying state file");
                stateFile.setState(StateFile.State.PROCESSING);
            }
        } finally {
            if (stateFileLockObtained) {
                LockManager.releaseWriteLock(stateFileLockFile);
            }
        }
    }

    private void setNodeName() throws UnknownHostException {
        nodeFullName = InetAddress.getLocalHost().getHostName();
        log.info("Node full name: " + nodeFullName);
        String[] nodeFullNameParts = nodeFullName.split("\\.");
        nodeName = nodeFullNameParts[0];

    }

    /**
     * Starts the {@link SubtaskServer}.
     *
     * @throws InterruptedException if the server is interrupted during initialization.
     */
    private void startSubtaskServer() throws InterruptedException {
        SubtaskServer subtaskServer = new SubtaskServer(nodeFullName, getInputsHandler());
        subtaskServer.startSubtaskServer();
        subtaskServerPort = subtaskServer.getServerPort();
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
     *
     * @throws InterruptedException if the semaphore acquire method is interrupted.
     */
    private void startSubtaskMasters() throws InterruptedException {

        int timeoutSecs = (int) TimeFormatter
            .timeStringHhMmSsToTimeInSeconds(stateFile.getRequestedWallTime());
        subtaskMasters = new Semaphore(coresPerNode);
        threadPool = Executors.newFixedThreadPool(coresPerNode);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("SubtaskMaster[%d]")
            .build();
        for (int i = 0; i < coresPerNode; i++) {
            subtaskMasters.acquire();
            threadPool.submit(new SubtaskMaster(i, nodeName, nodeFullName, subtaskMasters,
                stateFile.getModuleName(), workingDir, timeoutSecs, homeDir, pipelineConfigPath,
                subtaskServerPort), threadFactory);
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
     *
     * @throws InterruptedException if the call to wait is interrupted.
     */
    public void monitor() throws InterruptedException {

        log.info("Waiting for subtasks to complete");

        ScheduledThreadPoolExecutor monitoringThreadPool = new ScheduledThreadPoolExecutor(1);
        monitoringThreadPool.scheduleAtFixedRate(this, 0L, SLEEP_INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS);
        monitoringLatch.await();
        monitoringThreadPool.shutdownNow();
    }

    @Override
    public void run() {

        if (monitoringLatch.getCount() == 0) {
            return;
        }

        // If one of the subtask masters detected that the subtask server has failed, then we
        // don't need to do any finalization, just exit and start the process of all subtask
        // master threads stopping.
        if (hasSocketException()) {
            log.error("ComputeNodeMaster: error exit");
            return;
        }

        // Do state checks and updates
        boolean allSubtasksProcessed = monitor.allSubtasksProcessed();
        try {
            monitor.updateState();
        } catch (PipelineException | IllegalStateException | IOException e) {
            throw new PipelineException("Unable to update state file", e);
        }

        // If the task is slated for deletion, exit monitoring immediately.
        if (monitor.isDeleted()) {
            log.info("Task in deleted state, exiting");
            endMonitoring();
            return;
        }

        // If all the subtasks are either completed or failed, exit monitoring
        // immediately
        if (allSubtasksProcessed) {
            log.info("All subtasks complete");
            endMonitoring();
            return;
        }

        // If all RemoteSubtaskMasters are done we can exit monitoring
        if (subtaskMasters.availablePermits() == coresPerNode) {
            endMonitoring();
            return;
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
     *
     * @throws InterruptedException if the sleep to allow the NFS client cache to update is
     * interrupted.
     * @throws IOException if unable to mark the state file as done.
     * @throws IllegalStateException if unable to mark the state file as done.
     */
    public void finish() throws InterruptedException, IOException {

        TimestampFile.create(taskDir, TimestampFile.Event.PBS_JOB_FINISH);
        if (monitor.allSubtasksProcessed()) {
            monitor.markStateFileDone();
        }
        log.info("ComputeNodeMaster: Done");
    }

    public static void setSocketException(SocketException socketException) {
        ComputeNodeMaster.socketException = socketException;
    }

    public static boolean hasSocketException() {
        return socketException != null;
    }

    /**
     * Final cleanup from job execution. The {@link SubtaskMaster} thread pool is shut down and
     * logging is terminated.
     */
    public void cleanup() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        algorithmLog.endLogging();
    }

    private TaskConfigurationManager getInputsHandler() {
        if (inputsHandler == null) {
            inputsHandler = TaskConfigurationManager.restore(taskDir);
        }
        return inputsHandler;
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("USAGE: ComputeNodeMaster workingDir homeDir "
                + "stateFilePath pipelineConfigPath logFilePath");
            System.exit(-1);
        }

        String workingDir = args[0];
        String homeDir = args[1];
        String stateFilePath = args[2];
        String spiffyConfigPath = args[3];
        String logFilePath = args[4];

        // Start up logging
        TaskLog algorithmLog = new TaskLog(logFilePath);
        algorithmLog.startLogging();

        ComputeNodeMaster computeNodeMaster = null;

        // Startup: constructor and initialization
        try {
            computeNodeMaster = new ComputeNodeMaster(workingDir, homeDir, stateFilePath,
                spiffyConfigPath, algorithmLog);
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
        try {
            computeNodeMaster.monitor();
        } catch (InterruptedException e) {
            log.error("ComputeNodeMaster monitoring failed", e);
            computeNodeMaster.cleanup();
            System.exit(1);
        }

        // Wrap-up: finalize and clean up
        try {
            computeNodeMaster.finish();
        } catch (InterruptedException | IllegalStateException | IOException e) {
            log.error("ComputeNodeMaster wrap-up failed", e);
            System.exit(1);
        } finally {
            computeNodeMaster.cleanup();
        }
        System.exit(0);

    }
}
