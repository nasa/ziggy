/*
 * Copyright (C) 2022-2025 United States Government as represented by the Administrator of the
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import gov.nasa.ziggy.module.AlgorithmStateFiles.AlgorithmState;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.BuildInfo;
import gov.nasa.ziggy.util.HostNameUtils;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

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
public class ComputeNodeMaster {

    private static final Logger log = LoggerFactory.getLogger(ComputeNodeMaster.class);

    private final String workingDir;
    private int coresPerNode;

    private final File taskDir;
    private String nodeName = "<unknown>";

    private SubtaskServer subtaskServer;
    private CountDownLatch subtaskMasterCountdownLatch;
    private ExecutorService threadPool;

    private Set<SubtaskMaster> subtaskMasters = new HashSet<>();

    private TaskConfiguration inputsHandler;

    public ComputeNodeMaster(String workingDir) {
        this.workingDir = workingDir;

        log.info("RemoteTaskMaster START");
        log.info(" workingDir = {}", workingDir);

        // Tell anyone who cares that this task is no longer queued.
        new AlgorithmStateFiles(new File(workingDir)).updateCurrentState(AlgorithmState.PROCESSING);

        nodeName = HostNameUtils.shortHostName();

        taskDir = new File(workingDir);
    }

    /**
     * Initializes the {@link ComputeNodeMaster}. A number of timestamp files are created if needed
     * in the task directory; a {@link SubtaskServer} instance is created in for the node; a
     * collection of {@link SubtaskMaster} instances are started.
     */
    public void initialize() {

        log.info("Starting ComputeNodeMaster ({}, {})", BuildInfo.ziggyVersion(),
            BuildInfo.pipelineVersion());
        ZiggyConfiguration.logJvmProperties();

        coresPerNode = activeCoresFromFile();

        subtaskServer().start();
        createTimestamps();

        log.info("Starting {} subtask masters", coresPerNode);
        startSubtaskMasters();
    }

    private void createTimestamps() {
        long arriveTime = System.currentTimeMillis();

        TimestampFile.create(taskDir, TimestampFile.Event.ARRIVE_COMPUTE_NODES, arriveTime);
        TimestampFile.create(taskDir, TimestampFile.Event.START);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private int activeCoresFromFile() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(
                taskDir.toPath().resolve(AlgorithmExecutor.ACTIVE_CORES_FILE_NAME).toFile()),
            ZiggyFileUtils.ZIGGY_CHARSET))) {
            String fileText = reader.readLine();
            return Integer.parseInt(fileText);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Starts the {@link SubtaskMaster} instances in the threads of a thread pool, one thread per
     * active cores on this node. A {@link CountDownLatch} is used to track the number of
     * {@link SubtaskMaster} instances currently running.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    private void startSubtaskMasters() {

        int timeoutSecs = wallTimeFromFile();
        subtaskMasterCountdownLatch = new CountDownLatch(coresPerNode);
        threadPool = subtaskMasterThreadPool();
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("SubtaskMaster[%d]")
            .build();
        String executableName = ZiggyConfiguration.getInstance()
            .getString(PropertyName.ZIGGY_ALGORITHM_NAME.property());
        for (int i = 0; i < coresPerNode; i++) {
            SubtaskMaster subtaskMaster = new SubtaskMaster(i, nodeName,
                subtaskMasterCountdownLatch, executableName, workingDir, timeoutSecs);
            subtaskMasters.add(subtaskMaster);
            threadPool.submit(subtaskMaster, threadFactory);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private int wallTimeFromFile() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(
                taskDir.toPath().resolve(AlgorithmExecutor.WALL_TIME_FILE_NAME).toFile()),
            ZiggyFileUtils.ZIGGY_CHARSET))) {
            String fileText = reader.readLine();
            return Integer.parseInt(fileText);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void awaitSubtaskMastersComplete() {
        try {
            subtaskMasterCountdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * If monitoring ended with successful completion of the job, create a timestamp for the
     * completion time in the task directory.
     */
    public void finish() {

        TimestampFile.create(taskDir, TimestampFile.Event.FINISH);
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

    int subtaskMastersCount() {
        return subtaskMasters.size();
    }

    /**
     * Restores the {@link TaskConfigurationHandler} from disk. Package scope so it can be replaced
     * with a mocked instance.
     */
    TaskConfiguration getTaskConfiguration() {
        if (inputsHandler == null) {
            inputsHandler = TaskConfiguration.deserialize(taskDir);
        }
        return inputsHandler;
    }

    /**
     * Returns a new instance of {@link SubtaskServer}. Broken out as a separate method to support
     * testing.
     */
    SubtaskServer subtaskServer() {
        if (subtaskServer == null) {
            subtaskServer = new SubtaskServer(coresPerNode, getTaskConfiguration());
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
        if (args.length != 1) {
            System.err.println("USAGE: ComputeNodeMaster workingDir");
            System.exit(-1);
        }

        String workingDir = args[0];
        TaskLog.endConsoleLogging();
        ComputeNodeMaster computeNodeMaster = null;

        // Startup: constructor and initialization
        try {
            computeNodeMaster = new ComputeNodeMaster(workingDir);
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

        // Wait for the subtask masters to finish.
        computeNodeMaster.awaitSubtaskMastersComplete();

        // Wrap-up: finalize and clean up
        computeNodeMaster.finish();
        computeNodeMaster.cleanup();
        System.exit(0);
    }
}
