/*
 * Copyright (C) 2022-2024 United States Government as represented by the Administrator of the
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

package gov.nasa.ziggy.worker;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.logging.TaskLog;
import gov.nasa.ziggy.services.messages.HaltTasksRequest;
import gov.nasa.ziggy.services.messages.HeartbeatMessage;
import gov.nasa.ziggy.services.messages.ShutdownMessage;
import gov.nasa.ziggy.services.messages.TaskHaltedMessage;
import gov.nasa.ziggy.services.messaging.HeartbeatManager;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.services.messaging.ZiggyRmiClient;
import gov.nasa.ziggy.services.process.AbstractPipelineProcess;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.supervisor.TaskRequestHandler;
import gov.nasa.ziggy.util.SystemProxy;
import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Manages execution of a single pipeline task.
 * <p>
 * The {@link PipelineWorker} is started by the {@link TaskRequestHandler} that runs as a thread in
 * the {@link PipelineSupervisor}. The {@link PipelineWorker} runs in its own JVM, as a separate
 * process, in order to ensure that pipeline tasks can be processed in parallel even if they require
 * resources that do not support thread-level concurrency (this includes both HDF5 and netCDF).
 * <p>
 * The worker life cycle is linked to that of the process it manages. As soon as the process
 * completes or hands off to a remote compute node, the worker process exits. Each
 * {@link PipelineWorker} manages one and only one task at a time; the supervisor will create as
 * many {@link PipelineWorker} instances as it requires, subject to resource limits, to process
 * tasks.
 *
 * @author PT
 * @author Bill Wohler
 */
public class PipelineWorker extends AbstractPipelineProcess {

    private static final Logger log = LoggerFactory.getLogger(PipelineSupervisor.class);

    public static final String NAME = "Worker";
    public static final int MAX_TASK_RETRIEVE_RETRIES = 1;
    public static final long WAIT_BETWEEN_RETRIES_MILLIS = 100;
    public static final long FINAL_MESSAGE_LATCH_WAIT_MILLIS = 5000;

    private int workerId;
    private PipelineTask pipelineTask;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    public PipelineWorker(String name, PipelineTask pipelineTask, int workerId) {
        super(name + " " + Integer.toString(workerId));
        this.workerId = workerId;
        this.pipelineTask = pipelineTask;
    }

    /**
     * The {@link #main(String[])} method permits this class to be run in its own JVM, which is a
     * requirement for making it a separate process from the supervisor. Because the worker is never
     * instantiated except by the supervisor, positional arguments are acceptable.
     */
    public static void main(String[] args) {

        int workerId = Integer.parseInt(args[0]);
        long taskId = Long.parseLong(args[1]);
        RunMode runMode = RunMode.valueOf(args[2]);

        // Construct the WorkerProcess instance
        PipelineWorker workerProcess = new PipelineWorker(NAME,
            new PipelineTaskOperations().pipelineTask(taskId), workerId);
        workerProcess.initialize();
        workerProcess.processTask(runMode);
        log.info("Worker exiting with status 0");
        SystemProxy.exit(0);
    }

    /**
     * Main processing method. Instantiates the {@link ZiggyRmiClient} and {@link TaskExecutor}
     * instances, then starts the {@link TaskExecutor#executeTask()} method.
     */
    private void processTask(RunMode runMode) {

        TaskLog.endConsoleLogging();
        pipelineTaskDataOperations().incrementTaskLogIndex(pipelineTask);
        log.info("Process {} instance {} starting", NAME, workerId);

        // Initialize an instance of TaskExecutor
        TaskExecutor taskRequestExecutor = new TaskExecutor(workerId, pipelineTask, runMode);
        addProcessStatusReporter(taskRequestExecutor);

        // Initialize the ProcessHeartbeatManager for this process.
        log.info("Initializing ProcessHeartbeatManager");
        HeartbeatManager.startInstance();
        log.info("Initializing ProcessHeartbeatManager...done");

        // Initialize the UiCommunicator for this process.
        ZiggyRmiClient.start(NAME);
        ZiggyShutdownHook.addShutdownHook(() -> {

            // Note that we need to wait for the final status message to get sent
            // before we reset the ZiggyRmiClient.
            log.debug("Executing worker shutdown hook");
            CountDownLatch latch = new CountDownLatch(1);
            ZiggyMessenger.publish(taskRequestExecutor.statusMessage(true), latch);
            try {
                latch.await(FINAL_MESSAGE_LATCH_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ZiggyRmiClient.reset();
        });

        // Subscribe to messages as needed.
        subscribe();

        // Check for a halt request on the task.
        if (pipelineTaskDataOperations().haltRequested(pipelineTask)) {
            haltTask(new HaltTasksRequest(List.of(pipelineTask)));
        }

        // Start the TaskExecutor
        taskRequestExecutor.executeTask();
    }

    /**
     * Subscribes to messages where the worker as a whole is the intended recipient.
     */
    private void subscribe() {

        // When a heartbeat comes in, send out the update message.
        ZiggyMessenger.subscribe(HeartbeatMessage.class, message -> {
            Thread t = new Thread(() -> {
                log.debug("heartbeat received");
                AbstractPipelineProcess.sendUpdates();
            });
            t.setDaemon(true);
            t.start();
        });

        // When a shutdown request comes in, honor it.
        ZiggyMessenger.subscribe(ShutdownMessage.class, message -> {
            log.info("Shutting down due to shutdown signal");
            killWorker();
        });

        // When a halt-tasks request comes in, honor it if appropriate.
        ZiggyMessenger.subscribe(HaltTasksRequest.class, message -> {
            haltTask(message);
        });
    }

    /**
     * Determines whether a {@link HaltTasksRequest} wants to halt the task in this worker, and if
     * so sends the desired confirmation method and exits.
     * <p>
     * Default axis (package-only) for unit tests.
     *
     * @param message
     */
    void haltTask(HaltTasksRequest message) {
        if (message.getPipelineTasks().contains(pipelineTask)) {
            sendTaskHaltedMessage(message, pipelineTask);
            killWorker();
        }
    }

    /**
     * Sends the {@link TaskHaltedMessage} confirming that the task was in this worker and has been
     * halted.
     * <p>
     * Default access (package-only) for unit tests.
     */
    void sendTaskHaltedMessage(HaltTasksRequest message, PipelineTask pipelineTask) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ZiggyMessenger.publish(new TaskHaltedMessage(pipelineTask), countDownLatch);
        try {
            countDownLatch.await(FINAL_MESSAGE_LATCH_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Kills the worker by calling {@link System#exit(int)}. This method is called when the worker
     * receives a {@link HaltTasksRequest} message from the supervisor, and the task processed in
     * this worker is one of the ones that is to be killed. Because the worker is a standalone
     * process, and because each worker processes one and only one task, this approach (halting the
     * task by killing the worker) is the easiest and most robust way to ensure that the task is
     * stopped.
     * <p>
     * Default access (package-only) for unit test purposes.
     */
    void killWorker() {
        SystemProxy.exit(0);
    }

    /** For testing only. */
    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }
}
