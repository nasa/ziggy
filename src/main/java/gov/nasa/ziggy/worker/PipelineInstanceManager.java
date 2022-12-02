package gov.nasa.ziggy.worker;

import static gov.nasa.ziggy.services.database.DatabaseTransactionFactory.performTransaction;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.ModuleFatalProcessingException;
import gov.nasa.ziggy.pipeline.InstanceAndTasks;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;

/**
 * Initiates processing of a pipeline instance in the worker, and if so requested initiates
 * additional instances once the current one has run to completion. This class is intended to be
 * constructed and run in its own thread so as not to waste the time and attention of the worker.
 *
 * @author PT
 */
public class PipelineInstanceManager {

    private static final Logger log = LoggerFactory.getLogger(PipelineInstanceManager.class);

    /**
     * Interval between keep-alive log messages
     */
    private static int KEEP_ALIVE_LOG_MSG_INTERVAL_MINUTES = 15;

    /**
     * Interval for checks after the nominal repeat interval. If an instance hasn't yet completed
     * when the repeat interval is done, the thread goes back to sleep for this period of time
     * before it checks again.
     */
    private static int CHECK_AGAIN_INTERVAL_MINUTES = 15;

    private PipelineDefinition pipeline;
    private String instanceName;
    private PipelineDefinitionNode startNode;
    private PipelineDefinitionNode endNode;
    private int maxRepeats;
    private long repeatIntervalMillis;

    private int repeats = 0;
    private int statusChecks = 0;

    private long currentInstanceId;

    private PipelineDefinitionCrud pipelineDefinitionCrud;
    private PipelineInstanceCrud pipelineInstanceCrud;
    private PipelineOperations pipelineOperations;

    /**
     * Provided with package scope to facilitate testing.
     */
    PipelineInstanceManager() {
    }

    /**
     * Provided with package scope to facilitate testing.
     */
    void initialize(WorkerFireTriggerRequest triggerRequest) {
        instanceName = triggerRequest.getInstanceName();
        maxRepeats = triggerRequest.getMaxRepeats();
        if (maxRepeats < 0) {
            maxRepeats = Integer.MAX_VALUE;
        }
        String startNodeName = triggerRequest.getStartNodeName();
        String endNodeName = triggerRequest.getEndNodeName();
        String startString, endString;
        if (startNodeName != null && !startNodeName.isEmpty()) {
            startString = "start node: " + startNodeName;
        } else {
            startString = "start node not set";
        }
        if (endNodeName != null && !endNodeName.isEmpty()) {
            endString = "end node: " + endNodeName;
        } else {
            endString = "end node not set";
        }
        log.info("PipelineInstanceManager.initialize: " + startString + ", " + endString);
        repeatIntervalMillis = triggerRequest.getRepeatIntervalSeconds() * 1000;
        performTransaction(() -> {
            pipeline = pipelineDefinitionCrud()
                .retrieveLatestVersionForName(triggerRequest.getPipelineName());
            startNode = pipeline.getNodeByName(triggerRequest.getStartNodeName());
            endNode = pipeline.getNodeByName(triggerRequest.getEndNodeName());
            Hibernate.initialize(pipeline.getRootNodes());

            /*
             * Lock the definition so it can't be changed once it is referred to by a
             * PipelineInstance This preserves the pipeline configuration and all parameter sets for
             * the data accountability record
             */
            pipeline.lock();
            return null;
        });
    }

    public PipelineInstanceManager(WorkerFireTriggerRequest triggerRequest) {
        initialize(triggerRequest);
    }

    /**
     * Fires the trigger for the given pipeline. If repetitions are requested, they are also managed
     * by this method.
     */
    public void fireTrigger() {
        // loop over repeats
        while (repeats < maxRepeats) {
            String currentInstanceName = instanceName;
            if (maxRepeats > 1) {
                currentInstanceName = currentInstanceName + ":-" + Integer.toString(repeats);
            }
            final String finalCurrentInstanceName = currentInstanceName;
            final List<PipelineTask> pipelineTasks = new ArrayList<>();
            // Create a database transaction and within it fire the trigger
            performTransaction(() -> {

                InstanceAndTasks triggerResults = pipelineOperations().fireTrigger(pipeline,
                    finalCurrentInstanceName, startNode, endNode);
                currentInstanceId = triggerResults.getPipelineInstance().getId();
                pipelineTasks.addAll(triggerResults.getPipelineTasks());
                pipelineInstanceCrud().evict(triggerResults.getPipelineInstance());
                return null;
            });

            // Send pipeline task messages to the worker
            for (PipelineTask task : pipelineTasks) {
                pipelineOperations().sendWorkerMessageForTask(task);
            }

            // if we're not on the last repeat, we need to start the waiting
            if (repeats < maxRepeats - 1) {
                try {
                    if (waitAndCheckStatus()) {
                        repeats++;
                    } else {
                        throw new ModuleFatalProcessingException(
                            "Unable to start pipeline repeat " + (repeats + 1)
                                + " due to errored status of pipeline repeat " + repeats);
                    }
                } catch (InterruptedException e) {
                    throw new ModuleFatalProcessingException(
                        "Unable to start pipeline repeat " + (repeats + 1)
                            + " due to InterruptedException during pipeline repeat" + repeats,
                        e);
                }
                // If we're on the last repeat, we can exit the loop by incrementing repeats.
            } else {
                repeats++;
            }
        }
    }

    /**
     * Waits until two conditions are true: first, that the user-imposed wait between iterations is
     * complete; second, that the pipeline instance status is COMPLETED or ERRORS_*. If at that time
     * the instance is COMPLETED, returns true, otherwise false, so the caller can decide whether to
     * run another instance of the pipeline. During this time keep-alive messages are written to the
     * worker log.
     *
     * @return successful completion status of the instance.
     * @throws InterruptedException if the worker is shut down while waiting
     */
    private boolean waitAndCheckStatus() throws InterruptedException {

        statusChecks++;

        // compute the number of sleep intervals between keep-alive messages while waiting for
        // the user-mandated interval to expire
        long fullKeepAliveIntervals = repeatIntervalMillis() / keepAliveLogMsgIntervalMillis();
        long millisecondsInLastKeepAliveInterval = repeatIntervalMillis()
            - fullKeepAliveIntervals * keepAliveLogMsgIntervalMillis();
        long keepAliveIntervals = fullKeepAliveIntervals + 1;

        // Sleep for as long as required, but print keep-alive messages to the log every
        // few minutes

        final LoopStatus loopStatus = new LoopStatus();
        for (long iKeepAlive = 0; iKeepAlive < keepAliveIntervals; iKeepAlive++) {
            long keepAliveIntervalMillis;
            if (iKeepAlive < keepAliveIntervals - 1) {
                keepAliveIntervalMillis = keepAliveLogMsgIntervalMillis();
            } else {
                keepAliveIntervalMillis = millisecondsInLastKeepAliveInterval;
            }
            Thread.sleep(keepAliveIntervalMillis);
            log.info("Waiting for user-specified interval between pipeline instances");
        }

        final long checkAgainIntervalMillis = checkAgainIntervalMillis();
        while (loopStatus.keepLooping()) {
            performTransaction(() -> {
                PipelineInstance instance = pipelineInstanceCrud().retrieve(currentInstanceId);
                pipelineInstanceCrud().evict(instance);
                PipelineInstance.State state = instance.getState();
                switch (state) {
                    case COMPLETED:
                        loopStatus.setKeepLooping(false);
                        loopStatus.setInstanceCompleted(true);
                        break;
                    case ERRORS_RUNNING:
                    case ERRORS_STALLED:
                        loopStatus.setKeepLooping(false);
                        loopStatus.setInstanceCompleted(false);
                        break;
                    default:
                        loopStatus.setKeepLooping(true);
                        loopStatus.setInstanceCompleted(false);
                }
                return null;
            });
            if (loopStatus.keepLooping()) {
                log.info("Current pipeline instance " + currentInstanceId
                    + " still running, next instance start delayed");
                Thread.sleep(checkAgainIntervalMillis);
            } else {
                String loopMessage;
                if (loopStatus.instanceCompleted()) {
                    loopMessage = "Current pipeline instance " + currentInstanceId
                        + " completed, next instance starting";
                } else {
                    loopMessage = "Current pipeline instance " + currentInstanceId
                        + " errored, next instance canceled";
                }
                log.info(loopMessage);
            }
        }
        return loopStatus.instanceCompleted();
    }

    // getters
    public PipelineDefinition getPipeline() {
        return pipeline;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public PipelineDefinitionNode getStartNode() {
        return startNode;
    }

    public PipelineDefinitionNode getEndNode() {
        return endNode;
    }

    public int getMaxRepeats() {
        return maxRepeats;
    }

    public long getCurrentInstanceId() {
        return currentInstanceId;
    }

    public long getRepeatIntervalMillis() {
        return repeatIntervalMillis;
    }

    public int getRepeats() {
        return repeats;
    }

    public int getStatusChecks() {
        return statusChecks;
    }

    // The methods below are provided to support unit testing. Override them with methods
    // to provide mocked CRUD instances or keep-alive / check-again intervals shorter than
    // 15 minutes.
    PipelineDefinitionCrud pipelineDefinitionCrud() {
        if (pipelineDefinitionCrud == null) {
            pipelineDefinitionCrud = new PipelineDefinitionCrud();
        }
        return pipelineDefinitionCrud;
    }

    PipelineInstanceCrud pipelineInstanceCrud() {
        if (pipelineInstanceCrud == null) {
            pipelineInstanceCrud = new PipelineInstanceCrud();
        }
        return pipelineInstanceCrud;
    }

    PipelineOperations pipelineOperations() {
        if (pipelineOperations == null) {
            pipelineOperations = new PipelineOperations();
        }
        return pipelineOperations;
    }

    long keepAliveLogMsgIntervalMillis() {
        long minInterval = Math.min(1000 * 60 * KEEP_ALIVE_LOG_MSG_INTERVAL_MINUTES,
            repeatIntervalMillis);
        return Math.max(minInterval, 1000);
    }

    long checkAgainIntervalMillis() {
        long minInterval = Math.min(1000 * 60 * CHECK_AGAIN_INTERVAL_MINUTES, repeatIntervalMillis);
        return Math.max(minInterval, 1000);
    }

    long repeatIntervalMillis() {
        return repeatIntervalMillis;
    }

    /**
     * Class that carries status information in the wait loop. This class is needed so that a "final
     * or effectively final" but not immutable object can pass the "blood-brain barrier" into the
     * database transaction wrapper body.
     *
     * @author PT
     */
    private class LoopStatus {
        private boolean keepLooping = true;
        private boolean instanceCompleted = false;

        boolean keepLooping() {
            return keepLooping;
        }

        boolean instanceCompleted() {
            return instanceCompleted;
        }

        void setKeepLooping(boolean keepLooping) {
            this.keepLooping = keepLooping;
        }

        void setInstanceCompleted(boolean instanceCompleted) {
            this.instanceCompleted = instanceCompleted;
        }
    }
}
