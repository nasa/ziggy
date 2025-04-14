package gov.nasa.ziggy.supervisor;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.step.FatalAlgorithmProcessingException;
import gov.nasa.ziggy.services.messages.PipelineInstanceStartedMessage;
import gov.nasa.ziggy.services.messages.StartPipelineRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Initiates processing of a pipeline instance in the supervisor, and if so requested initiates
 * additional instances once the current one has run to completion. This class is intended to be
 * constructed and run in its own thread so as not to waste the time and attention of the
 * supervisor.
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

    private Pipeline pipeline;
    private String instanceName;
    private PipelineNode startNode;
    private PipelineNode endNode;
    private int maxRepeats;
    private long repeatIntervalMillis;

    private int repeats = 0;
    private int statusChecks = 0;

    private long currentInstanceId;

    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineExecutor pipelineExecutor = new PipelineExecutor();
    private PipelineOperations pipelineOperations = new PipelineOperations();

    /**
     * Provided with package scope to facilitate testing. This is used when creating a Mockito spy
     * object, so the explicit no-arg constructor is required.
     */
    PipelineInstanceManager() {
    }

    public PipelineInstanceManager(StartPipelineRequest startPipelineRequest) {
        initialize(startPipelineRequest);
    }

    /**
     * Provided with package scope to facilitate testing.
     */
    void initialize(StartPipelineRequest startPipelineRequest) {
        instanceName = startPipelineRequest.getInstanceName();
        maxRepeats = startPipelineRequest.getMaxRepeats();
        if (maxRepeats < 0) {
            maxRepeats = Integer.MAX_VALUE;
        }
        String startNodeName = startPipelineRequest.getStartNodeName();
        String endNodeName = startPipelineRequest.getEndNodeName();
        log.info("start node is {}, end node is {}",
            StringUtils.isBlank(startNodeName) ? "not set" : startNodeName,
            StringUtils.isBlank(endNodeName) ? "not set" : endNodeName);
        repeatIntervalMillis = startPipelineRequest.getRepeatIntervalSeconds() * 1000;
        pipeline = pipelineOperations()
            .lockAndReturnLatestPipeline(startPipelineRequest.getPipelineName());
        startNode = pipelineOperations().pipelineNodeByName(pipeline,
            startPipelineRequest.getStartNodeName());
        endNode = pipelineOperations().pipelineNodeByName(pipeline,
            startPipelineRequest.getEndNodeName());
    }

    /**
     * Starts the given pipeline. If repetitions are requested, they are also managed by this
     * method.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void startPipeline() {

        while (repeats < maxRepeats) {

            // Append count/total if repeats in use. Use "-" to represent "forever" more briefly
            // than 2147483647.
            StringBuilder currentInstanceName = new StringBuilder(
                instanceName != null ? instanceName : "");
            if (maxRepeats > 1) {
                if (!currentInstanceName.isEmpty()) {
                    currentInstanceName.append(" ");
                }
                currentInstanceName.append(repeats + 1).append("/");
                if (maxRepeats == Integer.MAX_VALUE) {
                    currentInstanceName.append("-");
                } else {
                    currentInstanceName.append(maxRepeats);
                }
            }

            PipelineInstance pipelineInstance = pipelineExecutor().launch(pipeline,
                currentInstanceName.toString(), startNode, endNode, null);
            currentInstanceId = pipelineInstance.getId();

            ZiggyMessenger.publish(new PipelineInstanceStartedMessage());

            // If we're not on the last repeat, we need to wait.
            // While the counter is 0-based, messages to the user are 1-based.
            if (repeats++ < maxRepeats - 1) {
                try {
                    if (!waitAndCheckStatus()) {
                        throw new FatalAlgorithmProcessingException(
                            "Unable to start pipeline repeat " + repeats
                                + " due to errored status of pipeline repeat " + (repeats - 1));
                    }
                } catch (InterruptedException e) {
                    throw new FatalAlgorithmProcessingException(
                        "Unable to start pipeline repeat " + repeats
                            + " due to InterruptedException during pipeline repeat" + (repeats - 1),
                        e);
                }
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
            PipelineInstance instance = pipelineInstanceOperations()
                .pipelineInstance(currentInstanceId);
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
            if (loopStatus.keepLooping()) {
                log.info("Current pipeline instance {} still running, next instance start delayed",
                    currentInstanceId);
                Thread.sleep(checkAgainIntervalMillis);
            } else {
                log.info("Current pipeline instance {} {}, next instance {}", currentInstanceId,
                    loopStatus.instanceCompleted() ? "completed" : "errored",
                    loopStatus.instanceCompleted() ? "starting" : "canceled");
            }
        }
        return loopStatus.instanceCompleted();
    }

    // getters
    public Pipeline getPipeline() {
        return pipeline;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public PipelineNode getStartNode() {
        return startNode;
    }

    public PipelineNode getEndNode() {
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

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    PipelineExecutor pipelineExecutor() {
        return pipelineExecutor;
    }

    PipelineOperations pipelineOperations() {
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
    private static class LoopStatus {
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
