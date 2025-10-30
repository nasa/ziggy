package gov.nasa.ziggy.pipeline.step.remote;

import java.util.List;
import java.util.stream.Collectors;

import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Provides data and code in support of selecting an optimal {@link Architecture}.
 *
 * @author PT
 */
public enum RemoteArchitectureOptimizer {

    /**
     * Minimizes the fraction of idled cores (they need to be idled due to the subtasks requiring
     * more RAM than the architecture's gigs per core provides). If the user specifies one subtask
     * at a time per node, this option is meaningless and cost will be used instead.
     */
    CORES {
        @Override
        public Architecture optimalNodeArchitecture(BatchParameters batchParameters,
            int totalSubtasks) {
            List<Architecture> architecturesWithSufficientRam = architecturesWithSufficientRam(
                batchParameters);
            double coreRatio = 0;

            // Loop over architectures and calculate the fraction of cores that are active.
            Architecture optimalArchitecture = null;
            for (Architecture architecture : architecturesWithSufficientRam) {
                double newCoreRatio = Math.min(1, architecture.gigsPerCore()
                    / batchParameters.executionResources().subtaskRamGigabytes());
                if (newCoreRatio > coreRatio) {
                    coreRatio = newCoreRatio;
                    optimalArchitecture = architecture;
                }
            }
            return optimalArchitecture;
        }
    },

    /**
     * Selects the architecture that has the smallest queue, as indicated by the number of days it
     * would take to run all queued jobs for a given architecture and NASA division.
     */
    QUEUE_DEPTH {
        @Override
        public Architecture optimalNodeArchitecture(BatchParameters batchParameters,
            int totalSubtasks) {
            List<Architecture> architecturesWithSufficientRam = architecturesWithSufficientRam(
                batchParameters);
            double minimumQueueDepthHours = Double.MAX_VALUE;
            Architecture optimalArchitecture = null;

            // Loop over architectures and find the one with the smallest queue.
            for (Architecture architecture : architecturesWithSufficientRam) {
                double queueDepth = batchParameters.executionResources()
                    .getRemoteEnvironment()
                    .queueTimeMetricsInstance()
                    .queueDepthHours(architecture);
                if (queueDepth < minimumQueueDepthHours) {
                    minimumQueueDepthHours = queueDepth;
                    optimalArchitecture = architecture;
                }
            }
            return optimalArchitecture;
        }
    },

    /**
     * Selects the architecture that minimizes the run time when including queue time, using the
     * "expansion" metric to estimate wait times in the queue.
     */
    QUEUE_TIME {
        @Override
        public Architecture optimalNodeArchitecture(BatchParameters batchParameters,
            int totalSubtasks) {
            List<Architecture> architecturesWithSufficientRam = architecturesWithSufficientRam(
                batchParameters);
            double minimumTimeIncludingQueueTime = Double.MAX_VALUE;
            Architecture optimalArchitecture = null;

            // Loop over architectures and determine the time including queue time.
            for (Architecture architecture : architecturesWithSufficientRam) {
                batchParameters.executionResources().setArchitecture(architecture);
                batchParameters.computeParameterValues(batchParameters.executionResources(),
                    totalSubtasks);
                double queueTimeFactor = batchParameters.executionResources()
                    .getRemoteEnvironment()
                    .queueTimeMetricsInstance()
                    .queueTimeFactor(architecture);
                double totalTime = queueTimeFactor * batchParameters.requestedWallTimeHours();
                if (totalTime < minimumTimeIncludingQueueTime) {
                    minimumTimeIncludingQueueTime = totalTime;
                    optimalArchitecture = architecture;
                }
            }
            return optimalArchitecture;
        }
    },

    /** Selects the architecture that minimizes job cost. */
    COST {
        @Override
        public Architecture optimalNodeArchitecture(BatchParameters batchParameters,
            int totalSubtasks) {
            List<Architecture> architecturesWithSufficientRam = architecturesWithSufficientRam(
                batchParameters);
            double cost = Double.MAX_VALUE;
            Architecture optimalArchitecture = null;

            // Loop over architectures and compute the cost.
            for (Architecture architecture : architecturesWithSufficientRam) {
                batchParameters.executionResources().setArchitecture(architecture);
                batchParameters.computeParameterValues(batchParameters.executionResources(),
                    totalSubtasks);
                if (batchParameters.estimatedCost() < cost) {
                    cost = batchParameters.estimatedCost();
                    optimalArchitecture = architecture;
                }
            }
            batchParameters.executionResources().setArchitecture(null);
            return optimalArchitecture;
        }
    };

    public abstract Architecture optimalNodeArchitecture(BatchParameters batchParameters,
        int totalSubtasks);

    /**
     * Filters a {@link List} of {@link Architecture} instances to eliminate the ones that have
     * inadequate RAM for the current task. The resulting {@link List} is sorted from lowest cost
     * per node to highest.
     */
    public static List<Architecture> architecturesWithSufficientRam(
        BatchParameters batchParameters) {
        double gigsPerSubtask = batchParameters.executionResources().subtaskRamGigabytes();
        List<Architecture> architecturesWithSufficientRam = batchParameters.executionResources()
            .getRemoteEnvironment()
            .getArchitectures()
            .stream()
            .filter(s -> s.getRamGigabytes() >= gigsPerSubtask)
            .sorted(RemoteArchitectureOptimizer::compareByCost)
            .collect(Collectors.toList());
        if (architecturesWithSufficientRam.isEmpty()) {
            throw new PipelineException(
                "All remote architectures have insufficient RAM for subtasks that require "
                    + gigsPerSubtask + " GB");
        }
        return architecturesWithSufficientRam;
    }

    private static int compareByCost(Architecture d1, Architecture d2) {
        return (int) Math.signum(d1.getCost() - d2.getCost());
    }

    /**
     * Finds an optimizer option for a given String. The matching between the String and the
     * optimizer is not case-sensitive and ignores underscores (i.e., "queuedepth" and "queue_Depth"
     * both match QUEUE_DEPTH).
     */
    public static RemoteArchitectureOptimizer fromName(String name) {
        RemoteArchitectureOptimizer option = null;
        for (RemoteArchitectureOptimizer o : RemoteArchitectureOptimizer.values()) {
            String cleanedUpOptimizerName = o.toString().replace("_", "");
            String cleanedUpRequestName = name.replace("_", "");
            if (cleanedUpRequestName.equalsIgnoreCase(cleanedUpOptimizerName)) {
                option = o;
            }
        }
        return option;
    }

    public static String options() {
        StringBuilder b = new StringBuilder();
        for (RemoteArchitectureOptimizer o : RemoteArchitectureOptimizer.values()) {
            b.append(o.toString());
            b.append(" ");
        }
        return b.toString();
    }

    @Override
    public String toString() {
        return ZiggyStringUtils.constantToSentenceWithSpaces(super.toString());
    }
}
