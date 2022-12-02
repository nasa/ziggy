package gov.nasa.ziggy.module.remote;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.remote.nas.NasQueueTimeMetrics;
import gov.nasa.ziggy.util.TimeFormatter;

/**
 * Provides data and code in support of selecting an optimal {@link RemoteNodeDescriptor}.
 *
 * @author PT
 */
public enum RemoteArchitectureOptimizer {

    // Minimizes the fraction of idled cores (they need to be idled due to the subtasks
    // requiring more RAM than the architecture's gigs per core provides). If the user
    // specifies one subtask at a time per node, this option is meaningless and
    // cost will be used instead.
    CORES {
        @Override
        public RemoteNodeDescriptor optimalArchitecture(RemoteParameters remoteParameters,
            int totalSubtasks, List<RemoteNodeDescriptor> acceptableDescriptors) {
            if (!remoteParameters.isNodeSharing()) {
                log.info(
                    "COST optimization not supported for one subtask per node, using COST instead");
                return COST.optimalArchitecture(remoteParameters, totalSubtasks,
                    acceptableDescriptors);
            }
            double coreRatio = 0;
            RemoteNodeDescriptor optimalDescriptor = null;
            for (RemoteNodeDescriptor descriptor : acceptableDescriptors) {
                double newCoreRatio = Math.min(1,
                    descriptor.getGigsPerCore() / remoteParameters.getGigsPerSubtask());
                if (newCoreRatio > coreRatio) {
                    coreRatio = newCoreRatio;
                    optimalDescriptor = descriptor;
                }
            }
            return optimalDescriptor;
        }
    },

    // Selects the architecture that has the smallest queue, as indicated by the number of
    // days it would take to run all queued jobs for a given architecture and NASA division.
    QUEUE_DEPTH {
        @Override
        public RemoteNodeDescriptor optimalArchitecture(RemoteParameters remoteParameters,
            int totalSubtasks, List<RemoteNodeDescriptor> acceptableDescriptors) {
            if (acceptableDescriptors.get(0)
                .getRemoteCluster()
                .equals(SupportedRemoteClusters.AWS)) {
                log.info("QUEUE_DEPTH optimization not supported for AWS, using COST instead");
                return COST.optimalArchitecture(remoteParameters, totalSubtasks,
                    acceptableDescriptors);
            }
            RemoteNodeDescriptor optimalDescriptor = null;
            double minQueueDepth = Double.MAX_VALUE;
            for (RemoteNodeDescriptor descriptor : acceptableDescriptors) {
                double queueDepth = NasQueueTimeMetrics.queueDepth(descriptor);
                if (queueDepth < minQueueDepth) {
                    minQueueDepth = queueDepth;
                    optimalDescriptor = descriptor;
                }
            }
            return optimalDescriptor;
        }
    },

    // Selects the architecture that minimizes the run time when including queue time,
    // using the "expansion" metric to estimate wait times in the queue.
    QUEUE_TIME {
        @Override
        public RemoteNodeDescriptor optimalArchitecture(RemoteParameters remoteParameters,
            int totalSubtasks, List<RemoteNodeDescriptor> acceptableDescriptors) {
            if (acceptableDescriptors.get(0)
                .getRemoteCluster()
                .equals(SupportedRemoteClusters.AWS)) {
                log.info("QUEUE_DEPTH optimization not supported for AWS, using COST instead");
                return COST.optimalArchitecture(remoteParameters, totalSubtasks,
                    acceptableDescriptors);
            }
            RemoteParameters duplicateParameters = new RemoteParameters(remoteParameters);
            RemoteNodeDescriptor optimalDescriptor = null;
            double minQueueTime = Double.MAX_VALUE;
            for (RemoteNodeDescriptor descriptor : acceptableDescriptors) {
                double queueTimeFactor = NasQueueTimeMetrics.queueTime(descriptor);
                duplicateParameters.setRemoteNodeArchitecture(descriptor.getNodeName());
                duplicateParameters.setMinCoresPerNode(Integer.toString(descriptor.getMaxCores()));
                duplicateParameters.setMinGigsPerNode(Integer.toString(descriptor.getMaxGigs()));
                PbsParameters pbsParameters = duplicateParameters.pbsParametersInstance();
                pbsParameters.populateResourceParameters(remoteParameters, totalSubtasks);
                double totalTime = queueTimeFactor * TimeFormatter
                    .timeStringHhMmSsToTimeInHours(pbsParameters.getRequestedWallTime());
                if (totalTime < minQueueTime) {
                    optimalDescriptor = descriptor;
                    minQueueTime = totalTime;
                }
            }
            return optimalDescriptor;
        }
    },

    // Selects the architecture that minimizes job cost.
    COST {
        @Override
        public RemoteNodeDescriptor optimalArchitecture(RemoteParameters remoteParameters,
            int totalSubtasks, List<RemoteNodeDescriptor> acceptableDescriptors) {

            RemoteParameters duplicateParameters = new RemoteParameters(remoteParameters);
            RemoteNodeDescriptor optimalArchitecture = null;
            double minimumCost = Double.MAX_VALUE;
            for (RemoteNodeDescriptor descriptor : acceptableDescriptors) {
                duplicateParameters.setRemoteNodeArchitecture(descriptor.getNodeName());
                duplicateParameters.setMinCoresPerNode(Integer.toString(descriptor.getMaxCores()));
                duplicateParameters.setMinGigsPerNode(Integer.toString(descriptor.getMaxGigs()));
                PbsParameters pbsParameters = duplicateParameters.pbsParametersInstance();
                pbsParameters.populateResourceParameters(remoteParameters, totalSubtasks);
                if (pbsParameters.getEstimatedCost() < minimumCost) {
                    minimumCost = pbsParameters.getEstimatedCost();
                    optimalArchitecture = descriptor;
                }
            }
            return optimalArchitecture;
        }
    };

    private static final Logger log = LoggerFactory.getLogger(RemoteArchitectureOptimizer.class);

    public abstract RemoteNodeDescriptor optimalArchitecture(RemoteParameters remoteParameters,
        int totalSubtasks, List<RemoteNodeDescriptor> acceptableDescriptors);

    /**
     * Finds an optimizer option for a given String. The matching between the String and the
     * optimizer is not case-sensitive and ignores underscores (i.e., "queuedepth" and "queue_Depth"
     * both match QUEUE_DEPTH).
     */
    public static RemoteArchitectureOptimizer fromName(String name) {
        RemoteArchitectureOptimizer option = null;
        for (RemoteArchitectureOptimizer o : RemoteArchitectureOptimizer.values()) {
            String cleanedUpOptimizerName = o.toString().replaceAll("_", "");
            String cleanedUpRequestName = name.toString().replaceAll("_", "");
            if (cleanedUpRequestName.toUpperCase().contentEquals(cleanedUpOptimizerName)) {
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

}
