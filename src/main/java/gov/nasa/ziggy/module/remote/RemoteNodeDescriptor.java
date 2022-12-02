package gov.nasa.ziggy.module.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Defines the architectures of remote nodes that can be used for Ziggy batch jobs.
 */
public enum RemoteNodeDescriptor {

    // Cost factors for the NAS systems are the SBU2 factors
    /**
     * The Sandy Bridge architecture. See <a href=
     * "https://www.nas.nasa.gov/hecc/support/kb/preparing-to-run-on-pleiades-sandy-bridge-nodes_322.html"
     * >Preparing to Run on Pleiades Sandy Bridge Nodes</a>.
     */
    SANDY_BRIDGE("san", 16, 16, 2, 0.47, SupportedRemoteClusters.NAS),

    /**
     * The Ivy Bridge architecture. See <a href=
     * "https://www.nas.nasa.gov/hecc/support/kb/preparing-to-run-on-pleiades-ivy-bridge-nodes_446.html"
     * >Preparing to Run on Pleiades Ivy Bridge Nodes</a>.
     */
    IVY_BRIDGE("ivy", 20, 20, 3.2, 0.66, SupportedRemoteClusters.NAS),

    /**
     * The Broadwell architecture. See <a href=
     * "https://www.nas.nasa.gov/hecc/support/kb/preparing-to-run-on-pleiades-broadwell-nodes_530.html"
     * >Preparing to Run on Pleiades Broadwell nodes</a>.
     */
    BROADWELL("bro", 28, 28, 4.57, 1.00, SupportedRemoteClusters.NAS),

    /**
     * The Haswell architecture. See <a href=
     * "https://www.nas.nasa.gov/hecc/support/kb/preparing-to-run-on-pleiades-haswell-nodes_491.html"
     * >Preparing to Run on Pleiades Haswell Nodes</a>.
     */
    HASWELL("has", 24, 24, 5.33, 0.80, SupportedRemoteClusters.NAS),

    /**
     * The Skylake architecture. See <a href=
     * "https://www.nas.nasa.gov/hecc/support/kb/preparing-to-run-on-electra-skylake-nodes_551.html"
     * >Preparing to Run on Electra Skylake nodes</a>.
     */
    SKYLAKE("sky_ele", 40, 40, 4.8, 1.59, SupportedRemoteClusters.NAS),

    CASCADE_LAKE("cas_ait", 40, 40, 4.0, 1.64, SupportedRemoteClusters.NAS),

    ROME("rom_ait", 128, 128, 4.0, 4.06, SupportedRemoteClusters.NAS),

    // Cost factors for AWS architectures is based on the actual cost in $/hour for the
    // least expensive node in each family (i.e., the one with the number of cores shown
    // in the min cores for that architecture), EBS only storage, and up to 10 gigabit
    // network bandwidth
    /**
     * AWS C5 architecture. This is the one with the smallest ratio of RAM per core.
     */
    C5("c5", 4, 48, 4, 0.308, SupportedRemoteClusters.AWS),

    /**
     * AWS M5 architecture.
     */
    M5("m5", 2, 48, 8, 0.344, SupportedRemoteClusters.AWS),

    /**
     * AWS R5 architecture. This is the one with the largest ratio of RAM per core.
     */
    R5("r5", 1, 48, 16, 0.452, SupportedRemoteClusters.AWS);

    private String nodeName; // arch types supported by PBS: san, wes, etc.
    private int minCores; // minimum number of cores available per node
    private int maxCores; // maximum number of cores available per node
    private double gigsPerCore; // physical memory per core, in GB
    private double costFactor; // relative costs for each node in the family
    private SupportedRemoteClusters remoteCluster; // which cluster has the architecture

    /**
     * Creates a new instance.
     */
    RemoteNodeDescriptor(String nodeName, int minCores, int maxCores, double gigsPerCore,
        double costFactor, SupportedRemoteClusters cluster) {
        this.nodeName = nodeName;
        this.minCores = minCores;
        this.maxCores = maxCores;
        this.gigsPerCore = gigsPerCore;
        this.costFactor = costFactor;
        remoteCluster = cluster;
    }

    /**
     * Gets the node architecture name.
     *
     * @return the architecture name
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Gets the minimum number of cores per node for this architecture.
     *
     * @return the number of cores
     */
    public int getMinCores() {
        return minCores;
    }

    /**
     * Gets the maximum number of cores per node for this architecture.
     *
     * @return the number of cores
     */
    public int getMaxCores() {
        return maxCores;
    }

    /**
     * Gets the maximum number of GB of RAM. This manages cases in which the product of gigs per
     * core * number of cores is something like 127.99 instead of 128. It makes use of our knowledge
     * that (a) systems have RAM provided in an integer # of GB, and (b) the gigs per core in the
     * remote node descriptors rounds down, such that it might produce a slightly sub-integer value
     * when multiplied by the number of cores.
     */
    public int getMaxGigs() {
        return (int) Math.round(gigsPerCore * maxCores);
    }

    public double getGigsPerCore() {
        return gigsPerCore;
    }

    public double getCostFactor() {
        return costFactor;
    }

    public SupportedRemoteClusters getRemoteCluster() {
        return remoteCluster;
    }

    /**
     * Selects an architecture based on the expected memory needs of each subtask. The returned
     * architecture will be the one which has the smallest amount of RAM per core that is greater
     * than or equal to the amount needed for each subtask. If there are no architectures that have
     * the necessary amount of RAM, the one with the largest ratio of RAM to cores will be returned.
     */
    public static RemoteNodeDescriptor selectArchitecture(SupportedRemoteClusters cluster,
        double subtaskGigsPerCore) {

        // default to the largest value
        RemoteNodeDescriptor selectedArchitecture = getArchWithMaxGigsPerCore(cluster);

        // loop over descriptors
        for (RemoteNodeDescriptor descriptor : RemoteNodeDescriptor.values()) {
            if (descriptor.getRemoteCluster().equals(cluster)
                && descriptor.getGigsPerCore() >= subtaskGigsPerCore
                && descriptor.getGigsPerCore() < selectedArchitecture.getGigsPerCore()) {
                selectedArchitecture = descriptor;
            }
        }

        return selectedArchitecture;
    }

    /**
     * Returns the descriptors for a given cluster, sorted from least to most expensive.
     */
    public static List<RemoteNodeDescriptor> descriptorsSortedByCost(
        SupportedRemoteClusters cluster) {
        List<RemoteNodeDescriptor> unsortedDescriptors = new ArrayList<>();
        for (RemoteNodeDescriptor descriptor : RemoteNodeDescriptor.values()) {
            if (descriptor.getRemoteCluster().equals(cluster)) {
                unsortedDescriptors.add(descriptor);
            }
        }
        List<RemoteNodeDescriptor> sortedDescriptors = unsortedDescriptors.stream()
            .sorted(RemoteNodeDescriptor::compareByCost)
            .collect(Collectors.toList());
        return sortedDescriptors;
    }

    /**
     * Returns the descriptors for a given cluster, sorted from the ones with most cores per node to
     * the ones with fewest cores per node.
     */
    public static List<RemoteNodeDescriptor> descriptorsSortedByCores(
        SupportedRemoteClusters cluster) {
        List<RemoteNodeDescriptor> unsortedDescriptors = new ArrayList<>();
        for (RemoteNodeDescriptor descriptor : RemoteNodeDescriptor.values()) {
            if (descriptor.getRemoteCluster().equals(cluster)) {
                unsortedDescriptors.add(descriptor);
            }
        }
        List<RemoteNodeDescriptor> sortedDescriptors = unsortedDescriptors.stream()
            .sorted(RemoteNodeDescriptor::compareByCores)
            .collect(Collectors.toList());
        return sortedDescriptors;
    }

    private static int compareByCost(RemoteNodeDescriptor d1, RemoteNodeDescriptor d2) {
        return (int) Math.signum(d1.getCostFactor() - d2.getCostFactor());
    }

    private static int compareByCores(RemoteNodeDescriptor d1, RemoteNodeDescriptor d2) {
        return d2.getMaxCores() - d1.getMaxCores();
    }

    /**
     * Returns the descriptors for a given cluster, sorted from least to greatest RAM per core. For
     * architectures in which the RAM per core is the same, the descriptors will be sorted by their
     * cost factors.
     */
    public static List<RemoteNodeDescriptor> descriptorsSortedByRamThenCost(
        SupportedRemoteClusters cluster) {
        List<RemoteNodeDescriptor> unsortedDescriptors = new ArrayList<>();
        for (RemoteNodeDescriptor descriptor : RemoteNodeDescriptor.values()) {
            if (descriptor.getRemoteCluster().equals(cluster)) {
                unsortedDescriptors.add(descriptor);
            }
        }
        List<RemoteNodeDescriptor> sortedDescriptors = unsortedDescriptors.stream()
            .sorted(RemoteNodeDescriptor::compareByRamThenCost)
            .collect(Collectors.toList());
        return sortedDescriptors;
    }

    private static int compareByRamThenCost(RemoteNodeDescriptor d1, RemoteNodeDescriptor d2) {
        int ramComparison = (int) Math.signum(d1.gigsPerCore - d2.gigsPerCore);
        if (ramComparison != 0) {
            return ramComparison;
        }
        return (int) Math.signum(d1.costFactor - d2.costFactor);
    }

    /**
     * Finds the {@link RemoteNodeDescriptor} for a given {@link RemoteCluster} that has the maximum
     * value of gigabytes per core.
     */
    private static RemoteNodeDescriptor getArchWithMaxGigsPerCore(SupportedRemoteClusters cluster) {
        RemoteNodeDescriptor maxGigsArch = null;
        for (RemoteNodeDescriptor descriptor : RemoteNodeDescriptor.values()) {
            if (descriptor.getRemoteCluster().equals(cluster) && (maxGigsArch == null
                || descriptor.getGigsPerCore() > maxGigsArch.getGigsPerCore())) {
                maxGigsArch = descriptor;
            }
        }
        return maxGigsArch;
    }

    public static RemoteNodeDescriptor fromName(String name) {
        RemoteNodeDescriptor descriptor = null;
        for (RemoteNodeDescriptor d : RemoteNodeDescriptor.values()) {
            if (d.getNodeName().contentEquals(name)) {
                descriptor = d;
            }
        }
        return descriptor;
    }

    /**
     * Returns a {@link ArrayList} of {@link RemoteNodeDescriptor} instances that have sufficient
     * RAM for a given task. The list ordering matches the ordering of the list given in the
     * arguments.
     *
     * @return
     */
    public static List<RemoteNodeDescriptor> nodesWithSufficientRam(
        List<RemoteNodeDescriptor> allNodes, double requiredRamGigs) {
        List<RemoteNodeDescriptor> adequateNodes = new ArrayList<>();
        for (int i = 0; i < allNodes.size(); i++) {
            RemoteNodeDescriptor d = allNodes.get(i);
            if (nodeHasSufficientRam(d, requiredRamGigs)) {
                adequateNodes.add(d);
            }
        }
        if (adequateNodes.isEmpty()) {
            throw new IllegalArgumentException("No architecture has " + requiredRamGigs + " GB");
        }
        return adequateNodes;
    }

    public static boolean nodeHasSufficientRam(RemoteNodeDescriptor descriptor,
        double requiredRamGigs) {
        return descriptor.getMaxGigs() >= requiredRamGigs;
    }

    public static String[] allNames() {
        List<RemoteNodeDescriptor> descriptors = descriptorsSortedByCost(
            SupportedRemoteClusters.remoteCluster());
        String[] allNames = new String[descriptors.size()];
        for (int i = 0; i < allNames.length; i++) {
            allNames[i] = descriptors.get(i).name();
        }
        return allNames;
    }
}
