package gov.nasa.ziggy.module.remote;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * Describes the available remote queues and their properties.
 *
 * @author PT
 */
public enum RemoteQueueDescriptor implements Comparator<RemoteQueueDescriptor> {

    ANY,
    LOW("low", 4.0, SupportedRemoteClusters.NAS, true),
    NORMAL("normal", 12.0, SupportedRemoteClusters.NAS, true),
    LONG("long", 120.0, SupportedRemoteClusters.NAS, true),
    DEVEL("devel", 2.0, SupportedRemoteClusters.NAS, false),
    DEBUG("debug", 2.0, SupportedRemoteClusters.NAS, false),
    RESERVED("reserved", Double.MAX_VALUE, SupportedRemoteClusters.NAS, false),
    CLOUD("cloud", Double.MAX_VALUE, SupportedRemoteClusters.AWS, true),
    UNKNOWN("", Double.MAX_VALUE, SupportedRemoteClusters.NAS, false);

    private String queueName;
    private double maxWallTimeHours;
    private SupportedRemoteClusters remoteCluster;
    private boolean autoSelectable;

    RemoteQueueDescriptor(String queueName, double maxWallTimeHours,
        SupportedRemoteClusters supportedCluster, boolean autoSelectable) {
        this.queueName = queueName;
        this.maxWallTimeHours = maxWallTimeHours;
        remoteCluster = supportedCluster;
        this.autoSelectable = autoSelectable;
    }

    RemoteQueueDescriptor() {
    }

    public String getQueueName() {
        return queueName;
    }

    public double getMaxWallTimeHours() {
        return maxWallTimeHours;
    }

    public SupportedRemoteClusters getRemoteCluster() {
        return remoteCluster;
    }

    public boolean isAutoSelectable() {
        return autoSelectable;
    }

    @Override
    public int compare(RemoteQueueDescriptor o1, RemoteQueueDescriptor o2) {
        return (int) (o1.maxWallTimeHours - o2.maxWallTimeHours);
    }

    /**
     * Uses the standard {@link #compare(RemoteQueueDescriptor, RemoteQueueDescriptor)} method
     * enforced by the {@link Comparator} interface to construct a knock-off of the
     * {@link #compareTo(RemoteQueueDescriptor)} method. The reason for this is that enumerations
     * have a final {@link #compareTo(RemoteQueueDescriptor)}, so it cannot be overridden, and the
     * syntax of {@link #compare(RemoteQueueDescriptor, RemoteQueueDescriptor)} is stupid: it's an
     * instance method that requires two arguments (i.e., you could say a = b.compare(c, d), which
     * is stupid).
     */
    public int compare(RemoteQueueDescriptor other) {
        return compare(this, other);
    }

    public static RemoteQueueDescriptor fromQueueName(String queueName) {
        checkArgument(!StringUtils.isEmpty(queueName), "name argument cannot be null or empty");
        if (queueName.startsWith("R")) {
            return RESERVED;
        }
        RemoteQueueDescriptor descriptor = UNKNOWN;
        for (RemoteQueueDescriptor d : RemoteQueueDescriptor.values()) {
            if (d.getQueueName() != null && d.getQueueName().contentEquals(queueName)) {
                descriptor = d;
            }
        }
        return descriptor;
    }

    /**
     * Returns the remote queue descriptors for a given cluster sorted by their maximum allowed wall
     * time. Only the queues that are auto-selectable will be returned, thus the DEBUG, DEVEL,
     * UNKNOWN, and RESERVED descriptors will never be included in the returned List.
     */
    public static List<RemoteQueueDescriptor> descriptorsSortedByMaxTime(
        SupportedRemoteClusters remoteCluster) {
        List<RemoteQueueDescriptor> unsortedDescriptors = new ArrayList<>();
        for (RemoteQueueDescriptor descriptor : RemoteQueueDescriptor.values()) {
            if (descriptor.getRemoteCluster() != null
                && descriptor.getRemoteCluster().equals(remoteCluster)
                && descriptor.isAutoSelectable()) {
                unsortedDescriptors.add(descriptor);
            }
        }
        return unsortedDescriptors.stream().sorted().collect(Collectors.toList());
    }

    public static RemoteQueueDescriptor max(RemoteQueueDescriptor r1, RemoteQueueDescriptor r2) {
        return r1.compare(r2) > 0 ? r1 : r2;
    }

    public static RemoteQueueDescriptor[] allDescriptors() {
        List<RemoteQueueDescriptor> descriptors = descriptorsSortedByMaxTime(
            SupportedRemoteClusters.remoteCluster());
        if (SupportedRemoteClusters.remoteCluster().equals(SupportedRemoteClusters.NAS)) {
            descriptors.add(DEBUG);
            descriptors.add(DEVEL);
            descriptors.add(RESERVED);
        }
        RemoteQueueDescriptor[] allDescriptors = new RemoteQueueDescriptor[descriptors.size() + 1];
        allDescriptors[0] = ANY;
        for (int i = 1; i < allDescriptors.length; i++) {
            allDescriptors[i] = descriptors.get(i - 1);
        }
        return allDescriptors;
    }

    @Override
    public String toString() {
        return gov.nasa.ziggy.util.StringUtils.constantToSentenceWithSpaces(super.toString());
    }
}
