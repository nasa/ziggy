package gov.nasa.ziggy.module.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * Describes the available remote queues and their properties.
 *
 * @author PT
 */
public enum RemoteQueueDescriptor {

    LOW("low", 4.0, SupportedRemoteClusters.NAS),
    NORMAL("normal", 12.0, SupportedRemoteClusters.NAS),
    LONG("long", 120.0, SupportedRemoteClusters.NAS),
    CLOUD("cloud", Double.MAX_VALUE, SupportedRemoteClusters.AWS),
    UNKNOWN("", Double.MAX_VALUE, SupportedRemoteClusters.NAS);

    private String queueName;
    private double maxWallTimeHours;
    private SupportedRemoteClusters remoteCluster;

    RemoteQueueDescriptor(String queueName, double maxWallTimeHours,
        SupportedRemoteClusters supportedCluster) {
        this.queueName = queueName;
        this.maxWallTimeHours = maxWallTimeHours;
        remoteCluster = supportedCluster;
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

    public static RemoteQueueDescriptor fromName(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("name argument cannot be null or empty");
        }
        RemoteQueueDescriptor descriptor = UNKNOWN;
        for (RemoteQueueDescriptor d : RemoteQueueDescriptor.values()) {
            if (d.getQueueName().contentEquals(name)) {
                descriptor = d;
            }
        }
        return descriptor;
    }

    /**
     * Returns the remote queue descriptors for a given cluster sorted by their maximum allowed wall
     * time.
     */
    public static List<RemoteQueueDescriptor> descriptorsSortedByMaxTime(
        SupportedRemoteClusters remoteCluster) {
        List<RemoteQueueDescriptor> unsortedDescriptors = new ArrayList<>();
        for (RemoteQueueDescriptor descriptor : RemoteQueueDescriptor.values()) {
            if (descriptor.getRemoteCluster().equals(remoteCluster)
                && !descriptor.equals(UNKNOWN)) {
                unsortedDescriptors.add(descriptor);
            }
        }
        List<RemoteQueueDescriptor> sortedDescriptors = unsortedDescriptors.stream()
            .sorted(RemoteQueueDescriptor::compareByMaxTime)
            .collect(Collectors.toList());
        return sortedDescriptors;
    }

    private static int compareByMaxTime(RemoteQueueDescriptor d1, RemoteQueueDescriptor d2) {
        return (int) Math.signum(d1.getMaxWallTimeHours() - d2.getMaxWallTimeHours());
    }

    public static String[] allNames() {
        List<RemoteQueueDescriptor> descriptors = descriptorsSortedByMaxTime(
            SupportedRemoteClusters.remoteCluster());
        String[] allNames = new String[descriptors.size()];
        for (int i = 0; i < allNames.length; i++) {
            allNames[i] = descriptors.get(i).name();
        }
        return allNames;
    }

}
