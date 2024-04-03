package gov.nasa.ziggy.module.remote;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

/**
 * Unit test class for {@link RemoteNodeDescriptor}.
 *
 * @author PT
 */
public class RemoteNodeDescriptorTest {

    @Test
    public void testSelectArchitecture() {

        // low memory per process should always return the arch with the lowest memory per core
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE,
            RemoteNodeDescriptor.selectArchitecture(SupportedRemoteClusters.NAS, 1.5));
        assertEquals(RemoteNodeDescriptor.C5,
            RemoteNodeDescriptor.selectArchitecture(SupportedRemoteClusters.AWS, 1.5));

        // intermediate memory per process should return an appropriate arch
        assertEquals(RemoteNodeDescriptor.BROADWELL,
            RemoteNodeDescriptor.selectArchitecture(SupportedRemoteClusters.NAS, 4.5));
        assertEquals(RemoteNodeDescriptor.M5,
            RemoteNodeDescriptor.selectArchitecture(SupportedRemoteClusters.AWS, 5));

        // memory per process greater than the largest gigs per core should return the arch with the
        // max gigs per core
        assertEquals(RemoteNodeDescriptor.HASWELL,
            RemoteNodeDescriptor.selectArchitecture(SupportedRemoteClusters.NAS, 25));
        assertEquals(RemoteNodeDescriptor.R5,
            RemoteNodeDescriptor.selectArchitecture(SupportedRemoteClusters.AWS, 25));
    }

    @Test
    public void testDescriptorsSortedByCost() {

        List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
            .descriptorsSortedByCost(SupportedRemoteClusters.NAS);
        assertEquals(7, descriptors.size());
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, descriptors.get(0));
        assertEquals(RemoteNodeDescriptor.IVY_BRIDGE, descriptors.get(1));
        assertEquals(RemoteNodeDescriptor.HASWELL, descriptors.get(2));
        assertEquals(RemoteNodeDescriptor.BROADWELL, descriptors.get(3));
        assertEquals(RemoteNodeDescriptor.SKYLAKE, descriptors.get(4));
        assertEquals(RemoteNodeDescriptor.CASCADE_LAKE, descriptors.get(5));
        assertEquals(RemoteNodeDescriptor.ROME, descriptors.get(6));

        descriptors = RemoteNodeDescriptor.descriptorsSortedByCost(SupportedRemoteClusters.AWS);
        assertEquals(3, descriptors.size());
        assertEquals(RemoteNodeDescriptor.C5, descriptors.get(0));
        assertEquals(RemoteNodeDescriptor.M5, descriptors.get(1));
        assertEquals(RemoteNodeDescriptor.R5, descriptors.get(2));
    }

    @Test
    public void testDescriptorsSortedByRamThenCost() {

        List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
            .descriptorsSortedByRamThenCost(SupportedRemoteClusters.NAS);
        assertEquals(7, descriptors.size());
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, descriptors.get(0));
        assertEquals(RemoteNodeDescriptor.IVY_BRIDGE, descriptors.get(1));
        assertEquals(RemoteNodeDescriptor.ROME, descriptors.get(2));
        assertEquals(RemoteNodeDescriptor.BROADWELL, descriptors.get(3));
        assertEquals(RemoteNodeDescriptor.SKYLAKE, descriptors.get(4));
        assertEquals(RemoteNodeDescriptor.CASCADE_LAKE, descriptors.get(5));
        assertEquals(RemoteNodeDescriptor.HASWELL, descriptors.get(6));

        descriptors = RemoteNodeDescriptor
            .descriptorsSortedByRamThenCost(SupportedRemoteClusters.AWS);
        assertEquals(3, descriptors.size());
        assertEquals(RemoteNodeDescriptor.C5, descriptors.get(0));
        assertEquals(RemoteNodeDescriptor.M5, descriptors.get(1));
        assertEquals(RemoteNodeDescriptor.R5, descriptors.get(2));
    }

    @Test
    public void testNodesWithSufficientRam() {
        List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
            .descriptorsSortedByRamThenCost(SupportedRemoteClusters.NAS);
        List<RemoteNodeDescriptor> acceptableDescriptors = RemoteNodeDescriptor
            .nodesWithSufficientRam(descriptors, 70.0);
        assertEquals(5, acceptableDescriptors.size());
        assertEquals(RemoteNodeDescriptor.ROME, acceptableDescriptors.get(0));
        assertEquals(RemoteNodeDescriptor.BROADWELL, acceptableDescriptors.get(1));
        assertEquals(RemoteNodeDescriptor.SKYLAKE, acceptableDescriptors.get(2));
        assertEquals(RemoteNodeDescriptor.CASCADE_LAKE, acceptableDescriptors.get(3));
        assertEquals(RemoteNodeDescriptor.HASWELL, acceptableDescriptors.get(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoNodesHaveSufficientRam() {
        List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
            .descriptorsSortedByRamThenCost(SupportedRemoteClusters.NAS);
        RemoteNodeDescriptor.nodesWithSufficientRam(descriptors, 1000.0);
    }

    @Test
    public void testGetMaxGigs() {
        assertEquals(128, RemoteNodeDescriptor.HASWELL.getMaxGigs());
        assertEquals(128, RemoteNodeDescriptor.BROADWELL.getMaxGigs());
        assertEquals(32, RemoteNodeDescriptor.SANDY_BRIDGE.getMaxGigs());
        assertEquals(64, RemoteNodeDescriptor.IVY_BRIDGE.getMaxGigs());
        assertEquals(192, RemoteNodeDescriptor.SKYLAKE.getMaxGigs());
        assertEquals(192, RemoteNodeDescriptor.CASCADE_LAKE.getMaxGigs());
        assertEquals(512, RemoteNodeDescriptor.ROME.getMaxGigs());
        assertEquals(192, RemoteNodeDescriptor.C5.getMaxGigs());
        assertEquals(384, RemoteNodeDescriptor.M5.getMaxGigs());
        assertEquals(768, RemoteNodeDescriptor.R5.getMaxGigs());
    }
}
