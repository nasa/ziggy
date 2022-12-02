package gov.nasa.ziggy.module.remote;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.module.remote.nas.NasQueueTimeMetrics;

/**
 * Unit test class for {@link RemoteArchitectureOptimizer} class.
 *
 * @author PT
 */
public class RemoteArchitectureOptimizerTest {

    private NasQueueTimeMetrics timeMetrics;

    @Before
    public void setup() {
        timeMetrics = Mockito.mock(NasQueueTimeMetrics.class);
        when(timeMetrics.getQueueDepth(RemoteNodeDescriptor.BROADWELL)).thenReturn(2.7);
        when(timeMetrics.getQueueDepth(RemoteNodeDescriptor.CASCADE_LAKE)).thenReturn(11.3);
        when(timeMetrics.getQueueDepth(RemoteNodeDescriptor.HASWELL)).thenReturn(2.7);
        when(timeMetrics.getQueueDepth(RemoteNodeDescriptor.IVY_BRIDGE)).thenReturn(2.2);
        when(timeMetrics.getQueueDepth(RemoteNodeDescriptor.ROME)).thenReturn(1.6);
        when(timeMetrics.getQueueDepth(RemoteNodeDescriptor.SANDY_BRIDGE)).thenReturn(6.7);
        when(timeMetrics.getQueueDepth(RemoteNodeDescriptor.SKYLAKE)).thenReturn(1000.0);

        when(timeMetrics.getQueueTime(RemoteNodeDescriptor.BROADWELL)).thenReturn(9.2);
        when(timeMetrics.getQueueTime(RemoteNodeDescriptor.CASCADE_LAKE)).thenReturn(4.3);
        when(timeMetrics.getQueueTime(RemoteNodeDescriptor.HASWELL)).thenReturn(6.5);
        when(timeMetrics.getQueueTime(RemoteNodeDescriptor.IVY_BRIDGE)).thenReturn(1.7);
        when(timeMetrics.getQueueTime(RemoteNodeDescriptor.ROME)).thenReturn(13.8);
        when(timeMetrics.getQueueTime(RemoteNodeDescriptor.SANDY_BRIDGE)).thenReturn(15.2);
        when(timeMetrics.getQueueTime(RemoteNodeDescriptor.SKYLAKE)).thenReturn(1000.0);

        NasQueueTimeMetrics.setInstance(timeMetrics);
    }

    @Test
    public void testOptimizeForCores() {

        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.CORES;
        List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
            .descriptorsSortedByRamThenCost(SupportedRemoteClusters.NAS);
        RemoteParameters remoteParameters = new RemoteParameters();
        remoteParameters.setGigsPerSubtask(0.5);
        RemoteNodeDescriptor descriptor = optimizer.optimalArchitecture(remoteParameters, 0,
            RemoteNodeDescriptor.nodesWithSufficientRam(descriptors,
                remoteParameters.getGigsPerSubtask()));
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, descriptor);

        remoteParameters.setGigsPerSubtask(3.0);
        descriptor = optimizer.optimalArchitecture(remoteParameters, 0, RemoteNodeDescriptor
            .nodesWithSufficientRam(descriptors, remoteParameters.getGigsPerSubtask()));
        assertEquals(RemoteNodeDescriptor.IVY_BRIDGE, descriptor);

        remoteParameters.setGigsPerSubtask(4.2);
        descriptor = optimizer.optimalArchitecture(remoteParameters, 0, RemoteNodeDescriptor
            .nodesWithSufficientRam(descriptors, remoteParameters.getGigsPerSubtask()));
        assertEquals(RemoteNodeDescriptor.BROADWELL, descriptor);

        remoteParameters.setGigsPerSubtask(10);
        descriptor = optimizer.optimalArchitecture(remoteParameters, 0, RemoteNodeDescriptor
            .nodesWithSufficientRam(descriptors, remoteParameters.getGigsPerSubtask()));
        assertEquals(RemoteNodeDescriptor.HASWELL, descriptor);

    }

    @Test
    public void testOptimizeForCost() {
        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.COST;
        List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
            .descriptorsSortedByCost(SupportedRemoteClusters.NAS);
        RemoteParameters remoteParameters = new RemoteParameters();
        remoteParameters.setGigsPerSubtask(6.0);
        remoteParameters.setSubtaskMaxWallTimeHours(4.5);
        remoteParameters.setSubtaskTypicalWallTimeHours(0.5);
        RemoteNodeDescriptor descriptor = optimizer.optimalArchitecture(remoteParameters, 500,
            RemoteNodeDescriptor.nodesWithSufficientRam(descriptors,
                remoteParameters.getGigsPerSubtask()));
        assertEquals(RemoteNodeDescriptor.HASWELL, descriptor);

    }

    @Test
    public void testOptimizeForQueueDepth() {
        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.QUEUE_DEPTH;
        List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
            .descriptorsSortedByCost(SupportedRemoteClusters.NAS);
        RemoteParameters remoteParameters = new RemoteParameters();
        remoteParameters.setGigsPerSubtask(6.0);
        remoteParameters.setSubtaskMaxWallTimeHours(4.5);
        remoteParameters.setSubtaskTypicalWallTimeHours(0.5);
        RemoteNodeDescriptor descriptor = optimizer.optimalArchitecture(remoteParameters, 500,
            RemoteNodeDescriptor.nodesWithSufficientRam(descriptors,
                remoteParameters.getGigsPerSubtask()));
        assertEquals(RemoteNodeDescriptor.ROME, descriptor);
    }

    @Test
    public void testOptimizeForQueueTime() {
        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.QUEUE_TIME;
        List<RemoteNodeDescriptor> descriptors = RemoteNodeDescriptor
            .descriptorsSortedByCost(SupportedRemoteClusters.NAS);
        RemoteParameters remoteParameters = new RemoteParameters();
        remoteParameters.setGigsPerSubtask(6.0);
        remoteParameters.setSubtaskMaxWallTimeHours(4.5);
        remoteParameters.setSubtaskTypicalWallTimeHours(0.5);
        RemoteNodeDescriptor descriptor = optimizer.optimalArchitecture(remoteParameters, 500,
            RemoteNodeDescriptor.nodesWithSufficientRam(descriptors,
                remoteParameters.getGigsPerSubtask()));
        assertEquals(RemoteNodeDescriptor.IVY_BRIDGE, descriptor);
    }

}
