package gov.nasa.ziggy.module.remote;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test class for {@link PbsParameters} class.
 *
 * @author PT
 */
public class PbsParametersTest {

    private RemoteParameters remoteParameters;
    private RemoteNodeDescriptor descriptor;
    private PbsParameters pbsParameters;

    @Before
    public void setup() {
        descriptor = RemoteNodeDescriptor.SANDY_BRIDGE;
        remoteParameters = new RemoteParameters();
        remoteParameters.setRemoteNodeArchitecture(descriptor.getNodeName());
        remoteParameters.setEnabled(true);
        remoteParameters.setGigsPerSubtask(6);
        remoteParameters.setSubtaskMaxWallTimeHours(4.5);
        remoteParameters.setSubtaskTypicalWallTimeHours(0.5);

        System.setProperty("pleiades.group", "12345");
    }

    @After
    public void teardown() {
        System.clearProperty("pleiades.group");
    }

    @Test
    public void testSimpleCase() {
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(12, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testNodeCountOverride() {
        remoteParameters.setMaxNodes("1");
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("long", pbsParameters.getQueueName());
        assertEquals(1, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(23.5, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("50:00:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testNodeCountOverrideSmallSubtaskCount() {
        remoteParameters.setMaxNodes("10");
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 5);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals(1, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(2.115, pbsParameters.getEstimatedCost(), 1e-3);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testSmallRamRequest() {
        remoteParameters.setGigsPerSubtask(0.5);
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(4, pbsParameters.getRequestedNodeCount());
        assertEquals(16, pbsParameters.getActiveCoresPerNode());
        assertEquals(8.46, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testSmallTask() {
        remoteParameters.setGigsPerSubtask(0.5);
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 10);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(1, pbsParameters.getRequestedNodeCount());
        assertEquals(16, pbsParameters.getActiveCoresPerNode());
        assertEquals(2.115, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testSubtaskPerCoreOverride() {
        remoteParameters.setSubtasksPerCore("12");
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(9, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("6:00:00", pbsParameters.getRequestedWallTime());

        remoteParameters.setSubtasksPerCore("6");
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(12, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testNodeSharingDisabled() {
        pbsParameters();
        remoteParameters.setSubtaskTypicalWallTimeHours(4.5);
        remoteParameters.setNodeSharing(false);
        remoteParameters.setWallTimeScaling(false);
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals(500, pbsParameters.getRequestedNodeCount());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(1057.5, pbsParameters.getEstimatedCost(), 1e-9);
    }

    @Test
    public void testNodeSharingDisabledTimeScalingEnabled() {
        pbsParameters();
        remoteParameters.setSubtaskTypicalWallTimeHours(4.5);
        remoteParameters.setNodeSharing(false);
        remoteParameters.setWallTimeScaling(true);
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals(500, pbsParameters.getRequestedNodeCount());
        assertEquals("0:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(117.5, pbsParameters.getEstimatedCost(), 1e-9);
    }

    @Test
    public void testQueueNameOverride() {
        remoteParameters.setQueueName("long");
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("long", pbsParameters.getQueueName());
        assertEquals(12, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testQueueNameForReservation() {
        remoteParameters.setQueueName("R14950266");
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 500);
        assertEquals("R14950266", pbsParameters.getQueueName());
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals(12, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test(expected = IllegalStateException.class)
    public void testBadQueueOverride() {
        remoteParameters.setQueueName("low");
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 500);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoQueuePossible() {
        remoteParameters.setMaxNodes("1");
        pbsParameters();
        pbsParameters.populateResourceParameters(remoteParameters, 5000);
    }

    @Test
    public void testArchitectureOverride() {
        pbsParameters();
        pbsParameters.populateArchitecture(remoteParameters, 500, SupportedRemoteClusters.NAS);
    }

    @Test(expected = IllegalStateException.class)
    public void testBadArchitectureOverride() {
        remoteParameters.setGigsPerSubtask(1000);
        pbsParameters();
        pbsParameters.populateArchitecture(remoteParameters, 500, SupportedRemoteClusters.NAS);
    }

    private void pbsParameters() {
        pbsParameters = remoteParameters.pbsParametersInstance();
        pbsParameters.setMinCoresPerNode(descriptor.getMinCores());
        pbsParameters
            .setMinGigsPerNode((int) (descriptor.getMinCores() * descriptor.getGigsPerCore()));
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
    }
}
