package gov.nasa.ziggy.module.remote;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;

/**
 * Unit test class for {@link PbsParameters} class.
 *
 * @author PT
 */
public class PbsParametersTest {

    private PipelineDefinitionNodeExecutionResources executionResources;
    private RemoteNodeDescriptor descriptor;
    private PbsParameters pbsParameters;

    @Before
    public void setup() {
        descriptor = RemoteNodeDescriptor.SANDY_BRIDGE;
        executionResources = new PipelineDefinitionNodeExecutionResources("dummy", "dummy");
        executionResources.setRemoteNodeArchitecture(descriptor.getNodeName());
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setGigsPerSubtask(6);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
    }

    @Test
    public void testSimpleCase() {
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(12, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testNodeCountOverride() {
        executionResources.setMaxNodes(1);
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("long", pbsParameters.getQueueName());
        assertEquals(1, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(23.5, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("50:00:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testNodeCountOverrideSmallSubtaskCount() {
        executionResources.setMaxNodes(10);
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 5);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals(1, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(2.115, pbsParameters.getEstimatedCost(), 1e-3);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testSmallRamRequest() {
        executionResources.setGigsPerSubtask(0.5);
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(4, pbsParameters.getRequestedNodeCount());
        assertEquals(16, pbsParameters.getActiveCoresPerNode());
        assertEquals(8.46, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testSmallTask() {
        executionResources.setGigsPerSubtask(0.5);
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 10);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(1, pbsParameters.getRequestedNodeCount());
        assertEquals(10, pbsParameters.getActiveCoresPerNode());
        assertEquals(2.115, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testSubtaskPerCoreOverride() {
        executionResources.setSubtasksPerCore(12.0);
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(9, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("6:00:00", pbsParameters.getRequestedWallTime());

        executionResources.setSubtasksPerCore(6.0);
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 500);
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
        executionResources.setSubtaskTypicalWallTimeHours(4.5);
        executionResources.setNodeSharing(false);
        executionResources.setWallTimeScaling(false);
        pbsParameters.populateResourceParameters(executionResources, 500);
        assertEquals(500, pbsParameters.getRequestedNodeCount());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(1057.5, pbsParameters.getEstimatedCost(), 1e-9);
    }

    @Test
    public void testNodeSharingDisabledTimeScalingEnabled() {
        pbsParameters();
        executionResources.setSubtaskTypicalWallTimeHours(4.5);
        executionResources.setNodeSharing(false);
        executionResources.setWallTimeScaling(true);
        pbsParameters.populateResourceParameters(executionResources, 500);
        assertEquals(500, pbsParameters.getRequestedNodeCount());
        assertEquals("0:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(117.5, pbsParameters.getEstimatedCost(), 1e-9);
    }

    @Test
    public void testQueueNameOverride() {
        executionResources.setQueueName("long");
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 500);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals("long", pbsParameters.getQueueName());
        assertEquals(12, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testQueueNameForReservation() {
        executionResources.setQueueName("R14950266");
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 500);
        assertEquals("R14950266", pbsParameters.getQueueName());
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
        assertEquals(12, pbsParameters.getRequestedNodeCount());
        assertEquals(5, pbsParameters.getActiveCoresPerNode());
        assertEquals(25.38, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test(expected = IllegalStateException.class)
    public void testBadQueueOverride() {
        executionResources.setQueueName("low");
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 500);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoQueuePossible() {
        executionResources.setMaxNodes(1);
        pbsParameters();
        pbsParameters.populateResourceParameters(executionResources, 5000);
    }

    @Test
    public void testArchitectureOverride() {
        pbsParameters();
        pbsParameters.populateArchitecture(executionResources, 500, SupportedRemoteClusters.NAS);
    }

    @Test(expected = IllegalStateException.class)
    public void testBadArchitectureOverride() {
        executionResources.setGigsPerSubtask(1000);
        pbsParameters();
        pbsParameters.populateArchitecture(executionResources, 500, SupportedRemoteClusters.NAS);
    }

    /**
     * Ensure that even when the required wall time is low, neither the DEBUG nor the DEVEL queue is
     * selected.
     */
    @Test
    public void testSelectQueueSmallJob() {
        pbsParameters();
        executionResources.setMaxNodes(5);
        executionResources.setSubtaskMaxWallTimeHours(0.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setGigsPerSubtask(2.0);
        pbsParameters.populateResourceParameters(executionResources, 50);
        assertEquals(RemoteQueueDescriptor.LOW.getQueueName(), pbsParameters.getQueueName());
        assertEquals("0:30:00", pbsParameters.getRequestedWallTime());
    }

    @Test
    public void testAggregatePbsParameters() {
        pbsParameters();
        PbsParameters parameterSet1 = pbsParameters;
        parameterSet1.populateResourceParameters(executionResources, 500);
        pbsParameters();
        PbsParameters parameterSet2 = pbsParameters;
        parameterSet2.populateResourceParameters(executionResources, 500);
        parameterSet2.setActiveCoresPerNode(3);
        parameterSet2.setRequestedWallTime("20:00:00");
        parameterSet2.setQueueName("long");
        Set<PbsParameters> parameterSets = Set.of(parameterSet1, parameterSet2);
        PbsParameters aggregatedParameters = PbsParameters.aggregatePbsParameters(parameterSets);
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, aggregatedParameters.getArchitecture());
        assertEquals("long", aggregatedParameters.getQueueName());
        assertEquals(5, aggregatedParameters.getActiveCoresPerNode());
        assertEquals("20:00:00", aggregatedParameters.getRequestedWallTime());
        assertEquals(24, aggregatedParameters.getRequestedNodeCount());
        assertEquals(50.76, aggregatedParameters.getEstimatedCost(), 1e-9);
    }

    private void pbsParameters() {
        pbsParameters = executionResources.pbsParametersInstance();
        pbsParameters.setMinCoresPerNode(descriptor.getMinCores());
        pbsParameters
            .setMinGigsPerNode((int) (descriptor.getMinCores() * descriptor.getGigsPerCore()));
        assertEquals(RemoteNodeDescriptor.SANDY_BRIDGE, pbsParameters.getArchitecture());
    }
}
