package gov.nasa.ziggy.pipeline.step.remote.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.step.remote.Architecture;
import gov.nasa.ziggy.pipeline.step.remote.ArchitectureTestUtils;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueue;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueueTestUtils;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;

/** Unit tests for {@link PbsBatchParameters}. */
public class PbsBatchParametersTest {

    private static final String ARCHITECTURE = "ivy";

    private PipelineNodeExecutionResources executionResources;
    private RemoteEnvironment remoteEnvironment = Mockito.mock(RemoteEnvironment.class);
    private Map<String, Architecture> architectureByName;
    private Map<String, BatchQueue> batchQueueByName;
    private PbsBatchParameters pbsBatchParameters;

    @Rule
    public ZiggyPropertyRule groupPropertyRule = new ZiggyPropertyRule("ziggy.remote.hecc.group",
        "12345");

    @Before
    public void setUp() {
        pbsBatchParameters = new PbsBatchParameters();
        architectureByName = ArchitectureTestUtils.architectureByName();
        List<Architecture> architectures = ArchitectureTestUtils.architectures();
        batchQueueByName = BatchQueueTestUtils.batchQueueByName();
        executionResources = new PipelineNodeExecutionResources("dummy", "dummy");
        executionResources.setRemoteEnvironment(remoteEnvironment);
        Mockito.when(remoteEnvironment.getArchitectures()).thenReturn(architectures);
        List<BatchQueue> batchQueues = BatchQueueTestUtils.batchQueues();
        Mockito.when(remoteEnvironment.getQueues()).thenReturn(batchQueues);
        Mockito.when(remoteEnvironment.getName()).thenReturn("hecc");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setSubtaskRamGigabytes(6);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setArchitecture(architectureByName.get(ARCHITECTURE));
    }

    @Test
    public void testSimpleCase() {
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals("normal", pbsBatchParameters.getBatchQueue().getName());
        assertEquals(6, pbsBatchParameters.nodeCount());
        assertEquals(10, pbsBatchParameters.activeCores());
        assertEquals(17.82, pbsBatchParameters.estimatedCost(), 1e-6);
        assertEquals("4:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testNodeCountOverride() {
        executionResources.setMaxNodes(1);
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals(architectureByName.get(ARCHITECTURE), pbsBatchParameters.getArchitecture());
        assertEquals("long", pbsBatchParameters.getBatchQueue().getName());
        assertEquals(1, pbsBatchParameters.nodeCount());
        assertEquals(10, pbsBatchParameters.activeCores());
        assertEquals(16.5, pbsBatchParameters.estimatedCost(), 1e-6);
        assertEquals("25:00:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testNodeCountOverrideSmallSubtaskCount() {
        executionResources.setMaxNodes(10);
        pbsBatchParameters.computeParameterValues(executionResources, 5);
        assertEquals(architectureByName.get(ARCHITECTURE), pbsBatchParameters.getArchitecture());
        assertEquals(1, pbsBatchParameters.nodeCount());
        assertEquals(5, pbsBatchParameters.activeCores());
        assertEquals("normal", pbsBatchParameters.getBatchQueue().getName());
        assertEquals(2.970, pbsBatchParameters.estimatedCost(), 1e-3);
        assertEquals("4:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testSmallRamRequest() {
        executionResources.setSubtaskRamGigabytes(0.5);
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals(architectureByName.get(ARCHITECTURE), pbsBatchParameters.getArchitecture());
        assertEquals("normal", pbsBatchParameters.getBatchQueue().getName());
        assertEquals(3, pbsBatchParameters.nodeCount());
        assertEquals(20, pbsBatchParameters.activeCores());
        assertEquals(8.91, pbsBatchParameters.estimatedCost(), 1e-3);
        assertEquals("4:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testSmallTask() {
        executionResources.setSubtaskRamGigabytes(0.5);
        pbsBatchParameters.computeParameterValues(executionResources, 10);
        assertEquals(architectureByName.get(ARCHITECTURE), pbsBatchParameters.getArchitecture());
        assertEquals("normal", pbsBatchParameters.getBatchQueue().getName());
        assertEquals(1, pbsBatchParameters.nodeCount());
        assertEquals(10, pbsBatchParameters.activeCores());
        assertEquals(2.970, pbsBatchParameters.estimatedCost(), 1e-3);
        assertEquals("4:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testSubtaskPerCoreOverride() {
        executionResources.setSubtasksPerCore(12.0);
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals(architectureByName.get(ARCHITECTURE), pbsBatchParameters.getArchitecture());
        assertEquals("normal", pbsBatchParameters.getBatchQueue().getName());
        assertEquals(5, pbsBatchParameters.nodeCount());
        assertEquals(10, pbsBatchParameters.activeCores());
        assertEquals(19.80, pbsBatchParameters.estimatedCost(), 1e-3);
        assertEquals("6:00:00", pbsBatchParameters.getRequestedWallTime());

        executionResources.setSubtasksPerCore(6.0);
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals(architectureByName.get(ARCHITECTURE), pbsBatchParameters.getArchitecture());
        assertEquals("normal", pbsBatchParameters.getBatchQueue().getName());
        assertEquals(6, pbsBatchParameters.nodeCount());
        assertEquals(10, pbsBatchParameters.activeCores());
        assertEquals(17.82, pbsBatchParameters.estimatedCost(), 1e-3);
        assertEquals("4:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testNodeSharingDisabled() {
        executionResources.setSubtaskTypicalWallTimeHours(4.5);
        executionResources.setNodeSharing(false);
        executionResources.setWallTimeScaling(false);
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals(500, pbsBatchParameters.nodeCount());
        assertEquals("4:30:00", pbsBatchParameters.getRequestedWallTime());
        assertEquals(1, pbsBatchParameters.activeCores());
        assertEquals(1485.0, pbsBatchParameters.estimatedCost(), 1e-3);
    }

    @Test
    public void testNodeSharingDisabledTimeScalingEnabled() {
        executionResources.setSubtaskTypicalWallTimeHours(4.5);
        executionResources.setNodeSharing(false);
        executionResources.setWallTimeScaling(true);
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals(500, pbsBatchParameters.nodeCount());
        assertEquals("0:15:00", pbsBatchParameters.getRequestedWallTime());
        assertEquals(1, pbsBatchParameters.activeCores());
        assertEquals(82.5, pbsBatchParameters.estimatedCost(), 1e-3);
    }

    @Test
    public void testQueueOverride() {
        executionResources.setBatchQueue(batchQueueByName.get("long"));
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals(architectureByName.get(ARCHITECTURE), pbsBatchParameters.getArchitecture());
        assertEquals("long", pbsBatchParameters.getBatchQueue().getName());
        assertEquals(6, pbsBatchParameters.nodeCount());
        assertEquals(10, pbsBatchParameters.activeCores());
        assertEquals(17.82, pbsBatchParameters.estimatedCost(), 1e-3);
        assertEquals("4:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testQueueNameForReservation() {
        executionResources.setReservedQueueName("R14950266");
        executionResources.setBatchQueue(batchQueueByName.get("reserved"));
        pbsBatchParameters.computeParameterValues(executionResources, 500);
        assertEquals("R14950266", pbsBatchParameters.getBatchQueue().getName());
        assertTrue(pbsBatchParameters.getBatchQueue().isReserved());
        assertFalse(pbsBatchParameters.getBatchQueue().isAutoSelectable());
        assertEquals(architectureByName.get(ARCHITECTURE), pbsBatchParameters.getArchitecture());
        assertEquals(6, pbsBatchParameters.nodeCount());
        assertEquals(10, pbsBatchParameters.activeCores());
        assertEquals(17.82, pbsBatchParameters.estimatedCost(), 1e-3);
        assertEquals("4:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test(expected = IllegalStateException.class)
    public void testBadQueueOverride() {
        executionResources.setBatchQueue(batchQueueByName.get("low"));
        pbsBatchParameters.computeParameterValues(executionResources, 500);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoQueuePossible() {
        executionResources.setMaxNodes(1);
        pbsBatchParameters.computeParameterValues(executionResources, 5000);
    }

    @Test(expected = IllegalStateException.class)
    public void testBadArchitectureOverride() {
        executionResources.setSubtaskRamGigabytes(1000);
        pbsBatchParameters.computeParameterValues(executionResources, 500);
    }

    /**
     * Ensure that even when the required wall time is low, neither the DEBUG nor the DEVEL queue is
     * selected.
     */
    @Test
    public void testSelectQueueSmallJob() {
        executionResources.setMaxNodes(5);
        executionResources.setSubtaskMaxWallTimeHours(0.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setSubtaskRamGigabytes(2.0);
        pbsBatchParameters.computeParameterValues(executionResources, 50);
        assertEquals("low", pbsBatchParameters.getBatchQueue().getName());
        assertEquals("0:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testWallTimeFromTypicalWallTime() {
        executionResources.setSubtaskRamGigabytes(4);
        executionResources.setSubtaskMaxWallTimeHours(0.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setMaxNodes(3);
        pbsBatchParameters.computeParameterValues(executionResources, 100);
        assertEquals("1:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testWallTimeFromMaxTime() {
        executionResources.setSubtaskRamGigabytes(4);
        executionResources.setSubtaskMaxWallTimeHours(3.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setMaxNodes(3);
        pbsBatchParameters.computeParameterValues(executionResources, 100);
        assertEquals("3:30:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testScaledWallTimeFromTypical() {
        executionResources.setSubtaskRamGigabytes(4);
        executionResources.setSubtaskMaxWallTimeHours(0.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setMaxNodes(3);
        executionResources.setNodeSharing(false);
        executionResources.setWallTimeScaling(true);
        pbsBatchParameters.computeParameterValues(executionResources, 100);
        assertEquals("1:00:00", pbsBatchParameters.getRequestedWallTime());
    }

    @Test
    public void testScaledWallTimeFromMax() {
        executionResources.setSubtaskRamGigabytes(4);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.05);
        executionResources.setMaxNodes(3);
        executionResources.setNodeSharing(false);
        executionResources.setWallTimeScaling(true);
        pbsBatchParameters.computeParameterValues(executionResources, 100);
        assertEquals("0:15:00", pbsBatchParameters.getRequestedWallTime());
    }
}
