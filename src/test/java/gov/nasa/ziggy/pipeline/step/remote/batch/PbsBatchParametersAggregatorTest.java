package gov.nasa.ziggy.pipeline.step.remote.batch;

import static org.junit.Assert.assertEquals;

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

/** Unit tests for {@link PbsBatchParametersAggregator} class. */
public class PbsBatchParametersAggregatorTest {

    private PipelineNodeExecutionResources executionResources;
    private RemoteEnvironment remoteEnvironment = Mockito.mock(RemoteEnvironment.class);
    private Map<String, Architecture> architectureByName;
    private PbsBatchParameters pbsBatchParameters1;
    private PbsBatchParameters pbsBatchParameters2;

    @Rule
    public ZiggyPropertyRule groupPropertyRule = new ZiggyPropertyRule("ziggy.remote.hecc.group",
        "12345");

    @Before
    public void setUp() {
        pbsBatchParameters1 = new PbsBatchParameters();
        pbsBatchParameters2 = new PbsBatchParameters();
        architectureByName = ArchitectureTestUtils.architectureByName();
        executionResources = new PipelineNodeExecutionResources("dummy", "dummy");
        executionResources.setRemoteEnvironment(remoteEnvironment);
        List<Architecture> architectures = ArchitectureTestUtils.architectures();
        Mockito.when(remoteEnvironment.getArchitectures()).thenReturn(architectures);
        List<BatchQueue> batchQueues = BatchQueueTestUtils.batchQueues();
        Mockito.when(remoteEnvironment.getQueues()).thenReturn(batchQueues);
        Mockito.when(remoteEnvironment.getName()).thenReturn("hecc");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setSubtaskRamGigabytes(6);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setArchitecture(architectureByName.get("ivy"));
    }

    @Test
    public void testAggregatePbsParameters() {
        pbsBatchParameters1.computeParameterValues(executionResources, 500);
        executionResources.setMaxNodes(12);
        pbsBatchParameters2.computeParameterValues(executionResources, 5000);
        PbsBatchParameters aggregatedParameters = new PbsBatchParametersAggregator()
            .aggregate(List.of(pbsBatchParameters1, pbsBatchParameters2));
        assertEquals("ivy", aggregatedParameters.getArchitecture().getName());
        assertEquals("long", aggregatedParameters.getBatchQueue().getName());
        assertEquals(10, aggregatedParameters.activeCores());
        assertEquals("21:00:00", aggregatedParameters.getRequestedWallTime());
        assertEquals(18, aggregatedParameters.nodeCount());
        assertEquals(184.14, aggregatedParameters.estimatedCost(), 1e-2);
    }
}
