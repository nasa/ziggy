package gov.nasa.ziggy.pipeline.step.remote;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.step.remote.batch.PbsBatchParameters;

/**
 * Unit test class for {@link RemoteArchitectureOptimizer} class.
 *
 * @author PT
 */
public class RemoteArchitectureOptimizerTest {

    private NasQueueTimeMetrics timeMetrics;

    private RemoteEnvironment remoteEnvironment = Mockito.mock(RemoteEnvironment.class);
    private BatchParameters batchParameters = Mockito.spy(PbsBatchParameters.class);
    PipelineNodeExecutionResources executionResources;
    private Architecture bro;
    private Architecture cas;
    private Architecture has;
    private Architecture ivy;
    private Architecture rom;
    private Architecture san;
    private Architecture sky;

    @Rule
    public ZiggyPropertyRule groupPropertyRule = new ZiggyPropertyRule("ziggy.remote.hecc.group",
        "12345");

    @Before
    public void setup() {
        String pipelineName = "dummy";
        String pipelineStepName = "dummy";
        timeMetrics = Mockito.mock(NasQueueTimeMetrics.class);

        Map<String, Architecture> architectureByName = ArchitectureTestUtils.architectureByName();
        bro = architectureByName.get("bro");
        cas = architectureByName.get("cas_ait");
        has = architectureByName.get("has");
        ivy = architectureByName.get("ivy");
        rom = architectureByName.get("rom_ait");
        san = architectureByName.get("san");
        sky = architectureByName.get("sky_ele");

        when(timeMetrics.queueDepthHours(bro)).thenReturn(2.7);
        when(timeMetrics.queueDepthHours(cas)).thenReturn(11.3);
        when(timeMetrics.queueDepthHours(has)).thenReturn(2.7);
        when(timeMetrics.queueDepthHours(ivy)).thenReturn(2.2);
        when(timeMetrics.queueDepthHours(rom)).thenReturn(1.6);
        when(timeMetrics.queueDepthHours(san)).thenReturn(6.7);
        when(timeMetrics.queueDepthHours(sky)).thenReturn(1000.0);

        when(timeMetrics.queueTimeFactor(bro)).thenReturn(9.2);
        when(timeMetrics.queueTimeFactor(cas)).thenReturn(4.3);
        when(timeMetrics.queueTimeFactor(has)).thenReturn(6.5);
        when(timeMetrics.queueTimeFactor(ivy)).thenReturn(1.7);
        when(timeMetrics.queueTimeFactor(rom)).thenReturn(13.8);
        when(timeMetrics.queueTimeFactor(san)).thenReturn(15.2);
        when(timeMetrics.queueTimeFactor(sky)).thenReturn(1000.0);

        executionResources = new PipelineNodeExecutionResources(pipelineName, pipelineStepName);
        Mockito.when(batchParameters.executionResources()).thenReturn(executionResources);
        executionResources.setRemoteEnvironment(remoteEnvironment);
        Mockito.when(remoteEnvironment.getArchitectures())
            .thenReturn(List.of(bro, cas, has, ivy, rom, san, sky));
        Mockito.when(remoteEnvironment.queueTimeMetricsInstance()).thenReturn(timeMetrics);
        List<BatchQueue> batchQueues = BatchQueueTestUtils.batchQueues();
        Mockito.when(remoteEnvironment.getQueues()).thenReturn(batchQueues);
        Mockito.when(remoteEnvironment.getName()).thenReturn("hecc");
    }

    @Test
    public void testOptimizeForCores() {

        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.CORES;

        assertEquals("san", optimizer.optimalNodeArchitecture(batchParameters, 0).getName());

        executionResources.setGigsPerSubtask(3.0);
        assertEquals("ivy", optimizer.optimalNodeArchitecture(batchParameters, 0).getName());

        executionResources.setGigsPerSubtask(4.2);
        assertEquals("has", optimizer.optimalNodeArchitecture(batchParameters, 0).getName());

        executionResources.setGigsPerSubtask(10);
        assertEquals("has", optimizer.optimalNodeArchitecture(batchParameters, 0).getName());
    }

    @Test
    public void testOptimizeForCost() {
        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.COST;
        executionResources.setGigsPerSubtask(6.0);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        assertEquals("has", optimizer.optimalNodeArchitecture(batchParameters, 500).getName());
    }

    @Test
    public void testOptimizeForQueueDepth() {
        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.QUEUE_DEPTH;
        executionResources.setGigsPerSubtask(6.0);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        assertEquals("rom_ait", optimizer.optimalNodeArchitecture(batchParameters, 500).getName());
    }

    @Test
    public void testOptimizeForQueueTime() {
        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.QUEUE_TIME;
        executionResources.setGigsPerSubtask(6.0);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        assertEquals("ivy", optimizer.optimalNodeArchitecture(batchParameters, 500).getName());
    }
}
