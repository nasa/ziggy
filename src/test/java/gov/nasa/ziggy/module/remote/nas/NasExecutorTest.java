package gov.nasa.ziggy.module.remote.nas;

import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_GROUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Unit tests for {@link NasExecutor} class.
 *
 * @author PT
 */
public class NasExecutorTest {

    @Rule
    public ZiggyPropertyRule groupPropertyRule = new ZiggyPropertyRule(REMOTE_GROUP, "12345");

    @Test
    public void testGeneratePbsParameters() {

        NasExecutor executor = new NasExecutor(new PipelineTask());
        PipelineDefinitionNodeExecutionResources executionResources = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setGigsPerSubtask(6.0);
        executionResources.setSubtaskMaxWallTimeHours(4.5);
        executionResources.setSubtaskTypicalWallTimeHours(0.5);
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setRemoteNodeArchitecture("");

        PbsParameters pbsParameters = executor.generatePbsParameters(executionResources, 500);
        assertTrue(pbsParameters.isEnabled());
        assertEquals(RemoteNodeDescriptor.HASWELL, pbsParameters.getArchitecture());
        assertEquals(pbsParameters.getArchitecture().getMaxCores(),
            pbsParameters.getMinCoresPerNode());
        assertEquals(128, pbsParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(21, pbsParameters.getActiveCoresPerNode());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(3, pbsParameters.getRequestedNodeCount());
        assertEquals(10.8, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("12345", pbsParameters.getRemoteGroup());
    }
}
