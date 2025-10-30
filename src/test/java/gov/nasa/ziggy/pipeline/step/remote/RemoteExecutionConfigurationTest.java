package gov.nasa.ziggy.pipeline.step.remote;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;

/**
 * Test class for {@link PipelineNodeExecutionResources} class.
 *
 * @author PT
 */
public class RemoteExecutionConfigurationTest {

    @Test
    public void testCopyConstructor() {
        PipelineNodeExecutionResources r1 = new PipelineNodeExecutionResources("dummy", "dummy");
        r1.setRemoteExecutionEnabled(true);
        r1.setSubtaskRamGigabytes(2.0);
        r1.setMaxNodes(3);
        r1.setOptimizer(RemoteArchitectureOptimizer.CORES);
        r1.setReservedQueueName("low");
        r1.setRemoteNodeArchitecture("bro");
        r1.setSubtaskMaxWallTimeHours(9);
        r1.setSubtaskTypicalWallTimeHours(0.5);

        PipelineNodeExecutionResources r2 = new PipelineNodeExecutionResources(r1);
        assertTrue(EqualsBuilder.reflectionEquals(r2, r1));
    }
}
