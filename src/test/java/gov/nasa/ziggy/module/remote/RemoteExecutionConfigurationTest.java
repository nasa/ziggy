package gov.nasa.ziggy.module.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;

/**
 * Test class for {@link PipelineDefinitionNodeExecutionResources} class.
 *
 * @author PT
 */
public class RemoteExecutionConfigurationTest {

    @Test
    public void testCopyConstructor() {
        PipelineDefinitionNodeExecutionResources r1 = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        r1.setRemoteExecutionEnabled(true);
        r1.setGigsPerSubtask(2.0);
        r1.setMaxNodes(3);
        r1.setMinCoresPerNode(4);
        r1.setMinGigsPerNode(5.0);
        r1.setOptimizer(RemoteArchitectureOptimizer.CORES);
        r1.setQueueName("low");
        r1.setRemoteNodeArchitecture("bro");
        r1.setSubtaskMaxWallTimeHours(9);
        r1.setSubtasksPerCore(1.5);
        r1.setSubtaskTypicalWallTimeHours(0.5);

        PipelineDefinitionNodeExecutionResources r2 = new PipelineDefinitionNodeExecutionResources(
            r1);
        assertTrue(EqualsBuilder.reflectionEquals(r2, r1));
    }

    @Test
    public void testPbsParametersInstance() {
        PipelineDefinitionNodeExecutionResources r1 = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        r1.setRemoteExecutionEnabled(true);
        r1.setGigsPerSubtask(2.0);
        r1.setMaxNodes(3);
        r1.setMinCoresPerNode(4);
        r1.setMinGigsPerNode(5.0);
        r1.setOptimizer(RemoteArchitectureOptimizer.CORES);
        r1.setQueueName("low");
        r1.setRemoteNodeArchitecture("bro");
        r1.setSubtaskMaxWallTimeHours(9);
        r1.setSubtasksPerCore(1.5);
        r1.setSubtaskTypicalWallTimeHours(0.5);

        PbsParameters p1 = r1.pbsParametersInstance();
        assertTrue(p1.isEnabled());
        assertEquals(RemoteNodeDescriptor.BROADWELL, p1.getArchitecture());
        assertEquals(2.0, p1.getGigsPerSubtask(), 1e-9);

        r1.setRemoteNodeArchitecture("");
        p1 = r1.pbsParametersInstance();
        assertTrue(p1.isEnabled());
        assertNull(p1.getArchitecture());
    }
}
