package gov.nasa.ziggy.module.remote.nas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteExecutionProperties;
import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Unit tests for {@link NasExecutor} class.
 *
 * @author PT
 */
public class NasExecutorTest {

    @Before
    public void setup() {
        System.setProperty(RemoteExecutionProperties.GROUP_PROPERTY, "12345");
    }

    @After
    public void teardown() {
        System.clearProperty(RemoteExecutionProperties.GROUP_PROPERTY);
    }

    @Test
    public void testGeneratePbsParameters() {

        NasExecutor executor = new NasExecutor(new PipelineTask());
        RemoteParameters remoteParameters = new RemoteParameters();
        remoteParameters.setGigsPerSubtask(6.0);
        remoteParameters.setSubtaskMaxWallTimeHours(4.5);
        remoteParameters.setSubtaskTypicalWallTimeHours(0.5);
        remoteParameters.setEnabled(true);
        remoteParameters.setRemoteNodeArchitecture("");

        PbsParameters pbsParameters = executor.generatePbsParameters(remoteParameters, 500);
        assertTrue(pbsParameters.isEnabled());
        assertEquals(RemoteNodeDescriptor.HASWELL, pbsParameters.getArchitecture());
        assertEquals(pbsParameters.getArchitecture().getMaxCores(),
            pbsParameters.getMinCoresPerNode());
        assertEquals(128, pbsParameters.getMinGigsPerNode());
        assertEquals(21, pbsParameters.getActiveCoresPerNode());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals("normal", pbsParameters.getQueueName());
        assertEquals(3, pbsParameters.getRequestedNodeCount());
        assertEquals(10.8, pbsParameters.getEstimatedCost(), 1e-9);
        assertEquals("12345", pbsParameters.getRemoteGroup());

    }

}
