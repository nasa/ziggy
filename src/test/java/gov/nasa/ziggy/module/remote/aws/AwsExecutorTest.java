package gov.nasa.ziggy.module.remote.aws;

import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_GROUP;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Unit tests for {@link AwsExecutor} class.
 *
 * @author PT
 */
public class AwsExecutorTest {

    @Rule
    public ZiggyPropertyRule groupPropertyRule = new ZiggyPropertyRule(REMOTE_GROUP, "12345");

    // TODO Fix or delete AWS test
    // We ignore this test because the AwsExecutor is (probably) obsolete, since it was originally
    // written for a proof-of-concept activity and represents an approach to AWS remote execution
    // that we actually don't want to use anymore. When we write the new AwsExecutor, or revive the
    // current one, we'll either delete and replace this test, or un-ignore it and fix it.
    // @Test
    public void testGeneratePbsParameters() {
        AwsExecutor executor = new AwsExecutor(new PipelineTask());

        // Start with a job that needs minimal gigs per core -- the optimization should
        // get us the C5 architecture, with a memory and cores configuration near the middle
        // of what's available on that architecture
        PipelineDefinitionNodeExecutionResources executionParameters = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        executionParameters.setGigsPerSubtask(1.0);
        executionParameters.setSubtaskMaxWallTimeHours(4.5);
        executionParameters.setSubtaskTypicalWallTimeHours(0.5);
        executionParameters.setRemoteExecutionEnabled(true);
        executionParameters.setRemoteNodeArchitecture("");

        PbsParameters pbsParameters = executor.generatePbsParameters(executionParameters, 500);
        assertEquals(RemoteNodeDescriptor.C5, pbsParameters.getArchitecture());
        assertEquals(16, pbsParameters.getActiveCoresPerNode());
        assertEquals(16, pbsParameters.getMinCoresPerNode());
        assertEquals(64, pbsParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(4, pbsParameters.getRequestedNodeCount());
        assertEquals("cloud", pbsParameters.getQueueName());
        assertEquals("12345", pbsParameters.getRemoteGroup());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(22.176, pbsParameters.getEstimatedCost(), 1e-9);

        // Now try a job that requires an R5 node and can't use all of its cores due to
        // memory demands.
        executionParameters.setGigsPerSubtask(32.0);
        pbsParameters = executor.generatePbsParameters(executionParameters, 500);
        assertEquals(RemoteNodeDescriptor.R5, pbsParameters.getArchitecture());
        assertEquals(8, pbsParameters.getActiveCoresPerNode());
        assertEquals(16, pbsParameters.getMinCoresPerNode());
        assertEquals(256, pbsParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(7, pbsParameters.getRequestedNodeCount());
        assertEquals("cloud", pbsParameters.getQueueName());
        assertEquals("12345", pbsParameters.getRemoteGroup());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(227.808, pbsParameters.getEstimatedCost(), 1e-9);

        // Now a job that requires an R5 node and a number of cores that is greater than the
        // default (default currently set to max / 3)
        executionParameters.setGigsPerSubtask(384.0);
        pbsParameters = executor.generatePbsParameters(executionParameters, 500);
        assertEquals(RemoteNodeDescriptor.R5, pbsParameters.getArchitecture());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(24, pbsParameters.getMinCoresPerNode());
        assertEquals(384, pbsParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(56, pbsParameters.getRequestedNodeCount());
        assertEquals("cloud", pbsParameters.getQueueName());
        assertEquals("12345", pbsParameters.getRemoteGroup());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(2733.696, pbsParameters.getEstimatedCost(), 1e-9);

        // Now do a test in which the minimum number of cores per node is set by the user
        executionParameters.setMinCoresPerNode(36);
        pbsParameters = executor.generatePbsParameters(executionParameters, 500);
        assertEquals(RemoteNodeDescriptor.R5, pbsParameters.getArchitecture());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(36, pbsParameters.getMinCoresPerNode());
        assertEquals(384, pbsParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(56, pbsParameters.getRequestedNodeCount());
        assertEquals("cloud", pbsParameters.getQueueName());
        assertEquals("12345", pbsParameters.getRemoteGroup());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(4100.544, pbsParameters.getEstimatedCost(), 1e-9);

        // Now a test in which the minimum amount of RAM per node is set by the user
        executionParameters.setMinCoresPerNode(0);
        executionParameters.setMinGigsPerNode(400.0);
        pbsParameters = executor.generatePbsParameters(executionParameters, 500);
        assertEquals(RemoteNodeDescriptor.R5, pbsParameters.getArchitecture());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(24, pbsParameters.getMinCoresPerNode());
        assertEquals(400, pbsParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(56, pbsParameters.getRequestedNodeCount());
        assertEquals("cloud", pbsParameters.getQueueName());
        assertEquals("12345", pbsParameters.getRemoteGroup());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(2733.696, pbsParameters.getEstimatedCost(), 1e-9);

        // Now a test in which both the minimum RAM and minimum cores are set
        // by the user
        executionParameters.setMinCoresPerNode(36);
        pbsParameters = executor.generatePbsParameters(executionParameters, 500);
        assertEquals(RemoteNodeDescriptor.R5, pbsParameters.getArchitecture());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(36, pbsParameters.getMinCoresPerNode());
        assertEquals(400, pbsParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(56, pbsParameters.getRequestedNodeCount());
        assertEquals("cloud", pbsParameters.getQueueName());
        assertEquals("12345", pbsParameters.getRemoteGroup());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(4100.544, pbsParameters.getEstimatedCost(), 1e-9);

        // Now a test in which the user asks for too little RAM, and the PBS parameter
        // calculator increases it to be sufficent to run the job.
        executionParameters.setMinGigsPerNode(200.0);
        pbsParameters = executor.generatePbsParameters(executionParameters, 500);
        assertEquals(RemoteNodeDescriptor.R5, pbsParameters.getArchitecture());
        assertEquals(1, pbsParameters.getActiveCoresPerNode());
        assertEquals(36, pbsParameters.getMinCoresPerNode());
        assertEquals(384, pbsParameters.getMinGigsPerNode(), 1e-3);
        assertEquals(56, pbsParameters.getRequestedNodeCount());
        assertEquals("cloud", pbsParameters.getQueueName());
        assertEquals("12345", pbsParameters.getRemoteGroup());
        assertEquals("4:30:00", pbsParameters.getRequestedWallTime());
        assertEquals(4100.544, pbsParameters.getEstimatedCost(), 1e-9);
    }
}
