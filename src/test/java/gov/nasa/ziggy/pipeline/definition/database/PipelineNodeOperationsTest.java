package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/** Unit tests for {@link PipelineNodeOperations}. */
public class PipelineNodeOperationsTest {

    private PipelineNodeOperations pipelineNodeOperations = new PipelineNodeOperations();
    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Test
    public void testPipelineNodeExecutionResources() {
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        PipelineNodeExecutionResources resources = new PipelineNodeExecutionResources(
            pipelineOperationsTestUtils.pipeline().getName(),
            pipelineOperationsTestUtils.pipelineStep().getName());
        resources.setGigsPerSubtask(10);
        resources.setMaxAutoResubmits(10);
        testOperations.persistResources(resources);
        PipelineNodeExecutionResources databaseResources = pipelineNodeOperations
            .pipelineNodeExecutionResources(pipelineOperationsTestUtils.pipelineNode());
        assertEquals(10, databaseResources.getGigsPerSubtask(), 1e-9);
        assertEquals(10, databaseResources.getMaxAutoResubmits());
    }

    @Test
    public void testPipelineNodesForPipeline() {
        pipelineOperationsTestUtils.setUpFourNodePipeline();
        List<PipelineNode> pipelineNodes = pipelineNodeOperations
            .pipelineNodesForPipeline("pipeline1");
        assertEquals("step1", pipelineNodes.get(0).getPipelineStepName());
        assertEquals("step2", pipelineNodes.get(1).getPipelineStepName());
        assertEquals("step3", pipelineNodes.get(2).getPipelineStepName());
        assertEquals("step4", pipelineNodes.get(3).getPipelineStepName());
        assertEquals(4, pipelineNodes.size());
    }

    @Test
    public void testNextNodes() {
        pipelineOperationsTestUtils.setUpFourNodePipeline();
        List<PipelineNode> nextNodes = pipelineNodeOperations
            .nextNodes(pipelineOperationsTestUtils.getPipelineNodes().get(0));
        assertNotNull(nextNodes);
        assertEquals(pipelineOperationsTestUtils.getPipelineNodes().get(1).getId().longValue(),
            nextNodes.get(0).getId().longValue());
        assertEquals(1, nextNodes.size());
    }

    private class TestOperations extends DatabaseOperations {

        public void persistResources(PipelineNodeExecutionResources resources) {
            performTransaction(() -> new PipelineNodeCrud().persist(resources));
        }
    }
}
