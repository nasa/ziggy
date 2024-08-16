package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/** Unit tests for {@link PipelineDefinitionNodeOperations}. */
public class PipelineDefinitionNodeOperationsTest {

    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();
    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Test
    public void testPipelineDefinitionNodeExecutionResources() {
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        PipelineDefinitionNodeExecutionResources resources = new PipelineDefinitionNodeExecutionResources(
            pipelineOperationsTestUtils.pipelineDefinition().getName(),
            pipelineOperationsTestUtils.pipelineModuleDefinition().getName());
        resources.setGigsPerSubtask(10);
        resources.setMaxAutoResubmits(10);
        testOperations.persistResources(resources);
        PipelineDefinitionNodeExecutionResources databaseResources = pipelineDefinitionNodeOperations
            .pipelineDefinitionNodeExecutionResources(
                pipelineOperationsTestUtils.pipelineDefinitionNode());
        assertEquals(10, databaseResources.getGigsPerSubtask(), 1e-9);
        assertEquals(10, databaseResources.getMaxAutoResubmits());
    }

    @Test
    public void testPipelineDefinitionNodesForPipelineDefinition() {
        pipelineOperationsTestUtils.setUpFourModulePipeline();
        List<PipelineDefinitionNode> pipelineDefinitionNodes = pipelineDefinitionNodeOperations
            .pipelineDefinitionNodesForPipelineDefinition("pipeline1");
        assertEquals("module1", pipelineDefinitionNodes.get(0).getModuleName());
        assertEquals("module2", pipelineDefinitionNodes.get(1).getModuleName());
        assertEquals("module3", pipelineDefinitionNodes.get(2).getModuleName());
        assertEquals("module4", pipelineDefinitionNodes.get(3).getModuleName());
        assertEquals(4, pipelineDefinitionNodes.size());
    }

    @Test
    public void testNextNodes() {
        pipelineOperationsTestUtils.setUpFourModulePipeline();
        List<PipelineDefinitionNode> nextNodes = pipelineDefinitionNodeOperations
            .nextNodes(pipelineOperationsTestUtils.getPipelineDefinitionNodes().get(0));
        assertNotNull(nextNodes);
        assertEquals(
            pipelineOperationsTestUtils.getPipelineDefinitionNodes().get(1).getId().longValue(),
            nextNodes.get(0).getId().longValue());
        assertEquals(1, nextNodes.size());
    }

    private class TestOperations extends DatabaseOperations {

        public void persistResources(PipelineDefinitionNodeExecutionResources resources) {
            performTransaction(() -> new PipelineDefinitionNodeCrud().persist(resources));
        }
    }
}
