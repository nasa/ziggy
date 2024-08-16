package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleExecutionResources;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/** Unit tests for {@link PipelineImportOperations}. */
public class PipelineImportOperationsTest {

    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private PipelineImportOperations pipelineImportOperations;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineImportOperations = new PipelineImportOperations();
    }

    @Test
    public void testPersistDefinitions() {
        pipelineOperationsTestUtils.generateSingleModulePipeline(false);

        PipelineDefinitionNodeExecutionResources nodeResources = new PipelineDefinitionNodeExecutionResources(
            pipelineOperationsTestUtils.pipelineDefinition().getName(),
            pipelineOperationsTestUtils.pipelineModuleDefinition().getName());
        nodeResources.setGigsPerSubtask(10);
        nodeResources.setMaxAutoResubmits(10);

        PipelineModuleExecutionResources moduleResources = new PipelineModuleExecutionResources();
        moduleResources.setPipelineModuleName("module1");
        moduleResources.setExeTimeoutSeconds(100);
        moduleResources.setMinMemoryMegabytes(10);

        Map<PipelineModuleDefinition, PipelineModuleExecutionResources> resourcesByPipelineModule = new HashMap<>();
        resourcesByPipelineModule.put(pipelineOperationsTestUtils.pipelineModuleDefinition(),
            moduleResources);
        Map<PipelineDefinition, Set<PipelineDefinitionNodeExecutionResources>> resourcesByNode = new HashMap<>();
        resourcesByNode.put(pipelineOperationsTestUtils.pipelineDefinition(),
            Set.of(nodeResources));

        pipelineImportOperations.persistDefinitions(resourcesByPipelineModule, resourcesByNode);
        List<PipelineDefinition> pipelineDefinitions = testOperations.pipelineDefinitions();
        assertEquals(1, pipelineDefinitions.size());
        PipelineDefinition pipelineDefinition = pipelineDefinitions.get(0);
        assertEquals("pipeline1", pipelineDefinition.getName());

        List<PipelineDefinitionNode> rootNodes = new PipelineDefinitionOperations()
            .rootNodes(pipelineDefinition);
        assertEquals(1, rootNodes.size());
        PipelineDefinitionNode rootNode = rootNodes.get(0);
        assertEquals("module1", rootNode.getModuleName());
        assertTrue(rootNode.getNextNodes().isEmpty());

        PipelineDefinitionNodeExecutionResources databaseNodeResources = new PipelineDefinitionNodeOperations()
            .pipelineDefinitionNodeExecutionResources(rootNode);
        assertEquals(10, databaseNodeResources.getGigsPerSubtask(), 1e-9);
        assertEquals(10, databaseNodeResources.getMaxAutoResubmits());

        List<PipelineModuleDefinition> pipelineModuleDefinitions = testOperations
            .pipelineModuleDefinitions();
        assertEquals(1, pipelineModuleDefinitions.size());
        assertEquals("module1", pipelineModuleDefinitions.get(0).getName());
        PipelineModuleExecutionResources databaseExecutionResources = new PipelineModuleDefinitionOperations()
            .pipelineModuleExecutionResources(pipelineModuleDefinitions.get(0));
        assertEquals(100, databaseExecutionResources.getExeTimeoutSeconds());
        assertEquals(10, databaseExecutionResources.getMinMemoryMegabytes());
    }

    private class TestOperations extends DatabaseOperations {

        public List<PipelineDefinition> pipelineDefinitions() {
            return performTransaction(() -> new PipelineDefinitionCrud().retrieveAll());
        }

        public List<PipelineModuleDefinition> pipelineModuleDefinitions() {
            return performTransaction(() -> new PipelineModuleDefinitionCrud().retrieveAll());
        }
    }
}
