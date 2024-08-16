package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Unit tests for {@link PipelineDefinitionOperations}.
 *
 * @author PT
 */
public class PipelineDefinitionOperationsTest {

    private PipelineDefinitionOperations pipelineDefinitionOperations;
    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {

        // This needs to be in setup because it needs the database schema in
        // order to instantiate.
        pipelineDefinitionOperations = new PipelineDefinitionOperations();
    }

    @Test
    public void testLatestPipelineDefinitionVersionForName() {
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        PipelineDefinition initialDefinition = pipelineDefinitionOperations
            .pipelineDefinition("pipeline1");
        assertEquals(1L, initialDefinition.getId().longValue());
        assertEquals(0, initialDefinition.getVersion());
        assertFalse(initialDefinition.isLocked());
        assertNull(initialDefinition.getDescription());
        Set<String> parameterSetNames = initialDefinition.getParameterSetNames();
        assertTrue(parameterSetNames.containsAll(Set.of("parameter1", "parameter2")));
        assertEquals(2, parameterSetNames.size());
        assertEquals(pipelineOperationsTestUtils.pipelineDefinitionNode().getId().longValue(),
            pipelineDefinitionOperations.rootNodes(initialDefinition).get(0).getId().longValue());
        assertEquals(1, pipelineDefinitionOperations.rootNodes(initialDefinition).size());
        parameterSetNames = pipelineDefinitionOperations.rootNodes(initialDefinition)
            .get(0)
            .getParameterSetNames();
        assertTrue(parameterSetNames.containsAll(Set.of("parameter3", "parameter4")));
        assertEquals(2, parameterSetNames.size());

        // Make a change and see if the retrieved version is correct.
        initialDefinition.setDescription("test");
        testOperations.merge(initialDefinition);
        PipelineDefinition newUnlockedDefinition = pipelineDefinitionOperations
            .pipelineDefinition("pipeline1");
        assertEquals(1L, newUnlockedDefinition.getId().longValue());
        assertEquals(0, newUnlockedDefinition.getVersion());
        assertFalse(newUnlockedDefinition.isLocked());
        assertEquals("test", newUnlockedDefinition.getDescription());
        parameterSetNames = pipelineDefinitionOperations.parameterSetNames(newUnlockedDefinition);
        assertTrue(parameterSetNames.containsAll(Set.of("parameter1", "parameter2")));
        assertEquals(2, parameterSetNames.size());
        assertEquals(pipelineOperationsTestUtils.pipelineDefinitionNode().getId().longValue(),
            pipelineDefinitionOperations.rootNodes(newUnlockedDefinition)
                .get(0)
                .getId()
                .longValue());
        assertEquals(1, pipelineDefinitionOperations.rootNodes(initialDefinition).size());
        parameterSetNames = pipelineDefinitionOperations.rootNodes(newUnlockedDefinition)
            .get(0)
            .getParameterSetNames();
        assertTrue(parameterSetNames.containsAll(Set.of("parameter3", "parameter4")));
        assertEquals(2, parameterSetNames.size());

        // Lock the current version.
        PipelineDefinition lockedDefinition = testOperations.lockAndMerge(newUnlockedDefinition);

        assertTrue(lockedDefinition.isLocked());
        assertEquals(1L, newUnlockedDefinition.getId().longValue());
        assertEquals(0, newUnlockedDefinition.getVersion());
        assertEquals("test", newUnlockedDefinition.getDescription());

        // Make a change and see if the retrieved version is updated.
        lockedDefinition.setDescription("test test");
        testOperations.merge(lockedDefinition);
        PipelineDefinition updatedVersionDefinition = pipelineDefinitionOperations
            .pipelineDefinition("pipeline1");

        assertFalse(updatedVersionDefinition.isLocked());
        assertEquals(2L, updatedVersionDefinition.getId().longValue());
        assertEquals(1, updatedVersionDefinition.getVersion());
        assertEquals("test test", updatedVersionDefinition.getDescription());

        // Note that the unlocked version has its own copies of the PipelineDefinitionNodes
        // in rootNodes.
        assertEquals(pipelineOperationsTestUtils.pipelineDefinitionNode().getId().longValue() + 1,
            pipelineDefinitionOperations.rootNodes(updatedVersionDefinition)
                .get(0)
                .getId()
                .longValue());
        assertEquals(1, pipelineDefinitionOperations.rootNodes(updatedVersionDefinition).size());
        parameterSetNames = pipelineDefinitionOperations.rootNodes(updatedVersionDefinition)
            .get(0)
            .getParameterSetNames();
        assertTrue(parameterSetNames.containsAll(Set.of("parameter3", "parameter4")));
        assertEquals(2, parameterSetNames.size());

        parameterSetNames = pipelineDefinitionOperations
            .parameterSetNames(updatedVersionDefinition);

        assertTrue(parameterSetNames.containsAll(Set.of("parameter1", "parameter2")));
        assertEquals(2, parameterSetNames.size());
    }

    @Test
    public void testPipelineDefinitions() {
        pipelineOperationsTestUtils.generateSingleModulePipeline(false);
        assertEquals(0, pipelineDefinitionOperations.pipelineDefinitions().size());
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        List<PipelineDefinition> pipelineDefinitions = pipelineDefinitionOperations
            .pipelineDefinitions();
        assertEquals(1, pipelineDefinitions.size());
        assertEquals("pipeline1", pipelineDefinitions.get(0).getName());
    }

    @Test
    public void testLockAndReturnLatestPipeline() {
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        PipelineDefinition pipelineDefinition = pipelineDefinitionOperations
            .pipelineDefinition("pipeline1");
        pipelineDefinition = pipelineDefinitionOperations
            .pipelineDefinition(pipelineDefinition.getName());
        assertFalse(pipelineDefinition.isLocked());
        PipelineDefinition lockedDefinition = pipelineDefinitionOperations
            .lockAndReturnLatestPipelineDefinition("pipeline1");
        lockedDefinition = pipelineDefinitionOperations
            .pipelineDefinition(pipelineDefinition.getName());
        assertTrue(pipelineDefinition.totalEquals(lockedDefinition));
        assertTrue(lockedDefinition.isLocked());
    }

    @Test
    public void testFindPipelineDefinitionNode() {
        pipelineOperationsTestUtils.setUpFourModulePipeline();
        PipelineDefinitionNode foundNode = pipelineDefinitionOperations
            .pipelineDefinitionNodeByName(pipelineOperationsTestUtils.pipelineDefinition(),
                "module2");
        assertNotNull(foundNode);
        assertEquals(pipelineOperationsTestUtils.getPipelineDefinitionNodes().get(1).getId(),
            foundNode.getId());
        assertNull(pipelineDefinitionOperations.pipelineDefinitionNodeByName(
            pipelineOperationsTestUtils.pipelineDefinition(), "name is not here"));
    }

    @Test
    public void testLock() {
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        PipelineDefinition pipelineDefinition = pipelineDefinitionOperations
            .pipelineDefinition(pipelineOperationsTestUtils.pipelineDefinition().getName());
        assertFalse(pipelineDefinition.isLocked());
        pipelineDefinitionOperations.lock(pipelineOperationsTestUtils.pipelineDefinition());
        pipelineDefinition = pipelineDefinitionOperations
            .pipelineDefinition(pipelineOperationsTestUtils.pipelineDefinition().getName());
        assertTrue(pipelineDefinition.isLocked());
    }

    private class TestOperations extends DatabaseOperations {

        public PipelineDefinition merge(PipelineDefinition pipelineDefinition) {
            return performTransaction(() -> new PipelineDefinitionCrud().merge(pipelineDefinition));
        }

        public PipelineDefinition lockAndMerge(PipelineDefinition pipelineDefinition) {
            return performTransaction(() -> {
                pipelineDefinition.lock();
                return new PipelineDefinitionCrud().merge(pipelineDefinition);
            });
        }
    }
}
