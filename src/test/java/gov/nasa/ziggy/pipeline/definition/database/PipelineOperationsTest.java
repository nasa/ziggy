package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.hibernate.Hibernate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Unit tests for {@link PipelineOperations}.
 *
 * @author PT
 */
public class PipelineOperationsTest {

    private PipelineOperations pipelineOperations;
    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {

        // This needs to be in setup because it needs the database schema in
        // order to instantiate.
        pipelineOperations = new PipelineOperations();
    }

    @Test
    public void testLatestPipelineVersionForName() {
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        Pipeline initialPipeline = pipelineOperations.pipeline("pipeline1");
        assertEquals(1L, initialPipeline.getId().longValue());
        assertEquals(0, initialPipeline.getVersion());
        assertFalse(initialPipeline.isLocked());
        assertNull(initialPipeline.getDescription());
        Set<String> parameterSetNames = initialPipeline.getParameterSetNames();
        assertTrue(parameterSetNames.containsAll(Set.of("parameter1", "parameter2")));
        assertEquals(2, parameterSetNames.size());
        assertEquals(pipelineOperationsTestUtils.pipelineNode().getId().longValue(),
            pipelineOperations.rootNodes(initialPipeline).get(0).getId().longValue());
        assertEquals(1, pipelineOperations.rootNodes(initialPipeline).size());
        parameterSetNames = pipelineOperations.rootNodes(initialPipeline)
            .get(0)
            .getParameterSetNames();
        assertTrue(parameterSetNames.containsAll(Set.of("parameter3", "parameter4")));
        assertEquals(2, parameterSetNames.size());

        // Make a change and see if the retrieved version is correct.
        initialPipeline.setDescription("test");
        testOperations.merge(initialPipeline);
        Pipeline newUnlockedPipeline = pipelineOperations.pipeline("pipeline1");
        assertEquals(1L, newUnlockedPipeline.getId().longValue());
        assertEquals(0, newUnlockedPipeline.getVersion());
        assertFalse(newUnlockedPipeline.isLocked());
        assertEquals("test", newUnlockedPipeline.getDescription());
        parameterSetNames = pipelineOperations.parameterSetNames(newUnlockedPipeline);
        assertTrue(parameterSetNames.containsAll(Set.of("parameter1", "parameter2")));
        assertEquals(2, parameterSetNames.size());
        assertEquals(pipelineOperationsTestUtils.pipelineNode().getId().longValue(),
            pipelineOperations.rootNodes(newUnlockedPipeline).get(0).getId().longValue());
        assertEquals(1, pipelineOperations.rootNodes(initialPipeline).size());
        parameterSetNames = pipelineOperations.rootNodes(newUnlockedPipeline)
            .get(0)
            .getParameterSetNames();
        assertTrue(parameterSetNames.containsAll(Set.of("parameter3", "parameter4")));
        assertEquals(2, parameterSetNames.size());

        // Lock the current version.
        Pipeline lockedPipeline = testOperations.lockAndMerge(newUnlockedPipeline);

        assertTrue(lockedPipeline.isLocked());
        assertEquals(1L, newUnlockedPipeline.getId().longValue());
        assertEquals(0, newUnlockedPipeline.getVersion());
        assertEquals("test", newUnlockedPipeline.getDescription());

        // Make a change and see if the retrieved version is updated.
        lockedPipeline.setDescription("test test");
        testOperations.merge(lockedPipeline);
        Pipeline updatedPipeline = pipelineOperations.pipeline("pipeline1");

        assertFalse(updatedPipeline.isLocked());
        assertEquals(2L, updatedPipeline.getId().longValue());
        assertEquals(1, updatedPipeline.getVersion());
        assertEquals("test test", updatedPipeline.getDescription());

        // Note that the unlocked version has its own copies of the PipelineNodes
        // in rootNodes.
        assertEquals(pipelineOperationsTestUtils.pipelineNode().getId().longValue() + 1,
            pipelineOperations.rootNodes(updatedPipeline).get(0).getId().longValue());
        assertEquals(1, pipelineOperations.rootNodes(updatedPipeline).size());
        parameterSetNames = pipelineOperations.rootNodes(updatedPipeline)
            .get(0)
            .getParameterSetNames();
        assertTrue(parameterSetNames.containsAll(Set.of("parameter3", "parameter4")));
        assertEquals(2, parameterSetNames.size());

        parameterSetNames = pipelineOperations.parameterSetNames(updatedPipeline);

        assertTrue(parameterSetNames.containsAll(Set.of("parameter1", "parameter2")));
        assertEquals(2, parameterSetNames.size());
    }

    @Test
    public void testPipelines() {
        pipelineOperationsTestUtils.generateSingleNodePipeline(false);
        assertEquals(0, pipelineOperations.pipelines().size());
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        List<Pipeline> pipelines = pipelineOperations.pipelines();
        assertEquals(1, pipelines.size());
        assertEquals("pipeline1", pipelines.get(0).getName());
        List<PipelineNode> pipelineNodes = pipelines.get(0).getRootNodes();
        assertEquals(1, pipelineNodes.size());
        PipelineNode pipelineNode = pipelineNodes.get(0);
        assertTrue(pipelineNode.getNextNodes().isEmpty());
        assertFalse(Hibernate.isInitialized(pipelineNode.getInputDataFileTypes()));
        assertFalse(Hibernate.isInitialized(pipelineNode.getOutputDataFileTypes()));
        assertFalse(Hibernate.isInitialized(pipelineNode.getModelTypes()));
        assertTrue(Hibernate.isInitialized(pipelineNode.getParameterSetNames()));
        pipelines = pipelineOperations.pipelines(true);
        pipelineNode = pipelines.get(0).getRootNodes().get(0);
        assertTrue(Hibernate.isInitialized(pipelineNode.getInputDataFileTypes()));
        assertTrue(Hibernate.isInitialized(pipelineNode.getOutputDataFileTypes()));
        assertTrue(Hibernate.isInitialized(pipelineNode.getModelTypes()));
        assertTrue(Hibernate.isInitialized(pipelineNode.getParameterSetNames()));
    }

    @Test
    public void testLockAndReturnLatestPipeline() {
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        Pipeline pipeline = pipelineOperations.pipeline("pipeline1");
        pipeline = pipelineOperations.pipeline(pipeline.getName());
        assertFalse(pipeline.isLocked());
        Pipeline lockedPipeline = pipelineOperations.lockAndReturnLatestPipeline("pipeline1");
        lockedPipeline = pipelineOperations.pipeline(pipeline.getName());
        assertTrue(pipeline.totalEquals(lockedPipeline));
        assertTrue(lockedPipeline.isLocked());
    }

    @Test
    public void testFindPipelineNode() {
        pipelineOperationsTestUtils.setUpFourNodePipeline();
        PipelineNode foundNode = pipelineOperations
            .pipelineNodeByName(pipelineOperationsTestUtils.pipeline(), "step2");
        assertNotNull(foundNode);
        assertEquals(pipelineOperationsTestUtils.getPipelineNodes().get(1).getId(),
            foundNode.getId());
        assertNull(pipelineOperations.pipelineNodeByName(pipelineOperationsTestUtils.pipeline(),
            "name is not here"));
    }

    @Test
    public void testLock() {
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        Pipeline pipeline = pipelineOperations
            .pipeline(pipelineOperationsTestUtils.pipeline().getName());
        assertFalse(pipeline.isLocked());
        pipelineOperations.lock(pipelineOperationsTestUtils.pipeline());
        pipeline = pipelineOperations.pipeline(pipelineOperationsTestUtils.pipeline().getName());
        assertTrue(pipeline.isLocked());
    }

    private class TestOperations extends DatabaseOperations {

        public Pipeline merge(Pipeline pipeline) {
            return performTransaction(() -> new PipelineCrud().merge(pipeline));
        }

        public Pipeline lockAndMerge(Pipeline pipeline) {
            return performTransaction(() -> {
                pipeline.lock();
                return new PipelineCrud().merge(pipeline);
            });
        }
    }
}
