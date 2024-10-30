package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Hibernate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/** Unit test class for {@link PipelineInstanceOperations}. */
public class PipelineInstanceOperationsTest {

    private PipelineInstanceOperations pipelineInstanceOperations;

    private PipelineInstance pipelineInstance;
    private TestOperations testOperations = new TestOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineInstanceOperations = Mockito.spy(PipelineInstanceOperations.class);
    }

    private void setUpSingleModulePipeline() {
        PipelineOperationsTestUtils testUtils = new PipelineOperationsTestUtils();
        testUtils.setUpSingleModulePipeline();
        pipelineInstance = testUtils.pipelineInstance();
    }

    @Test
    public void testSetInstanceToErrorsStalledState() {
        setUpSingleModulePipeline();
        pipelineInstanceOperations.setInstanceToErrorsStalledState(pipelineInstance);
        PipelineInstance databaseInstance = testOperations.pipelineInstance(1L);
        assertEquals(PipelineInstance.State.ERRORS_STALLED, databaseInstance.getState());
    }

    @Test
    public void testRootNodes() {
        setUpSingleModulePipeline();
        List<PipelineInstanceNode> rootNodes = pipelineInstanceOperations
            .rootNodes(pipelineInstance);
        assertEquals(1L, rootNodes.get(0).getId().longValue());
        assertEquals(1, rootNodes.size());
    }

    @Test
    public void testAddRootNode() {
        setUpSingleModulePipeline();
        List<PipelineInstanceNode> rootNodes = pipelineInstanceOperations
            .rootNodes(pipelineInstance);
        assertEquals(1L, rootNodes.get(0).getId().longValue());
        assertEquals(1, rootNodes.size());
        List<PipelineInstanceNode> instanceNodes = testOperations
            .pipelineInstanceNodes(pipelineInstance);
        assertEquals(1L, instanceNodes.get(0).getId().longValue());
        assertEquals(1, instanceNodes.size());

        PipelineInstanceNode newNode = testOperations.merge(new PipelineInstanceNode());
        pipelineInstanceOperations.addRootNode(pipelineInstance, newNode);
        rootNodes = pipelineInstanceOperations.rootNodes(pipelineInstance);
        assertEquals(1L, rootNodes.get(0).getId().longValue());
        assertEquals(newNode.getId().longValue(), rootNodes.get(1).getId().longValue());
        assertEquals(2, rootNodes.size());
        instanceNodes = testOperations.pipelineInstanceNodes(pipelineInstance);
        assertEquals(1L, instanceNodes.get(0).getId().longValue());
        assertEquals(newNode.getId().longValue(), instanceNodes.get(1).getId().longValue());
        assertEquals(2, instanceNodes.size());
    }

    @Test
    public void testAddPipelineInstanceNode() {
        setUpSingleModulePipeline();
        List<PipelineInstanceNode> rootNodes = pipelineInstanceOperations
            .rootNodes(pipelineInstance);
        assertEquals(1L, rootNodes.get(0).getId().longValue());
        assertEquals(1, rootNodes.size());
        List<PipelineInstanceNode> instanceNodes = testOperations
            .pipelineInstanceNodes(pipelineInstance);
        assertEquals(1L, instanceNodes.get(0).getId().longValue());
        assertEquals(1, instanceNodes.size());

        PipelineInstanceNode newNode = testOperations.merge(new PipelineInstanceNode());
        pipelineInstanceOperations.addPipelineInstanceNode(pipelineInstance, newNode);
        rootNodes = pipelineInstanceOperations.rootNodes(pipelineInstance);
        assertEquals(1L, rootNodes.get(0).getId().longValue());
        assertEquals(1, rootNodes.size());
        instanceNodes = testOperations.pipelineInstanceNodes(pipelineInstance);
        assertEquals(1L, instanceNodes.get(0).getId().longValue());
        assertEquals(newNode.getId().longValue(), instanceNodes.get(1).getId().longValue());
        assertEquals(2, instanceNodes.size());
    }

    @Test
    public void testBindParameters() {
        setUpSingleModulePipeline();
        testOperations.configurePipelineParameterSets(pipelineInstance.getId());
        PipelineInstance databaseInstance = pipelineInstanceOperations
            .pipelineInstance(pipelineInstance.getId());
        pipelineInstanceOperations.bindParameterSets(databaseInstance.getPipelineDefinition(),
            databaseInstance);
        Set<ParameterSet> parameterSets = pipelineInstanceOperations
            .parameterSets(pipelineInstance);
        assertFalse(CollectionUtils.isEmpty(parameterSets));
        ParameterSet parameterSet = parameterSets.iterator().next();
        assertEquals("dummy100", parameterSet.getName());
        assertEquals(1, parameterSets.size());
    }

    @Test
    public void testValidInstanceId() {
        setUpSingleModulePipeline();
        assertTrue(new PipelineInstanceOperations().validInstanceId(pipelineInstance.getId()));
        assertFalse(new PipelineInstanceOperations().validInstanceId(pipelineInstance.getId() + 1));
    }

    @Test
    public void testInstanceId() {
        setUpSingleModulePipeline();
        assertEquals(1, new PipelineInstanceOperations().instanceId(0, "module1"));
        assertEquals(1, new PipelineInstanceOperations().instanceId(1, null));
        assertEquals(0, new PipelineInstanceOperations().instanceId(0, "module2"));
    }

    @Test
    public void testTasksInInstance() {
        setUpSingleModulePipeline();
        assertTrue(new PipelineInstanceOperations().tasksInInstance(0, "module1"));
        assertTrue(new PipelineInstanceOperations().tasksInInstance(1, "module1"));
        assertFalse(new PipelineInstanceOperations().tasksInInstance(1, "module2"));
    }

    private class TestOperations extends DatabaseOperations {

        public PipelineInstance pipelineInstance(long instanceId) {
            return performTransaction(() -> new PipelineInstanceCrud().retrieve(instanceId));
        }

        public PipelineInstanceNode merge(PipelineInstanceNode pipelineInstanceNode) {
            return performTransaction(
                () -> new PipelineInstanceNodeCrud().merge(pipelineInstanceNode));
        }

        public List<PipelineInstanceNode> pipelineInstanceNodes(PipelineInstance pipelineInstance) {
            return performTransaction(() -> {
                PipelineInstance databaseInstance = new PipelineInstanceCrud()
                    .retrieve(pipelineInstance.getId());
                Hibernate.initialize(databaseInstance.getPipelineInstanceNodes());
                return databaseInstance.getPipelineInstanceNodes();
            });
        }

        public void configurePipelineParameterSets(long instanceId) {
            performTransaction(() -> {
                PipelineInstance instance = new PipelineInstanceCrud().retrieve(instanceId);
                PipelineDefinition pipelineDefinition = instance.getPipelineDefinition();
                pipelineDefinition.getParameterSetNames().clear();
                pipelineDefinition.getParameterSetNames().add("dummy100");
                ParameterSet parameterSet = new ParameterSet();
                parameterSet.setName("dummy100");
                new ParameterSetCrud().persist(parameterSet);
                new PipelineDefinitionCrud().merge(pipelineDefinition);
                new PipelineInstanceCrud().merge(instance);
            });
        }
    }
}
