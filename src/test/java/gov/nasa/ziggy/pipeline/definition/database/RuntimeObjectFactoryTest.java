package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.crud.SimpleCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.models.ModelOperations;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Unit tests for {@link RuntimeObjectFactory}.
 *
 * @author PT
 */
public class RuntimeObjectFactoryTest {

    private RuntimeObjectFactory factory;
    private PipelineDefinition pipelineDefinition;
    private PipelineInstance pipelineInstance;
    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineDefinitionNode definitionNode, definitionNode4;
    private TestOperations testOperations;
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations;
    private PipelineInstanceOperations pipelineInstanceOperations;
    private ParametersOperations parametersOperations;
    private List<PipelineDefinitionNode> pipelineDefinitionNodes;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    // TODO replace the parameters operations with a real instance as part of ZIGGY-421.
    @Before
    public void setUp() {
        factory = Mockito.spy(RuntimeObjectFactory.class);
        testOperations = new TestOperations();
        pipelineInstanceNodeOperations = Mockito.spy(PipelineInstanceNodeOperations.class);
        Mockito.doReturn(pipelineInstanceNodeOperations)
            .when(factory)
            .pipelineInstanceNodeOperations();
        parametersOperations = Mockito.mock(ParametersOperations.class);
        Mockito.doReturn(parametersOperations).when(factory).parametersOperations();
        Mockito.doReturn(parametersOperations)
            .when(pipelineInstanceNodeOperations)
            .parametersOperations();
        pipelineInstanceOperations = Mockito.spy(PipelineInstanceOperations.class);
        Mockito.doReturn(pipelineInstanceOperations).when(factory).pipelineInstanceOperations();
        Mockito.doReturn(parametersOperations)
            .when(pipelineInstanceOperations)
            .parametersOperations();
    }

    @Test
    public void testCreateInstanceNodes() {
        setUpFourModulePipeline();
        List<PipelineInstanceNode> createdInstanceNodes = factory.newInstanceNodes(
            pipelineDefinition, pipelineInstance, List.of(definitionNode), definitionNode4);
        PipelineInstanceNode createdInstanceNode = createdInstanceNodes.get(0);
        assertEquals("module1", createdInstanceNode.getModuleName());
        assertEquals(1, createdInstanceNodes.size());
        for (PipelineInstanceNode node : createdInstanceNodes) {
            assertEquals("pipeline1", node.getPipelineDefinitionNode().getPipelineName());
            assertEquals(1L,
                pipelineInstanceNodeOperations.pipelineInstance(node).getId().longValue());
        }
        createdInstanceNodes = testOperations.allInstanceNodes();
        Map<String, PipelineInstanceNode> instanceNodesByName = new HashMap<>();
        for (PipelineInstanceNode instanceNode : createdInstanceNodes) {
            instanceNodesByName.put(instanceNode.getModuleName(), instanceNode);
        }
        assertNotNull(instanceNodesByName.get("module1"));
        assertNotNull(instanceNodesByName.get("module2"));
        assertNotNull(instanceNodesByName.get("module3"));
        assertNotNull(instanceNodesByName.get("module4"));
        assertEquals(4, instanceNodesByName.size());

        assertEquals(pipelineDefinitionNodes.get(0).getId().longValue(),
            instanceNodesByName.get("module1").getPipelineDefinitionNode().getId().longValue());

        List<PipelineInstanceNode> nextNodes = pipelineInstanceNodeOperations
            .nextPipelineInstanceNodes(createdInstanceNodes.get(0).getId());
        PipelineInstanceNode nextNode = nextNodes.get(0);
        assertEquals(pipelineDefinitionNodes.get(1).getId().longValue(),
            nextNode.getPipelineDefinitionNode().getId().longValue());
        assertEquals("module2", nextNode.getModuleName());
        assertEquals(1, nextNodes.size());

        nextNodes = pipelineInstanceNodeOperations
            .nextPipelineInstanceNodes(instanceNodesByName.get("module2").getId());
        nextNode = nextNodes.get(0);
        assertEquals(pipelineDefinitionNodes.get(2).getId().longValue(),
            nextNode.getPipelineDefinitionNode().getId().longValue());
        assertEquals("module3", nextNode.getModuleName());
        assertEquals(1, nextNodes.size());

        nextNodes = pipelineInstanceNodeOperations
            .nextPipelineInstanceNodes(instanceNodesByName.get("module3").getId());
        nextNode = nextNodes.get(0);
        assertEquals(pipelineDefinitionNodes.get(3).getId().longValue(),
            nextNode.getPipelineDefinitionNode().getId().longValue());
        assertEquals("module4", nextNode.getModuleName());
        assertEquals(1, nextNodes.size());

        nextNodes = pipelineInstanceNodeOperations
            .nextPipelineInstanceNodes(instanceNodesByName.get("module4").getId());
        assertTrue(CollectionUtils.isEmpty(nextNodes));
    }

    private void setUpFourModulePipeline() {
        PipelineOperationsTestUtils testUtils = new PipelineOperationsTestUtils();
        testUtils.setUpFourModulePipeline();
        pipelineDefinitionNodes = testUtils.getPipelineDefinitionNodes();
        definitionNode = pipelineDefinitionNodes.get(0);
        definitionNode4 = pipelineDefinitionNodes.get(3);
        pipelineInstance = testUtils.pipelineInstance();
        pipelineDefinition = testUtils.pipelineDefinition();
    }

    @Test
    public void testNewPipelineInstance() {
        setUpSingleModulePipeline();
        ModelRegistry modelRegistry = new ModelOperations().unlockedRegistry();
        PipelineInstance newPipelineInstance = factory.newPipelineInstance("test",
            pipelineDefinition, modelRegistry);
        assertEquals(2L, newPipelineInstance.getId().longValue());
        assertEquals("test", newPipelineInstance.getName());
        assertEquals("pipeline1", newPipelineInstance.getPipelineDefinition().getName());
        assertEquals(PipelineInstance.State.PROCESSING, newPipelineInstance.getState());
    }

    private void setUpSingleModulePipeline() {
        PipelineOperationsTestUtils testUtils = new PipelineOperationsTestUtils();
        testUtils.setUpSingleModulePipeline();
        pipelineDefinition = testUtils.pipelineDefinition();
        definitionNode = testUtils.pipelineDefinitionNode();
        pipelineInstance = testUtils.pipelineInstance();
        pipelineInstanceNode = testUtils.pipelineInstanceNode();
    }

    @Test
    public void testNewPipelineTasks() {
        setUpSingleModulePipeline();
        PipelineDefinitionNodeExecutionResources resources = new PipelineDefinitionNodeExecutionResources(
            pipelineDefinition.getName(), definitionNode.getModuleName());
        resources.setMaxFailedSubtaskCount(5);
        resources.setMaxAutoResubmits(2);
        testOperations.persist(resources);
        UnitOfWork uow1 = new UnitOfWork();
        uow1.addParameter(new Parameter("bauhaus", "love", ZiggyDataType.ZIGGY_STRING));
        UnitOfWork uow2 = new UnitOfWork();
        uow2.addParameter(new Parameter("duran", "duran", ZiggyDataType.ZIGGY_STRING));
        List<PipelineTask> pipelineTasks = factory.newPipelineTasks(pipelineInstanceNode,
            pipelineInstance, List.of(uow1, uow2));
        assertEquals(2, pipelineTasks.size());
        pipelineTasks = testOperations.pipelineTasks(pipelineInstanceNode);
        assertFalse(CollectionUtils.isEmpty(pipelineTasks));
        Map<Long, PipelineTask> pipelineTasksById = new HashMap<>();
        for (PipelineTask task : pipelineTasks) {
            pipelineTasksById.put(task.getId(), task);
        }

        PipelineTask pipelineTask = pipelineTasksById.get(3L);
        assertNotNull(pipelineTask);
        assertEquals(pipelineInstance.getId().longValue(), pipelineTask.getPipelineInstanceId());
        assertEquals(definitionNode.getModuleName(), pipelineTask.getModuleName());
        UnitOfWork taskUow = pipelineTask.uowTaskInstance();
        Set<Parameter> parameters = taskUow.getParameters();
        assertFalse(CollectionUtils.isEmpty(parameters));
        Parameter parameter = parameters.iterator().next();
        assertEquals("bauhaus", parameter.getName());
        assertEquals(1, parameters.size());

        pipelineTask = pipelineTasksById.get(4L);
        assertNotNull(pipelineTask);
        assertEquals(pipelineInstance.getId().longValue(), pipelineTask.getPipelineInstanceId());
        assertEquals(definitionNode.getModuleName(), pipelineTask.getModuleName());
        taskUow = pipelineTask.uowTaskInstance();
        parameters = taskUow.getParameters();
        assertFalse(CollectionUtils.isEmpty(parameters));
        parameter = parameters.iterator().next();
        assertEquals("duran", parameter.getName());
        assertEquals(1, parameters.size());

        // In addition to the 2 tasks created here, there were already 2 in the
        // database from PipelineOperationsTestUtils.
        assertEquals(4, pipelineTasks.size());
    }

    @Test
    public void testPipelineInstanceParameterBinding() {
        setUpSingleModulePipeline();
        ParameterSet parameter1 = new ParameterSet();
        parameter1.setName("parameter1");
        ParameterSet parameter2 = new ParameterSet();
        parameter2.setName("parameter2");
        testOperations.persist(List.of(parameter1, parameter2));
        ModelRegistry modelRegistry = new ModelOperations().unlockedRegistry();
        ParametersOperations realParametersOperations = new ParametersOperations();
        Mockito.doReturn(realParametersOperations)
            .when(pipelineInstanceOperations)
            .parametersOperations();
        PipelineInstance pipelineInstance = factory.newPipelineInstance("partest",
            pipelineDefinition, modelRegistry);
        assertEquals(2, pipelineInstance.getParameterSets().size());
        Set<ParameterSet> parameterSets = pipelineInstanceOperations
            .parameterSets(pipelineInstance);
        Map<String, ParameterSet> parameterSetsByName = ParameterSet
            .parameterSetByName(parameterSets);
        assertNotNull(parameterSetsByName.get("parameter1"));
        assertNotNull(parameterSetsByName.get("parameter2"));
        assertEquals(2, parameterSets.size());
    }

    @Test
    public void testPipelineInstanceNodeParameterBinding() {
        setUpSingleModulePipeline();
        ParameterSet parameter3 = new ParameterSet();
        parameter3.setName("parameter3");
        ParameterSet parameter4 = new ParameterSet();
        parameter4.setName("parameter4");
        testOperations.persist(List.of(parameter3, parameter4));
        ParametersOperations realParametersOperations = new ParametersOperations();
        Mockito.doReturn(realParametersOperations)
            .when(pipelineInstanceNodeOperations)
            .parametersOperations();
        List<PipelineInstanceNode> pipelineInstanceNodes = factory.newInstanceNodes(
            pipelineDefinition, pipelineInstance, List.of(definitionNode), definitionNode);
        PipelineInstanceNode pipelineInstanceNode = pipelineInstanceNodes.get(0);
        assertEquals(2, pipelineInstanceNode.getParameterSets().size());
        Set<ParameterSet> parameterSets = pipelineInstanceNodeOperations
            .parameterSets(pipelineInstanceNode);
        Map<String, ParameterSet> parameterSetsByName = ParameterSet
            .parameterSetByName(parameterSets);
        assertNotNull(parameterSetsByName.get("parameter3"));
        assertNotNull(parameterSetsByName.get("parameter4"));
        assertEquals(2, parameterSets.size());
        assertEquals(1, pipelineInstanceNodes.size());
    }

    private class TestOperations extends DatabaseOperations {

        public List<PipelineInstanceNode> allInstanceNodes() {
            return performTransaction(
                () -> new PipelineInstanceNodeCrud().retrieveAll(pipelineInstance));
        }

        public void persist(PipelineDefinitionNodeExecutionResources resources) {
            performTransaction(() -> new PipelineDefinitionNodeCrud().persist(resources));
        }

        public List<PipelineTask> pipelineTasks(PipelineInstanceNode pipelineInstanceNode) {
            return performTransaction(() -> {
                SimpleCrud<PipelineInstance> simpleCrud = new SimpleCrud<>();
                ZiggyQuery<PipelineInstanceNode, PipelineTask> query = simpleCrud
                    .createZiggyQuery(PipelineInstanceNode.class, PipelineTask.class);
                query.column(PipelineInstanceNode_.id).in(pipelineInstanceNode.getId());
                query.column(PipelineInstanceNode_.pipelineTasks).select();
                return simpleCrud.list(query);
            });
        }

        public void persist(Collection<ParameterSet> parameterSets) {
            performTransaction(() -> {
                for (ParameterSet parameterSet : parameterSets) {
                    new ParameterSetCrud().persist(parameterSet);
                }
            });
        }
    }
}
