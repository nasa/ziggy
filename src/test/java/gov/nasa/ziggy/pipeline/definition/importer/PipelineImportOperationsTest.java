package gov.nasa.ziggy.pipeline.definition.importer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.database.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.PipelineStepExecutionResources;
import gov.nasa.ziggy.pipeline.xml.ParameterSetDescriptor;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.util.PipelineException;

/** Unit tests for {@link PipelineImportOperations}. */
public class PipelineImportOperationsTest {

    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private PipelineImportOperations pipelineImportOperations;
    private TestOperations testOperations = new TestOperations();
    private List<ParameterSetDescriptor> parameterSetDescriptors;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {
        pipelineImportOperations = new PipelineImportOperations();
    }

    @Test
    public void testPersistClusterDefinition() {
        constructParameterSetDescriptors();
        pipelineOperationsTestUtils.generateSingleNodePipeline(false);

        PipelineNodeExecutionResources nodeResources = new PipelineNodeExecutionResources(
            pipelineOperationsTestUtils.pipeline().getName(),
            pipelineOperationsTestUtils.pipelineStep().getName());
        nodeResources.setGigsPerSubtask(10);
        nodeResources.setMaxAutoResubmits(10);

        PipelineStepExecutionResources pipelineStepResources = new PipelineStepExecutionResources();
        pipelineStepResources.setPipelineStepName("step1");
        pipelineStepResources.setExeTimeoutSeconds(100);
        pipelineStepResources.setMinMemoryMegabytes(10);

        Map<PipelineStep, PipelineStepExecutionResources> resourcesByPipelineStep = new HashMap<>();
        resourcesByPipelineStep.put(pipelineOperationsTestUtils.pipelineStep(),
            pipelineStepResources);
        Map<Pipeline, Set<PipelineNodeExecutionResources>> resourcesByNode = new HashMap<>();
        resourcesByNode.put(pipelineOperationsTestUtils.pipeline(), Set.of(nodeResources));

        pipelineImportOperations.persistClusterDefinition(parameterSetDescriptors, null, null,
            resourcesByPipelineStep, resourcesByNode, null);
        List<Pipeline> pipelines = testOperations.pipelines();
        assertEquals(1, pipelines.size());
        Pipeline pipeline = pipelines.get(0);
        assertEquals("pipeline1", pipeline.getName());

        List<PipelineNode> rootNodes = new PipelineOperations().rootNodes(pipeline);
        assertEquals(1, rootNodes.size());
        PipelineNode rootNode = rootNodes.get(0);
        assertEquals("step1", rootNode.getPipelineStepName());
        assertTrue(rootNode.getNextNodes().isEmpty());

        PipelineNodeExecutionResources databaseNodeResources = new PipelineNodeOperations()
            .pipelineNodeExecutionResources(rootNode);
        assertEquals(10, databaseNodeResources.getGigsPerSubtask(), 1e-9);
        assertEquals(10, databaseNodeResources.getMaxAutoResubmits());

        List<PipelineStep> pipelineSteps = testOperations.pipelineSteps();
        assertEquals(1, pipelineSteps.size());
        assertEquals("step1", pipelineSteps.get(0).getName());
        PipelineStepExecutionResources databaseExecutionResources = new PipelineStepOperations()
            .pipelineStepExecutionResources(pipelineSteps.get(0));
        assertEquals(100, databaseExecutionResources.getExeTimeoutSeconds());
        assertEquals(10, databaseExecutionResources.getMinMemoryMegabytes());

        Map<String, ParameterSet> parameterSetByName = ParameterSet
            .parameterSetByName(new ParametersOperations().parameterSets());
        assertTrue(parameterSetByName.containsKey("parameter1"));
        assertTrue(parameterSetByName.containsKey("parameter2"));
        assertEquals(2, parameterSetByName.size());
        Map<String, Parameter> parameterByName = parameterSetByName.get("parameter1")
            .parameterByName();
        assertTrue(parameterByName.containsKey("p1"));
        assertEquals(1, parameterByName.size());
        parameterByName = parameterSetByName.get("parameter2").parameterByName();
        assertTrue(parameterByName.containsKey("p2"));
        assertTrue(parameterByName.containsKey("p2a"));
        assertEquals(2, parameterByName.size());
    }

    // Tests that an exception in the persister rolls back all of the definitions.
    @Test
    public void testErrorInPersistingClusterDefinitions() {
        constructParameterSetDescriptors();
        pipelineImportOperations = Mockito.spy(PipelineImportOperations.class);
        PipelineNodeCrud pipelineNodeCrud = Mockito.spy(PipelineNodeCrud.class);
        Mockito.doReturn(pipelineNodeCrud).when(pipelineImportOperations).pipelineNodeCrud();
        Mockito.doThrow(PipelineException.class)
            .when(pipelineNodeCrud)
            .merge(ArgumentMatchers.any(PipelineNodeExecutionResources.class));

        pipelineOperationsTestUtils.generateSingleNodePipeline(false);

        PipelineNodeExecutionResources nodeResources = new PipelineNodeExecutionResources(
            pipelineOperationsTestUtils.pipeline().getName(),
            pipelineOperationsTestUtils.pipelineStep().getName());
        nodeResources.setGigsPerSubtask(10);
        nodeResources.setMaxAutoResubmits(10);

        PipelineStepExecutionResources pipelineStepResources = new PipelineStepExecutionResources();
        pipelineStepResources.setPipelineStepName("step1");
        pipelineStepResources.setExeTimeoutSeconds(100);
        pipelineStepResources.setMinMemoryMegabytes(10);

        Map<PipelineStep, PipelineStepExecutionResources> resourcesByPipelineStep = new HashMap<>();
        resourcesByPipelineStep.put(pipelineOperationsTestUtils.pipelineStep(),
            pipelineStepResources);
        Map<Pipeline, Set<PipelineNodeExecutionResources>> resourcesByNode = new HashMap<>();
        resourcesByNode.put(pipelineOperationsTestUtils.pipeline(), Set.of(nodeResources));

        try {
            pipelineImportOperations.persistClusterDefinition(parameterSetDescriptors, null, null,
                resourcesByPipelineStep, resourcesByNode, null);
        } catch (PipelineException e) {
            // swallow.
        }

        // There should be no pipelines defined.
        assertEquals(0, testOperations.pipelines().size());

        // There should be no steps defined.
        assertEquals(0, testOperations.pipelineSteps().size());

        // There should be one parameter set defined, the one that was persisted
        // when the parameter set descriptors were constructed.
        assertEquals(1, new ParametersOperations().parameterSets().size());
        ParameterSet parameterSet = new ParametersOperations().parameterSets().get(0);
        assertEquals("parameter2", parameterSet.getName());

        // The new parameter in the parameter set should not be persisted.
        assertEquals(1, parameterSet.getParameters().size());
        assertEquals("p2", parameterSet.getParameters().iterator().next().getName());
    }

    private void constructParameterSetDescriptors() {
        parameterSetDescriptors = new ArrayList<>();
        ParameterSet parameterSet = new ParameterSet("parameter1");
        parameterSet.getParameters().add(new Parameter("p1", "0", ZiggyDataType.ZIGGY_INT, true));
        ParameterSetDescriptor descriptor = new ParameterSetDescriptor(parameterSet);
        descriptor.setState(ParameterSetDescriptor.State.CREATE);
        parameterSetDescriptors.add(descriptor);
        parameterSet = new ParameterSet("parameter2");
        parameterSet.getParameters()
            .add(new Parameter("p2", "Yowza", ZiggyDataType.ZIGGY_STRING, true));
        testOperations.persist(parameterSet);
        parameterSet.getParameters().add(new Parameter("p2a", "0", ZiggyDataType.ZIGGY_BYTE, true));
        descriptor = new ParameterSetDescriptor(parameterSet);
        descriptor.setState(ParameterSetDescriptor.State.UPDATE);
        parameterSetDescriptors.add(descriptor);
        parameterSet = new ParameterSet("parameter3");
        parameterSet.getParameters()
            .add(new Parameter("p3", "11.424", ZiggyDataType.ZIGGY_FLOAT, true));
        descriptor = new ParameterSetDescriptor(parameterSet);
        descriptor.setState(ParameterSetDescriptor.State.SAME);
        parameterSetDescriptors.add(descriptor);
        parameterSet = new ParameterSet("parameter4");
        parameterSet.getParameters()
            .add(new Parameter("p4", "false", ZiggyDataType.ZIGGY_BOOLEAN, true));
        descriptor = new ParameterSetDescriptor(parameterSet);
        descriptor.setState(ParameterSetDescriptor.State.LIBRARY_ONLY);
        parameterSetDescriptors.add(descriptor);
    }

    private class TestOperations extends DatabaseOperations {

        public List<Pipeline> pipelines() {
            return performTransaction(() -> new PipelineCrud().retrieveAll());
        }

        public List<PipelineStep> pipelineSteps() {
            return performTransaction(() -> new PipelineStepCrud().retrieveAll());
        }

        public void persist(ParameterSet parameterSet) {
            performTransaction(() -> new ParameterSetCrud().persist(parameterSet));
        }
    }
}
