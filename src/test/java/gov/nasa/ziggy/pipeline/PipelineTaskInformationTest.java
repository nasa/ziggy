package gov.nasa.ziggy.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.DatastoreDirectoryPipelineInputs;
import gov.nasa.ziggy.module.PipelineInputs;
import gov.nasa.ziggy.module.SubtaskInformation;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Unit tests for {@link PipelineTaskInformation} class.
 *
 * @author PT
 */
public class PipelineTaskInformationTest {

    private PipelineDefinitionOperations pipelineDefinitionOperations = mock(
        PipelineDefinitionOperations.class);
    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = mock(
        PipelineModuleDefinitionOperations.class);
    private PipelineTaskInformation pipelineTaskInformation = spy(PipelineTaskInformation.class);
    private String instancePars1Name = "Instance Pars 1";
    private String instancePars2Name = "Instance Pars 2";
    private ParameterSet instanceParSet1 = new ParameterSet(instancePars1Name);
    private ParameterSet instanceParSet2 = new ParameterSet(instancePars2Name);
    private String moduleParsName = "Module Pars";
    private PipelineTask p1 = spy(PipelineTask.class);
    private PipelineTask p2 = spy(PipelineTask.class);
    private SubtaskInformation s1, s2;
    private PipelineDefinitionNode node;
    private PipelineDefinition pipelineDefinition;
    private ClassWrapper<UnitOfWorkGenerator> uowGenerator = new ClassWrapper<>(
        UnitOfWorkGenerator.class);

    @Before
    public void setup() {

        // Put a fully-mocked instance in place
        when(pipelineTaskInformation.pipelineDefinitionOperations())
            .thenReturn(pipelineDefinitionOperations);
        when(pipelineTaskInformation.pipelineModuleDefinitionOperations())
            .thenReturn(pipelineModuleDefinitionOperations);
        PipelineTaskInformation.setInstance(pipelineTaskInformation);

        // Construct the instances of pipeline infrastructure needed for these tests
        node = new PipelineDefinitionNode();
        PipelineModuleDefinition moduleDefinition = new PipelineModuleDefinition("module");
        ClassWrapper<PipelineInputs> inputsClass = new ClassWrapper<>(
            DatastoreDirectoryPipelineInputs.class);
        moduleDefinition.setInputsClass(inputsClass);
        node.setModuleName(moduleDefinition.getName());
        pipelineDefinition = new PipelineDefinition("pipeline");
        node.setPipelineName(pipelineDefinition.getName());
        when(pipelineDefinitionOperations.pipelineDefinition("pipeline"))
            .thenReturn(pipelineDefinition);

        // Set up of instance-level parameters
        pipelineDefinition.getParameterSetNames().add(instancePars1Name);
        pipelineDefinition.getParameterSetNames().add(instancePars2Name);
        instanceParSet1.getParameters()
            .add(new Parameter("intParam", "0", ZiggyDataType.ZIGGY_INT));
        instanceParSet2.getParameters()
            .add(new Parameter("floatParam", "0", ZiggyDataType.ZIGGY_FLOAT));

        // Set up of module-level parameters
        when(
            pipelineModuleDefinitionOperations.pipelineModuleDefinition(moduleDefinition.getName()))
                .thenReturn(moduleDefinition);
        node.getParameterSetNames().add(moduleParsName);

        // Set up unit of work generation
        doReturn(uowGenerator).when(pipelineTaskInformation).unitOfWorkGenerator(node);
        UnitOfWork u1 = new UnitOfWork();
        u1.setParameters(Set.of(new Parameter("param1", "value1")));
        UnitOfWork u2 = new UnitOfWork();
        u2.setParameters(Set.of(new Parameter("param2", "value2")));
        List<UnitOfWork> uowList = new ArrayList<>();
        uowList.add(u1);
        uowList.add(u2);
        doReturn(uowList).when(pipelineTaskInformation)
            .unitsOfWork(ArgumentMatchers.<ClassWrapper<UnitOfWorkGenerator>> any(),
                ArgumentMatchers.<PipelineInstanceNode> any(),
                ArgumentMatchers.<PipelineInstance> any());

        // Set up pipeline task generation
        doReturn(1L).when(p1).getId();
        doReturn(2L).when(p2).getId();
        doReturn(p1).doReturn(p2)
            .when(pipelineTaskInformation)
            .pipelineTask(any(PipelineInstance.class), any(PipelineInstanceNode.class),
                any(UnitOfWork.class));

        // Set up SubtaskInformation returns
        s1 = new SubtaskInformation("module", "u1", 3);
        s2 = new SubtaskInformation("module", "u2", 5);
        doReturn(s1).when(pipelineTaskInformation).subtaskInformation(moduleDefinition, p1, node);
        doReturn(s2).when(pipelineTaskInformation).subtaskInformation(moduleDefinition, p2, node);
    }

    @Test
    public void testBasicFunctionality() {

        // At the start, the has-information query should return false
        assertFalse(PipelineTaskInformation.hasPipelineDefinitionNode(node));

        // Asking for the subtask information should cause it to be generated
        List<SubtaskInformation> subtaskInfo = PipelineTaskInformation.subtaskInformation(node);
        assertTrue(PipelineTaskInformation.hasPipelineDefinitionNode(node));
        assertEquals(2, subtaskInfo.size());
        assertTrue(subtaskInfo.contains(s1));
        assertTrue(subtaskInfo.contains(s2));

        // Resetting it should cause it to disappear again
        PipelineTaskInformation.reset(node);
        assertFalse(PipelineTaskInformation.hasPipelineDefinitionNode(node));
    }
}
