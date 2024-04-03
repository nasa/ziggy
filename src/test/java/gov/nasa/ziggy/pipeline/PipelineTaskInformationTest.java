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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import gov.nasa.ziggy.module.DatastoreDirectoryPipelineInputs;
import gov.nasa.ziggy.module.PipelineInputs;
import gov.nasa.ziggy.module.SubtaskInformation;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Unit tests for {@link PipelineTaskInformation} class.
 *
 * @author PT
 */
public class PipelineTaskInformationTest {

    private ParameterSetCrud parameterSetCrud = mock(ParameterSetCrud.class);
    private PipelineDefinitionCrud pipelineDefinitionCrud = mock(PipelineDefinitionCrud.class);
    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = mock(
        PipelineModuleDefinitionCrud.class);
    private PipelineTaskInformation pipelineTaskInformation = spy(PipelineTaskInformation.class);
    private String instancePars1Name = "Instance Pars 1";
    private String instancePars2Name = "Instance Pars 2";
    private ParameterSet instanceParSet1 = new ParameterSet(instancePars1Name);
    private ParameterSet instanceParSet2 = new ParameterSet(instancePars2Name);
    private String moduleParsName = "Module Pars";
    private ParameterSet moduleParSet = new ParameterSet(moduleParsName);
    private PipelineTask p1 = new PipelineTask();
    private PipelineTask p2 = new PipelineTask();
    private SubtaskInformation s1, s2;
    private PipelineDefinitionNode node;
    private PipelineDefinition pipelineDefinition;
    private ClassWrapper<UnitOfWorkGenerator> uowGenerator = new ClassWrapper<>(
        UnitOfWorkGenerator.class);

    @Before
    public void setup() {

        // Put a fully-mocked instance in place
        pipelineTaskInformation.setParameterSetCrud(parameterSetCrud);
        pipelineTaskInformation.setPipelineDefinitionCrud(pipelineDefinitionCrud);
        pipelineTaskInformation.setPipelineModuleDefinitionCrud(pipelineModuleDefinitionCrud);
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
        when(pipelineDefinitionCrud.retrieveLatestVersionForName("pipeline"))
            .thenReturn(pipelineDefinition);

        // Set up of instance-level parameters
        Map<ClassWrapper<ParametersInterface>, String> instanceParameterNames = new HashMap<>();
        instanceParameterNames.put(new ClassWrapper<>(InstancePars1.class), instancePars1Name);
        instanceParameterNames.put(new ClassWrapper<>(InstancePars2.class), instancePars2Name);
        pipelineDefinition.setPipelineParameterSetNames(instanceParameterNames);
        instanceParSet1.setTypedParameters(new InstancePars1().getParameters());
        instanceParSet2.setTypedParameters(new InstancePars2().getParameters());
        when(parameterSetCrud.retrieveLatestVersionForName(instancePars1Name))
            .thenReturn(instanceParSet1);
        when(parameterSetCrud.retrieveLatestVersionForName(instancePars2Name))
            .thenReturn(instanceParSet2);

        // Set up of module-level parameters
        when(pipelineModuleDefinitionCrud.retrieveLatestVersionForName(moduleDefinition.getName()))
            .thenReturn(moduleDefinition);
        Map<ClassWrapper<ParametersInterface>, String> moduleParameterNames = new HashMap<>();
        node.setModuleParameterSetNames(moduleParameterNames);
        when(parameterSetCrud.retrieveLatestVersionForName(moduleParsName))
            .thenReturn(moduleParSet);

        // Set up unit of work generation
        doReturn(uowGenerator).when(pipelineTaskInformation).unitOfWorkGenerator(node);
        UnitOfWork u1 = new UnitOfWork();
        UnitOfWork u2 = new UnitOfWork();
        List<UnitOfWork> uowList = new ArrayList<>();
        uowList.add(u1);
        uowList.add(u2);
        doReturn(uowList).when(pipelineTaskInformation)
            .unitsOfWork(ArgumentMatchers.<ClassWrapper<UnitOfWorkGenerator>> any(),
                ArgumentMatchers.<PipelineInstanceNode> any(),
                ArgumentMatchers.<PipelineInstance> any());

        // Set up pipeline task generation
        p1.setId(1L);
        p2.setId(2L);
        doReturn(p1).doReturn(p2)
            .when(pipelineTaskInformation)
            .pipelineTask(any(PipelineInstance.class), any(PipelineInstanceNode.class),
                any(UnitOfWork.class));

        // Set up SubtaskInformation returns
        s1 = new SubtaskInformation("module", "u1", 3);
        s2 = new SubtaskInformation("module", "u2", 5);
        doReturn(s1).when(pipelineTaskInformation).subtaskInformation(moduleDefinition, p1);
        doReturn(s2).when(pipelineTaskInformation).subtaskInformation(moduleDefinition, p2);
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

    public static class InstancePars1 extends Parameters {
        private int intParam;

        public int getIntParam() {
            return intParam;
        }

        public void setIntParam(int intParam) {
            this.intParam = intParam;
        }
    }

    public static class InstancePars2 extends Parameters {
        private float floatParam;

        public float getFloatParam() {
            return floatParam;
        }

        public void setFloatParam(float floatParam) {
            this.floatParam = floatParam;
        }
    }
}
