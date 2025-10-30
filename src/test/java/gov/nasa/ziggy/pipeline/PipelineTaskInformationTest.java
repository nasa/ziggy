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
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.io.DatastoreDirectoryPipelineInputs;
import gov.nasa.ziggy.pipeline.step.io.PipelineInputs;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskInformation;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Unit tests for {@link PipelineTaskInformation} class.
 *
 * @author PT
 */
public class PipelineTaskInformationTest {

    private PipelineOperations pipelineOperations = mock(PipelineOperations.class);
    private PipelineStepOperations pipelineStepOperations = mock(PipelineStepOperations.class);
    private PipelineTaskInformation pipelineTaskInformation = spy(PipelineTaskInformation.class);
    private String instancePars1Name = "Instance Pars 1";
    private String instancePars2Name = "Instance Pars 2";
    private ParameterSet instanceParSet1 = new ParameterSet(instancePars1Name);
    private ParameterSet instanceParSet2 = new ParameterSet(instancePars2Name);
    private String algorithmParsName = "Algorithm Pars";
    private PipelineTask p1 = spy(PipelineTask.class);
    private PipelineTask p2 = spy(PipelineTask.class);
    private SubtaskInformation s1, s2;
    private PipelineNode node;
    private Pipeline pipeline;
    private PipelineStep pipelineStep;
    private ClassWrapper<UnitOfWorkGenerator> uowGenerator = new ClassWrapper<>(
        UnitOfWorkGenerator.class);

    @Before
    public void setUp() {

        // Put a fully-mocked instance in place.
        when(pipelineTaskInformation.pipelineOperations()).thenReturn(pipelineOperations);
        when(pipelineTaskInformation.pipelineStepOperations()).thenReturn(pipelineStepOperations);
        PipelineTaskInformation.setInstance(pipelineTaskInformation);

        // Construct the instances of pipeline infrastructure needed for these tests.
        node = new PipelineNode();
        pipelineStep = new PipelineStep("step");
        ClassWrapper<PipelineInputs> inputsClass = new ClassWrapper<>(
            DatastoreDirectoryPipelineInputs.class);
        pipelineStep.setInputsClass(inputsClass);
        node.setPipelineStepName(pipelineStep.getName());
        pipeline = new Pipeline("pipeline");
        node.setPipelineName(pipeline.getName());
        when(pipelineOperations.pipeline("pipeline")).thenReturn(pipeline);

        // Set up instance-level parameters.
        pipeline.getParameterSetNames().add(instancePars1Name);
        pipeline.getParameterSetNames().add(instancePars2Name);
        instanceParSet1.getParameters()
            .add(new Parameter("intParam", "0", ZiggyDataType.ZIGGY_INT));
        instanceParSet2.getParameters()
            .add(new Parameter("floatParam", "0", ZiggyDataType.ZIGGY_FLOAT));

        // Set up node-level parameters.
        when(pipelineStepOperations.pipelineStep(pipelineStep.getName())).thenReturn(pipelineStep);
        node.getParameterSetNames().add(algorithmParsName);
    }

    @Test
    public void testBasicFunctionality() {

        // Set up unit of work generation.
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

        // Set up pipeline task generation.
        doReturn(1L).when(p1).getId();
        doReturn(2L).when(p2).getId();
        doReturn(p1).doReturn(p2)
            .when(pipelineTaskInformation)
            .pipelineTask(any(PipelineInstance.class), any(PipelineInstanceNode.class),
                any(UnitOfWork.class));

        // Set up SubtaskInformation returns.
        s1 = new SubtaskInformation("node", "u1", 3);
        s2 = new SubtaskInformation("node", "u2", 5);
        doReturn(s1).when(pipelineTaskInformation).subtaskInformation(pipelineStep, p1, node);
        doReturn(s2).when(pipelineTaskInformation).subtaskInformation(pipelineStep, p2, node);

        // At the start, the has-information query should return false
        assertFalse(PipelineTaskInformation.hasPipelineNode(node));

        // Asking for the subtask information should cause it to be generated
        List<SubtaskInformation> subtaskInfo = PipelineTaskInformation.subtaskInformation(node);
        assertTrue(PipelineTaskInformation.hasPipelineNode(node));
        assertEquals(2, subtaskInfo.size());
        assertTrue(subtaskInfo.contains(s1));
        assertTrue(subtaskInfo.contains(s2));

        // Resetting it should cause it to disappear again
        PipelineTaskInformation.reset(node);
        assertFalse(PipelineTaskInformation.hasPipelineNode(node));
    }

    @Test
    public void testSingleUnitOfWork() {
        ClassWrapper<UnitOfWorkGenerator> uowGenerator = new ClassWrapper<>(
            SingleUnitOfWorkGenerator.class);
        pipelineStep.setUnitOfWorkGenerator(uowGenerator);
        doReturn(uowGenerator).when(pipelineTaskInformation).unitOfWorkGenerator(node);
        List<SubtaskInformation> subtaskInfo = PipelineTaskInformation.subtaskInformation(node);
        assertEquals(1, subtaskInfo.size());
        SubtaskInformation subtaskInformation = subtaskInfo.get(0);
        assertEquals(pipelineStep.getName(), subtaskInformation.getPipelineStepName());
        assertEquals(SingleUnitOfWorkGenerator.BRIEF_STATE, subtaskInformation.getUowBriefState());
        assertEquals(1, subtaskInformation.getSubtaskCount());
    }
}
