package gov.nasa.ziggy.module;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.module.remote.nas.NasExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;

/**
 * Unit tests for {@link AlgorithmExecutor} class.
 *
 * @author PT
 */
public class AlgorithmExecutorTest {

    // Tests the newRemoteInstance() static class.
    @Test
    public void testNewRemoteInstance() {
        AlgorithmExecutor executor = AlgorithmExecutor.newRemoteInstance(new PipelineTask());
        assertTrue(executor instanceof NasExecutor);
    }

    // Tests that when the task is null, a local executor is returned.
    @Test
    public void testNewInstanceNullTask() {
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(null);
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when the task has no remote parameters, a local executor is
    // returned.
    @Test
    public void testNewInstanceNullRemoteParameters() {
        PipelineTask task = Mockito.spy(PipelineTask.class);
        PipelineTaskOperations pipelineTaskOperations = Mockito.mock(PipelineTaskOperations.class);
        Mockito
            .when(
                pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(new PipelineDefinitionNodeExecutionResources("dummy", "dummy"));
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, pipelineTaskOperations);
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when the task's remote execution is disabled, a local executor
    // is returned.
    @Test
    public void testNewInstanceRemoteDisabled() {
        PipelineTask task = Mockito.spy(PipelineTask.class);
        PipelineDefinitionNodeExecutionResources executionResources = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setRemoteExecutionEnabled(false);
        PipelineTaskOperations pipelineTaskOperations = Mockito.mock(PipelineTaskOperations.class);
        Mockito
            .when(
                pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(executionResources);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, pipelineTaskOperations);
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when the number of subtasks falls below the remote execution
    // threshold, a local executor is returned.
    @Test
    public void testNewInstanceTooFewSubtasks() {
        PipelineTask task = Mockito.mock(PipelineTask.class);
        Mockito.when(task.getId()).thenReturn(100L);
        PipelineDefinitionNodeExecutionResources executionResources = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setMinSubtasksForRemoteExecution(5);
        PipelineTaskOperations pipelineTaskOperations = Mockito.mock(PipelineTaskOperations.class);
        Mockito
            .when(
                pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(executionResources);
        Mockito.when(task.getTotalSubtaskCount()).thenReturn(100);
        Mockito.when(task.getCompletedSubtaskCount()).thenReturn(99);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, pipelineTaskOperations);
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when all conditions are correct for a remote executor, a remote
    // executor is returned.
    @Test
    public void testNewInstanceRemote() {
        PipelineTask task = Mockito.mock(PipelineTask.class);
        Mockito.when(task.getId()).thenReturn(100L);
        PipelineDefinitionNodeExecutionResources executionResources = new PipelineDefinitionNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setMinSubtasksForRemoteExecution(5);
        PipelineTaskOperations pipelineTaskOperations = Mockito.mock(PipelineTaskOperations.class);
        Mockito
            .when(
                pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(executionResources);
        Mockito.when(task.getTotalSubtaskCount()).thenReturn(100);
        Mockito.when(task.getCompletedSubtaskCount()).thenReturn(90);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, pipelineTaskOperations);
        assertTrue(executor instanceof NasExecutor);
    }
}
