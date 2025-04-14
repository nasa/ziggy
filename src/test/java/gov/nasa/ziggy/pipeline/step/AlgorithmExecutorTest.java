package gov.nasa.ziggy.pipeline.step;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.remote.RemoteAlgorithmExecutor;

/**
 * Unit tests for {@link AlgorithmExecutor} class.
 *
 * @author PT
 */
public class AlgorithmExecutorTest {

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
            .thenReturn(new PipelineNodeExecutionResources("dummy", "dummy"));
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, pipelineTaskOperations,
            new PipelineTaskDataOperations());
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when the task's remote execution is disabled, a local executor
    // is returned.
    @Test
    public void testNewInstanceRemoteDisabled() {
        PipelineTask task = Mockito.spy(PipelineTask.class);
        PipelineNodeExecutionResources executionResources = new PipelineNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setRemoteExecutionEnabled(false);
        PipelineTaskOperations pipelineTaskOperations = Mockito.mock(PipelineTaskOperations.class);
        Mockito
            .when(
                pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(executionResources);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, pipelineTaskOperations,
            new PipelineTaskDataOperations());
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when the number of subtasks falls below the remote execution
    // threshold, a local executor is returned.
    @Test
    public void testNewInstanceTooFewSubtasks() {
        PipelineTask task = Mockito.mock(PipelineTask.class);
        Mockito.when(task.getId()).thenReturn(100L);
        PipelineNodeExecutionResources executionResources = new PipelineNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setMinSubtasksForRemoteExecution(5);
        PipelineTaskOperations pipelineTaskOperations = Mockito.mock(PipelineTaskOperations.class);
        Mockito
            .when(
                pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(executionResources);
        PipelineTaskDataOperations pipelineTaskDataOperations = Mockito
            .mock(PipelineTaskDataOperations.class);
        SubtaskCounts subtaskCounts = new SubtaskCounts(100, 99, 0);
        Mockito.when(pipelineTaskDataOperations.subtaskCounts(task)).thenReturn(subtaskCounts);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, pipelineTaskOperations,
            pipelineTaskDataOperations);
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when all conditions are correct for a remote executor, a remote
    // executor is returned.
    @Test
    public void testNewInstanceRemote() {
        PipelineTask task = Mockito.mock(PipelineTask.class);
        Mockito.when(task.getId()).thenReturn(100L);
        PipelineNodeExecutionResources executionResources = new PipelineNodeExecutionResources(
            "dummy", "dummy");
        executionResources.setRemoteExecutionEnabled(true);
        executionResources.setMinSubtasksForRemoteExecution(5);
        PipelineTaskOperations pipelineTaskOperations = Mockito.mock(PipelineTaskOperations.class);
        Mockito
            .when(
                pipelineTaskOperations.executionResources(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(executionResources);
        PipelineTaskDataOperations pipelineTaskDataOperations = Mockito
            .mock(PipelineTaskDataOperations.class);
        SubtaskCounts subtaskCounts = new SubtaskCounts(100, 90, 0);
        Mockito.when(pipelineTaskDataOperations.subtaskCounts(task)).thenReturn(subtaskCounts);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, pipelineTaskOperations,
            pipelineTaskDataOperations);
        assertTrue(executor instanceof RemoteAlgorithmExecutor);
    }
}
