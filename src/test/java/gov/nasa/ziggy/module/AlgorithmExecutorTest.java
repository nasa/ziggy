package gov.nasa.ziggy.module;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.module.remote.nas.NasExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;

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
        PipelineTask task = new PipelineTask();
        ParameterSetCrud parameterSetCrud = Mockito.mock(ParameterSetCrud.class);
        Mockito.when(parameterSetCrud.retrieveRemoteParameters(task)).thenReturn(null);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, parameterSetCrud,
            new ProcessingSummaryOperations());
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when the task's remote execution is disabled, a local executor
    // is returned.
    @Test
    public void testNewInstanceRemoteDisabled() {
        PipelineTask task = new PipelineTask();
        ParameterSetCrud parameterSetCrud = Mockito.mock(ParameterSetCrud.class);
        RemoteParameters remotePars = new RemoteParameters();
        remotePars.setEnabled(false);
        Mockito.when(parameterSetCrud.retrieveRemoteParameters(task)).thenReturn(remotePars);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, parameterSetCrud,
            new ProcessingSummaryOperations());
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when the number of subtasks falls below the remote execution
    // threshold, a local executor is returned.
    @Test
    public void testNewInstanceTooFewSubtasks() {
        PipelineTask task = Mockito.mock(PipelineTask.class);
        Mockito.when(task.getId()).thenReturn(100L);
        ParameterSetCrud parameterSetCrud = Mockito.mock(ParameterSetCrud.class);
        RemoteParameters remotePars = new RemoteParameters();
        remotePars.setEnabled(true);
        remotePars.setMinSubtasksForRemoteExecution(5);
        Mockito.when(parameterSetCrud.retrieveRemoteParameters(task)).thenReturn(remotePars);
        ProcessingSummaryOperations sumOps = Mockito.mock(ProcessingSummaryOperations.class);
        ProcessingSummary summary = Mockito.mock(PipelineTask.ProcessingSummary.class);
        Mockito.when(summary.getTotalSubtaskCount()).thenReturn(100);
        Mockito.when(summary.getCompletedSubtaskCount()).thenReturn(99);
        Mockito.when(sumOps.processingSummary(100L)).thenReturn(summary);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, parameterSetCrud, sumOps);
        assertTrue(executor instanceof LocalAlgorithmExecutor);
    }

    // Tests that when all conditions are correct for a remote executor, a remote
    // executor is returned.
    @Test
    public void testNewInstanceRemote() {
        PipelineTask task = Mockito.mock(PipelineTask.class);
        Mockito.when(task.getId()).thenReturn(100L);
        ParameterSetCrud parameterSetCrud = Mockito.mock(ParameterSetCrud.class);
        RemoteParameters remotePars = new RemoteParameters();
        remotePars.setEnabled(true);
        remotePars.setMinSubtasksForRemoteExecution(5);
        Mockito.when(parameterSetCrud.retrieveRemoteParameters(task)).thenReturn(remotePars);
        ProcessingSummaryOperations sumOps = Mockito.mock(ProcessingSummaryOperations.class);
        ProcessingSummary summary = Mockito.mock(PipelineTask.ProcessingSummary.class);
        Mockito.when(summary.getTotalSubtaskCount()).thenReturn(100);
        Mockito.when(summary.getCompletedSubtaskCount()).thenReturn(90);
        Mockito.when(sumOps.processingSummary(100L)).thenReturn(summary);
        AlgorithmExecutor executor = AlgorithmExecutor.newInstance(task, parameterSetCrud, sumOps);
        assertTrue(executor instanceof NasExecutor);

    }
}
