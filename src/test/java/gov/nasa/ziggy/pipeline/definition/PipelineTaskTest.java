package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** Unit tests for {@link PipelineTask} class. */
public class PipelineTaskTest {

    private PipelineTask pipelineTask;

    @Before
    public void setUp() {
        pipelineTask = Mockito.spy(PipelineTask.class);
    }

    @Test
    public void testTaskBaseName() {
        Mockito.when(pipelineTask.getId()).thenReturn(100L);
        Mockito.when(pipelineTask.getPipelineInstanceId()).thenReturn(50L);
        Mockito.when(pipelineTask.getPipelineStepName()).thenReturn("tps");
        assertEquals("50-100-tps", pipelineTask.taskBaseName());
        Mockito.when(pipelineTask.getPipelineStepName()).thenReturn("pa ppa");
        assertEquals("50-100-pa_ppa", pipelineTask.taskBaseName());
    }
}
