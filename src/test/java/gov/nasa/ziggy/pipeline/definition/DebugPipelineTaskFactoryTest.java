package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertNotNull;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;

/**
 * @author Sean McCaulif
 */
public class DebugPipelineTaskFactoryTest {
    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Test
    public void testTaskFactory() throws Exception {
        FakePipelineTaskFactory taskFactory = new FakePipelineTaskFactory();
        PipelineTask task = taskFactory.newTask();
        assertNotNull(task);
    }
}
