package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.services.database.TestUtils;

/**
 * @author Sean McCaulif
 */
public class DebugPipelineTaskFactoryTest {
    @Before
    public void setUp() {
        TestUtils.setUpDatabase();
    }

    @After
    public void tearDown() {
        TestUtils.tearDownDatabase();
    }

    @Test
    public void testTaskFactory() throws Exception {
        FakePipelineTaskFactory taskFactory = new FakePipelineTaskFactory();
        PipelineTask task = taskFactory.newTask();
        assertNotNull(task);
    }
}
