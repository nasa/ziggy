package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;

import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.data.management.DatastoreProducerConsumer;

/**
 * Test class for the ResultsOriginator class. Which is so simple that it doesn't really rate a test
 * class, but completeness is a worthwhile goal.
 *
 * @author PT
 */
public class DatastoreProducerConsumerTest {

    public static final String FILE_SPEC = "d1/d2/d3/fake-file.h5";
    public static final long TASK_ID = 30L;

    @Test
    public void testResultsOriginatorMethods() {

        PipelineTask p = Mockito.mock(PipelineTask.class);
        Mockito.when(p.getId()).thenReturn(TASK_ID);
        DatastoreProducerConsumer r = new DatastoreProducerConsumer(p, Paths.get(FILE_SPEC));
        assertEquals(FILE_SPEC, r.getFilename());
        assertEquals(TASK_ID, r.getProducer());
    }
}
